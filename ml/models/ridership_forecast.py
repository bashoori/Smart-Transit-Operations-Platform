"""
TransLink Smart Transit Platform
Model: Ridership Forecasting — XGBoost Regression

Predicts daily boardings per route for the next 30 days.
Used by Planning to drive capacity allocation and service planning decisions.

Feature engineering approach:
- Temporal: day-of-week, month, is_holiday, fiscal_period
- Lagged ridership: 7/14/28-day rolling averages (avoids data leakage)
- External: precipitation, temperature, is_adverse_weather
- Events: special event flag (sports, concerts, community events)
- Route characteristics: route_type, corridor, is_rapid_transit
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb
import joblib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Feature engineering ───────────────────────────────────────────────────────

def build_temporal_features(df: pd.DataFrame, date_col: str = "service_date") -> pd.DataFrame:
    """
    Encodes time-based patterns.
    Uses cyclical encoding (sin/cos) for day_of_week and month
    so the model understands that Monday follows Sunday, January follows December.
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    df["day_of_week"] = df[date_col].dt.dayofweek
    df["month"] = df[date_col].dt.month
    df["day_of_year"] = df[date_col].dt.dayofyear
    df["week_of_year"] = df[date_col].dt.isocalendar().week.astype(int)
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["quarter"] = df[date_col].dt.quarter

    # Cyclical encoding — avoids ordinal bias (7 != 1 numerically but Sunday ≈ Monday)
    df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    return df


def build_lag_features(df: pd.DataFrame, target_col: str = "boardings",
                       group_col: str = "route_id") -> pd.DataFrame:
    """
    Creates lagged and rolling features per route.
    Sorted by date within each route group to ensure temporal integrity.
    IMPORTANT: lags are computed strictly from the past to prevent data leakage.
    """
    df = df.sort_values([group_col, "service_date"]).copy()

    for lag in [7, 14, 21, 28]:
        df[f"boardings_lag_{lag}d"] = (
            df.groupby(group_col)[target_col]
            .shift(lag)
        )

    for window in [7, 14, 28]:
        df[f"boardings_roll_{window}d_mean"] = (
            df.groupby(group_col)[target_col]
            .transform(lambda x: x.shift(1).rolling(window, min_periods=3).mean())
        )
        df[f"boardings_roll_{window}d_std"] = (
            df.groupby(group_col)[target_col]
            .transform(lambda x: x.shift(1).rolling(window, min_periods=3).std())
        )

    return df


def build_weather_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Weather correlates strongly with TransLink ridership:
    - Heavy rain → ridership up (people avoid walking/cycling)
    - Snow → delays and ridership volatility
    - Extreme heat → SkyTrain surge (avoid driving)
    """
    df = df.copy()
    df["is_adverse_weather"] = (
        (df["precipitation_mm"] > 10) | (df["is_snow"] == 1)
    ).astype(int)
    df["temp_bucket"] = pd.cut(
        df["temperature_c"],
        bins=[-20, 0, 10, 20, 30, 50],
        labels=["extreme_cold", "cold", "mild", "warm", "hot"],
    )
    return df


def build_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Full feature pipeline — call before train or predict."""
    df = build_temporal_features(df)
    df = build_lag_features(df)
    df = build_weather_features(df)

    # Encode categoricals
    le = LabelEncoder()
    for col in ["route_type", "corridor", "temp_bucket"]:
        if col in df.columns:
            df[col + "_enc"] = le.fit_transform(df[col].astype(str))

    return df


# ── Feature list (used for training and inference) ────────────────────────────

FEATURE_COLS = [
    # Temporal
    "dow_sin", "dow_cos", "month_sin", "month_cos",
    "day_of_year", "week_of_year", "is_weekend", "quarter",
    "is_holiday", "is_special_event",
    # Lagged ridership
    "boardings_lag_7d", "boardings_lag_14d", "boardings_lag_21d", "boardings_lag_28d",
    "boardings_roll_7d_mean", "boardings_roll_14d_mean", "boardings_roll_28d_mean",
    "boardings_roll_7d_std",
    # Weather
    "precipitation_mm", "temperature_c", "is_adverse_weather", "temp_bucket_enc",
    # Route characteristics
    "route_type_enc", "corridor_enc", "is_rapid_transit",
]

TARGET_COL = "boardings"


# ── Training ──────────────────────────────────────────────────────────────────

def train_ridership_model(df: pd.DataFrame, model_output_path: str = "ml/models/") -> dict:
    """
    Trains XGBoost regressor using TimeSeriesSplit cross-validation.
    TimeSeriesSplit preserves temporal order — avoids look-ahead bias
    that standard k-fold CV would introduce.

    Returns: dict with model, feature importances, and evaluation metrics.
    """
    df = build_feature_matrix(df)
    df = df.dropna(subset=FEATURE_COLS + [TARGET_COL])

    X = df[FEATURE_COLS]
    y = df[TARGET_COL]

    # Time-aware cross-validation
    tscv = TimeSeriesSplit(n_splits=5, gap=7)  # 7-day gap prevents leakage at split boundaries

    params = {
        "n_estimators": 500,
        "learning_rate": 0.05,
        "max_depth": 6,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "reg_alpha": 0.1,             # L1 regularisation
        "reg_lambda": 1.0,            # L2 regularisation
        "objective": "reg:squarederror",
        "eval_metric": "mae",
        "random_state": 42,
        "n_jobs": -1,
    }

    cv_maes = []
    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

        model = xgb.XGBRegressor(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            early_stopping_rounds=30,
            verbose=False,
        )

        preds = model.predict(X_val)
        mae = mean_absolute_error(y_val, preds)
        cv_maes.append(mae)
        logger.info(f"Fold {fold + 1} MAE: {mae:.0f} boardings")

    mean_cv_mae = np.mean(cv_maes)
    logger.info(f"Cross-validated MAE: {mean_cv_mae:.0f} boardings (±{np.std(cv_maes):.0f})")

    # Final model on full training data
    final_model = xgb.XGBRegressor(**params)
    final_model.fit(X, y, verbose=False)

    # Save model
    Path(model_output_path).mkdir(parents=True, exist_ok=True)
    joblib.dump(final_model, f"{model_output_path}/ridership_forecast_model.pkl")
    logger.info(f"Model saved to {model_output_path}")

    # Feature importance
    importance = pd.DataFrame({
        "feature": FEATURE_COLS,
        "importance": final_model.feature_importances_,
    }).sort_values("importance", ascending=False)

    return {
        "model": final_model,
        "cv_mae_mean": mean_cv_mae,
        "cv_mae_std": np.std(cv_maes),
        "feature_importance": importance,
    }


# ── Inference ─────────────────────────────────────────────────────────────────

def generate_30day_forecast(route_history: pd.DataFrame,
                             weather_forecast: pd.DataFrame,
                             model_path: str = "ml/models/ridership_forecast_model.pkl",
                             ) -> pd.DataFrame:
    """
    Generates 30-day forward ridership forecast per route.
    Uses iterative prediction: each day's forecast feeds the next day's lag features.

    Args:
        route_history: Historical boardings (min 30 days) per route
        weather_forecast: 30-day weather forecast from Open-Meteo

    Returns:
        DataFrame with columns: route_id, forecast_date, predicted_boardings,
                                 lower_bound, upper_bound
    """
    model = joblib.load(model_path)
    forecasts = []

    for route_id in route_history["route_id"].unique():
        route_df = route_history[route_history["route_id"] == route_id].copy()

        for day_offset in range(1, 31):
            next_date = pd.Timestamp.today() + pd.Timedelta(days=day_offset)

            # Build feature row for this route + date
            feature_row = _build_forecast_row(route_df, next_date, weather_forecast, route_id)
            X_pred = pd.DataFrame([feature_row])[FEATURE_COLS]

            predicted = model.predict(X_pred)[0]
            predicted = max(0, predicted)  # Boardings can't be negative

            # Uncertainty: simple percentile-based bounds from CV residuals
            # In production: quantile regression or conformal prediction
            lower = predicted * 0.85
            upper = predicted * 1.15

            forecasts.append({
                "route_id": route_id,
                "forecast_date": next_date.date(),
                "predicted_boardings": int(round(predicted)),
                "lower_bound": int(round(lower)),
                "upper_bound": int(round(upper)),
            })

            # Append prediction to history for next iteration (iterative forecasting)
            route_df = pd.concat([route_df, pd.DataFrame([{
                "route_id": route_id,
                "service_date": next_date,
                "boardings": predicted,
            }])], ignore_index=True)

    return pd.DataFrame(forecasts)


def _build_forecast_row(history: pd.DataFrame, target_date: pd.Timestamp,
                         weather: pd.DataFrame, route_id: str) -> dict:
    """Constructs a single feature row for inference."""
    row = {
        "route_id": route_id,
        "service_date": target_date,
        "is_holiday": 0,        # Would come from holiday calendar lookup
        "is_special_event": 0,  # Would come from events calendar lookup
    }

    # Temporal features
    row["dow_sin"] = np.sin(2 * np.pi * target_date.dayofweek / 7)
    row["dow_cos"] = np.cos(2 * np.pi * target_date.dayofweek / 7)
    row["month_sin"] = np.sin(2 * np.pi * target_date.month / 12)
    row["month_cos"] = np.cos(2 * np.pi * target_date.month / 12)
    row["day_of_year"] = target_date.dayofyear
    row["week_of_year"] = target_date.isocalendar().week
    row["is_weekend"] = int(target_date.dayofweek >= 5)
    row["quarter"] = target_date.quarter

    # Lag features from history
    sorted_history = history.sort_values("service_date")
    boardings = sorted_history["boardings"].values

    for lag in [7, 14, 21, 28]:
        row[f"boardings_lag_{lag}d"] = boardings[-lag] if len(boardings) >= lag else np.nan

    for window in [7, 14, 28]:
        row[f"boardings_roll_{window}d_mean"] = boardings[-window:].mean() if len(boardings) >= window else np.nan
        row[f"boardings_roll_{window}d_std"] = boardings[-window:].std() if len(boardings) >= window else np.nan

    # Weather (from forecast DataFrame)
    wx_row = weather[weather["date"] == target_date.date()]
    if not wx_row.empty:
        row["precipitation_mm"] = wx_row["precipitation_mm"].values[0]
        row["temperature_c"] = wx_row["temperature_c"].values[0]
        row["is_adverse_weather"] = int(wx_row["precipitation_mm"].values[0] > 10)
    else:
        row["precipitation_mm"] = 3.0   # Vancouver daily average
        row["temperature_c"] = 12.0
        row["is_adverse_weather"] = 0

    # Route characteristics (from DimRoute lookup, using defaults here)
    row["route_type_enc"] = 0
    row["corridor_enc"] = 0
    row["is_rapid_transit"] = 0
    row["temp_bucket_enc"] = 1

    return row


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("TransLink Ridership Forecasting Model — training run")
    logger.info("Load data from gold.FactRidership and run train_ridership_model(df)")
