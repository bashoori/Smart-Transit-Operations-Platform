-- ============================================================
-- TransLink Smart Transit Platform
-- Data Quality Checks — Production SQL Suite
-- Runs nightly after Silver load, before Gold promotion
-- All violations written to monitoring.dq_violations
-- ============================================================

-- ── Rule 1: Duplicate GPS Records ────────────────────────────────────────────
-- Same vehicle, same timestamp should never appear twice in Silver.
-- Root cause: duplicate message delivery from GTFS-RT feed.

INSERT INTO monitoring.dq_violations (rule_id, rule_name, severity, table_name, violation_count, sample_keys, run_date)
SELECT
    'DQ-001',
    'duplicate_gps_records',
    'CRITICAL',
    'silver.vehicle_positions',
    COUNT(*),
    STRING_AGG(CONCAT(vehicle_id, '@', CONVERT(VARCHAR, recorded_at, 120)), ' | '),
    GETUTCDATE()
FROM (
    SELECT vehicle_id, recorded_at, COUNT(*) AS cnt
    FROM silver.vehicle_positions
    WHERE CAST(recorded_at AS DATE) = CAST(GETUTCDATE() AS DATE)
    GROUP BY vehicle_id, recorded_at
    HAVING COUNT(*) > 1
) dupes;

-- ── Rule 2: Missing GPS — Trips Dark for > 15 Minutes ────────────────────────
-- Any in-service trip with no GPS signal for 15+ minutes needs an operations alert.

INSERT INTO monitoring.dq_violations (rule_id, rule_name, severity, table_name, violation_count, sample_keys, run_date)
SELECT
    'DQ-002',
    'missing_gps_active_trip',
    'WARNING',
    'silver.vehicle_positions',
    COUNT(*),
    STRING_AGG(t.trip_id, ' | '),
    GETUTCDATE()
FROM silver.trips t
LEFT JOIN (
    SELECT trip_id, MAX(recorded_at) AS last_signal
    FROM silver.vehicle_positions
    GROUP BY trip_id
) gps ON t.trip_id = gps.trip_id
WHERE t.service_date = CAST(GETUTCDATE() AS DATE)
  AND t.trip_status = 'IN_SERVICE'
  AND (
      gps.last_signal IS NULL
      OR DATEDIFF(MINUTE, gps.last_signal, GETUTCDATE()) > 15
  );

-- ── Rule 3: Boardings Exceed Vehicle Capacity ─────────────────────────────────
-- Passenger counts above physical capacity are data errors (APC sensor fault,
-- wrong vehicle assignment, or data entry error).

INSERT INTO monitoring.dq_violations (rule_id, rule_name, severity, table_name, violation_count, sample_keys, run_date)
SELECT
    'DQ-003',
    'boardings_exceed_capacity',
    'WARNING',
    'silver.ridership',
    COUNT(*),
    STRING_AGG(CONCAT(r.trip_id, '/', r.stop_id), ' | '),
    GETUTCDATE()
FROM silver.ridership r
JOIN gold.DimVehicle v ON r.vehicle_id = v.vehicle_id
WHERE r.service_date = CAST(GETUTCDATE() AS DATE)
  AND r.boardings > (v.capacity_seated + v.capacity_standing)
  AND v.capacity_seated IS NOT NULL;

-- ── Rule 4: Schedule Adherence Outliers ───────────────────────────────────────
-- Trips arriving more than 60 minutes early or late are likely data errors,
-- not real service events. Flag for human review before Gold promotion.

INSERT INTO monitoring.dq_violations (rule_id, rule_name, severity, table_name, violation_count, sample_keys, run_date)
SELECT
    'DQ-004',
    'schedule_adherence_outlier',
    'WARNING',
    'silver.ridership',
    COUNT(*),
    STRING_AGG(trip_id, ' | '),
    GETUTCDATE()
FROM silver.ridership
WHERE service_date = CAST(GETUTCDATE() AS DATE)
  AND actual_time IS NOT NULL
  AND ABS(DATEDIFF(MINUTE, scheduled_time, actual_time)) > 60;

-- ── Rule 5: Late Data — Compass Card File Missing ────────────────────────────
-- Compass Card daily extract must arrive and be loaded by 08:00 Pacific.
-- If the count for today is zero after that threshold, alert the team.

INSERT INTO monitoring.dq_violations (rule_id, rule_name, severity, table_name, violation_count, sample_keys, run_date)
SELECT
    'DQ-005',
    'compass_file_not_received',
    'CRITICAL',
    'bronze.compass_transactions_raw',
    1,
    'No records for today',
    GETUTCDATE()
WHERE NOT EXISTS (
    SELECT 1
    FROM bronze.compass_transactions_raw
    WHERE CAST(ingested_at AS DATE) = CAST(GETUTCDATE() AS DATE)
)
AND DATEPART(HOUR, GETUTCDATE()) >= 15;  -- 08:00 Pacific = 15:00 UTC

-- ── Rule 6: Null GPS Coordinates ─────────────────────────────────────────────
-- Vehicle positions with null lat/lon are unusable for operations and ML.

INSERT INTO monitoring.dq_violations (rule_id, rule_name, severity, table_name, violation_count, sample_keys, run_date)
SELECT
    'DQ-006',
    'null_gps_coordinates',
    'WARNING',
    'silver.vehicle_positions',
    COUNT(*),
    STRING_AGG(vehicle_id, ' | '),
    GETUTCDATE()
FROM silver.vehicle_positions
WHERE CAST(recorded_at AS DATE) = CAST(GETUTCDATE() AS DATE)
  AND (latitude IS NULL OR longitude IS NULL
       OR latitude = 0 OR longitude = 0
       -- Out-of-bounds for Metro Vancouver service area
       OR latitude NOT BETWEEN 48.9 AND 49.5
       OR longitude NOT BETWEEN -123.5 AND -122.2);

-- ── Rule 7: Fare Revenue Anomaly ─────────────────────────────────────────────
-- Today's total fare revenue should be within ±30% of the 28-day rolling average.
-- Larger deviations may indicate data load failure or upstream system issues.

WITH rolling_avg AS (
    SELECT AVG(daily_revenue) AS avg_revenue
    FROM (
        SELECT CAST(ingested_at AS DATE) AS txn_date,
               SUM(fare_amount) AS daily_revenue
        FROM bronze.compass_transactions_raw
        WHERE CAST(ingested_at AS DATE) BETWEEN
              CAST(DATEADD(DAY, -29, GETUTCDATE()) AS DATE)
          AND CAST(DATEADD(DAY, -1, GETUTCDATE()) AS DATE)
        GROUP BY CAST(ingested_at AS DATE)
    ) history
),
today AS (
    SELECT SUM(fare_amount) AS today_revenue
    FROM bronze.compass_transactions_raw
    WHERE CAST(ingested_at AS DATE) = CAST(GETUTCDATE() AS DATE)
)
INSERT INTO monitoring.dq_violations (rule_id, rule_name, severity, table_name, violation_count, sample_keys, run_date)
SELECT
    'DQ-007',
    'fare_revenue_anomaly',
    'WARNING',
    'bronze.compass_transactions_raw',
    1,
    CONCAT('Today: $', ROUND(t.today_revenue, 0),
           ' | 28d avg: $', ROUND(r.avg_revenue, 0),
           ' | deviation: ', ROUND(ABS(t.today_revenue - r.avg_revenue) / NULLIF(r.avg_revenue, 0) * 100, 1), '%'),
    GETUTCDATE()
FROM today t, rolling_avg r
WHERE ABS(t.today_revenue - r.avg_revenue) / NULLIF(r.avg_revenue, 0) > 0.30;

-- ── DQ Summary View ──────────────────────────────────────────────────────────
-- Used by the Operations Dashboard to show today's data health status.

CREATE OR ALTER VIEW monitoring.vw_dq_daily_summary AS
SELECT
    CAST(run_date AS DATE) AS check_date,
    COUNT(*) AS total_violations,
    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN severity = 'WARNING' THEN 1 ELSE 0 END) AS warning_count,
    CASE
        WHEN SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) > 0 THEN 'RED'
        WHEN SUM(CASE WHEN severity = 'WARNING' THEN 1 ELSE 0 END) > 3 THEN 'AMBER'
        ELSE 'GREEN'
    END AS health_status,
    MIN(run_date) AS first_check_time,
    MAX(run_date) AS last_check_time
FROM monitoring.dq_violations
WHERE CAST(run_date AS DATE) >= CAST(DATEADD(DAY, -7, GETUTCDATE()) AS DATE)
GROUP BY CAST(run_date AS DATE);
