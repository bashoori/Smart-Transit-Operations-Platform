# Architecture Decision Records

## ADR-001 — Medallion Architecture over Single-Layer Warehouse

**Status**: Accepted  
**Date**: 2024-01

### Context
Transit data arrives from multiple systems with varying quality and latency. Previous architecture wrote directly to a reporting database, which meant bad data reached dashboards before it was caught.

### Decision
Three-layer Medallion (Bronze / Silver / Gold) with explicit quality gates between each layer.

### Consequences
- Bronze provides a recovery point for any downstream failure — we can always re-process
- Silver is the contract layer — downstream consumers get standardised, validated data
- Gold is treated as an SLA'd product — tested, monitored, and never overwritten without a DQ pass
- Added complexity vs. single-layer, but the operational reliability improvement is worth it

---

## ADR-002 — Delta Tables over Plain Parquet for Silver and Gold

**Status**: Accepted

### Context
Plain Parquet files are portable but don't support ACID transactions, upserts, or schema evolution — all of which are needed for Silver's SCD Type 2 dimension updates and Gold's incremental fact loads.

### Decision
Delta Lake format for all Silver and Gold tables.

### Consequences
- Time-travel queries (audit / replay)
- Schema evolution without pipeline restarts
- Z-order clustering for performance on common query patterns (date, route_id)
- Vendor portability: Delta is open-source, works on Databricks, Fabric, and local

---

## ADR-003 — Star Schema in Gold vs. Wide Flat Tables

**Status**: Accepted

### Context
Analytics tools (Power BI, ad-hoc SQL) can work with either normalised star schemas or denormalised flat tables. Flat tables are simpler to query but expensive to maintain when dimensions change.

### Decision
Star schema with four Fact tables and five Dimension tables.

### Consequences
- Dimension changes (new route, stop rename) happen in one place only
- Power BI DirectLake can use the star schema natively with automatic relationship detection
- Query patterns (filter by route, aggregate by date) match the star schema optimally
- Slightly more complex SQL for ad-hoc analysts — mitigated by view layer and PBI

---

## ADR-004 — Airflow for Orchestration vs. Azure Data Factory Alone

**Status**: Accepted (Portfolio: Airflow; Production: ADF + Airflow)

### Context
ADF is excellent for Azure-native pipelines but lacks Python extensibility for complex ingestion logic (GTFS-RT protobuf decoding, custom retry patterns, DQ integration).

### Decision
Airflow as primary orchestrator for Python-based pipelines. ADF for pure data movement within Azure (Blob → ADLS, SQL → SQL).

### Consequences
- Full Python control over retry logic, alerting, and DQ integration
- Easier local development and testing (Docker Compose vs. Azure subscription)
- ADF handles high-volume SQL and Blob-level operations more efficiently
- Dual-orchestrator adds operational complexity — mitigated by clear boundary (Python logic → Airflow; data movement → ADF)

---

## ADR-005 — XGBoost vs. LSTM for Ridership Forecasting

**Status**: Accepted

### Context
Ridership has strong temporal patterns (weekday/weekend, seasonal) which LSTMs handle naturally. However, LSTMs require more data, are harder to debug, and slower to train incrementally.

### Decision
XGBoost with lag/rolling features and cyclical temporal encoding.

### Consequences
- Feature importance is interpretable — planners can understand *why* a route is forecasted high
- Incremental retraining is fast (daily updates in seconds)
- Lag feature engineering makes temporal patterns explicit and auditable
- If data volume grows significantly (real-time stop-level prediction), LSTM or Prophet hybrid would be revisited
