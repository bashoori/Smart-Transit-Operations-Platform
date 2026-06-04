CREATE DATABASE airflow;
\connect translink_platform;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE TABLE IF NOT EXISTS monitoring.dq_violations (
    id              SERIAL PRIMARY KEY,
    rule_id         VARCHAR(20),
    rule_name       VARCHAR(100),
    severity        VARCHAR(20),
    table_name      VARCHAR(100),
    violation_count INT,
    sample_keys     TEXT,
    run_date        TIMESTAMP DEFAULT NOW()
);
