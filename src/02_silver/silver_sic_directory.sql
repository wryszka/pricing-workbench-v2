-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: SIC directory
-- MAGIC Cleansed UK SIC 2007 directory with internal risk tier.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_sic_directory (
  CONSTRAINT valid_sic  EXPECT (sic_code IS NOT NULL AND LENGTH(sic_code) >= 3) ON VIOLATION DROP ROW,
  CONSTRAINT valid_tier EXPECT (internal_risk_tier IN ('Low', 'Medium', 'High')) ON VIOLATION DROP ROW
)
COMMENT 'UK SIC 2007 industry directory with internal risk tiers assigned by the pricing team.'
AS
SELECT
  CAST(sic_code AS STRING)           AS sic_code,
  CAST(division AS STRING)           AS division,
  CAST(description AS STRING)        AS description,
  CAST(internal_risk_tier AS STRING) AS internal_risk_tier,
  _ingested_at,
  _source_file
FROM raw_sic_directory
