-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: market benchmark
-- MAGIC Cleansed vendor market-rate aggregates. DLT expectations enforce
-- MAGIC plausibility ranges + non-null joins keys.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_market_benchmark (
  CONSTRAINT valid_match_key EXPECT (match_key_sic_region IS NOT NULL)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_median_rate EXPECT (market_median_rate > 0 AND market_median_rate < 50)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_year EXPECT (year BETWEEN 2018 AND 2030)
    ON VIOLATION DROP ROW,
  CONSTRAINT valid_quarter EXPECT (quarter BETWEEN 1 AND 4)
    ON VIOLATION DROP ROW
)
COMMENT 'Cleansed market-rate aggregates per SIC × region × year-quarter. Join key: match_key_sic_region. Enables point-in-time market-rate lookups for historical policy exposures.'
AS
SELECT
  match_key_sic_region,
  CAST(sic_code AS STRING)               AS sic_code,
  CAST(region AS STRING)                 AS region,
  CAST(year AS INT)                      AS year,
  CAST(quarter AS INT)                   AS quarter,
  MAKE_DATE(year, (quarter - 1) * 3 + 1, 1) AS quarter_start_date,
  CAST(market_median_rate AS DOUBLE)     AS market_median_rate,
  CAST(competitor_a_min_rate AS DOUBLE)  AS competitor_a_min_rate,
  CAST(competitor_b_min_rate AS DOUBLE)  AS competitor_b_min_rate,
  CAST(price_index AS DOUBLE)            AS price_index,
  _ingested_at,
  _source_file
FROM raw_market_benchmark
