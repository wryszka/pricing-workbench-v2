-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver: company bureau
-- MAGIC Cleansed bureau feed **keyed on company_registration_number** (not policy_id).
-- MAGIC Adds credit_risk_tier + business_stability_score.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_company_bureau (
  CONSTRAINT valid_reg_number  EXPECT (company_registration_number IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_credit      EXPECT (credit_score BETWEEN 200 AND 900)        ON VIOLATION DROP ROW,
  CONSTRAINT valid_ccj         EXPECT (ccj_count >= 0)                          ON VIOLATION DROP ROW,
  CONSTRAINT valid_years       EXPECT (years_trading IS NULL OR years_trading BETWEEN 0 AND 200)
    ON VIOLATION DROP ROW
)
COMMENT 'Cleansed credit bureau feed keyed on company_registration_number. Bureau never sees our internal policy_id — this is the realistic data shape.'
AS
SELECT
  company_registration_number,
  CAST(credit_score AS INT)         AS credit_score,
  CAST(ccj_count AS INT)            AS ccj_count,
  CAST(years_trading AS INT)        AS years_trading,
  CAST(director_changes AS INT)     AS director_changes,
  CAST(bankruptcy_flag AS BOOLEAN)  AS bankruptcy_flag,
  CAST(filing_on_time AS BOOLEAN)   AS filing_on_time,
  CAST(employee_count_banded AS INT) AS employee_count_banded,
  CASE
    WHEN credit_score >= 700 AND ccj_count = 0 AND COALESCE(bankruptcy_flag, false) = false THEN 'Prime'
    WHEN credit_score >= 500 AND ccj_count <= 2 AND COALESCE(bankruptcy_flag, false) = false THEN 'Standard'
    WHEN credit_score >= 350 THEN 'Sub-Standard'
    ELSE 'High Risk'
  END AS credit_risk_tier,
  LEAST(100, GREATEST(0,
      ((credit_score - 200) / 7.0)
    - (ccj_count * 8)
    + (LEAST(COALESCE(years_trading, 0), 40) * 0.5)
    - (director_changes * 2)
    + (CASE WHEN filing_on_time THEN 10 ELSE 0 END)
    - (CASE WHEN bankruptcy_flag THEN 30 ELSE 0 END)
  )) AS business_stability_score,
  _ingested_at,
  _source_file
FROM raw_company_bureau
