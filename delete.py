from freg_quality_metrics.bigquery import BigQuery

GCP_project = "dev-freg-3896"
database="inndata"
table="v_identifikasjonsnummer"


BQ = BigQuery()

def get_query(id): 
    return f"""
        WITH
table AS (
    SELECT folkeregisteridentifikator  FROM `{GCP_project}.{database}.{table}`
    UNION DISTINCT
    SELECT '{id}' FROM `{GCP_project}.{database}.{table}`
),
format_check AS (
  SELECT folkeregisteridentifikator as fnr FROM table
  WHERE REGEXP_CONTAINS(folkeregisteridentifikator,"^[0-9]{{11}}$")
),
date_input AS (
    SELECT
        fnr,
        CAST(SUBSTR(fnr,5,2) as INT64) AS year_short, 
        CAST(SUBSTR(fnr,7,3) AS INT64) as id,
        IF(CAST(SUBSTR(fnr,1,1) AS INT64) > 3,CAST(SUBSTR(fnr,1,2) AS INT64)-40,CAST(SUBSTR(fnr,1,2) AS INT64)) as day,
        CAST(SUBSTR(fnr,3,2) AS INT64) as month,
        CAST(SUBSTR(fnr,1,1) AS INT64) <= 3 as is_fnr
    FROM format_check
),
format_year AS (
  SELECT 
    CASE
      WHEN id <= 499 THEN 1900 + year_short
      WHEN id >= 500 AND id <= 749 AND year_short >= 54 THEN 1800 + year_short
      WHEN id >= 500 AND id <= 999 AND year_short <=39 THEN 2000 + year_short
      WHEN id >= 900 AND year_short >= 40 THEN 1900 + year_short
      ELSE 0
      END
    AS year,
    day, month, fnr, is_fnr
  FROM date_input
),
date_check AS (
  SELECT is_fnr, fnr
  FROM format_year
  WHERE SAFE.DATE(year, month, day) IS NOT NULL
),
control_check AS (
  SELECT 
    is_fnr
  FROM date_check
  WHERE CAST(SUBSTR(fnr,10,1) AS INT64) = MOD(11-MOD(
    3*CAST(SUBSTR(fnr,1,1) AS INT64)+7*CAST(SUBSTR(fnr,2,1) AS INT64)+6*CAST(SUBSTR(fnr,3,1) AS INT64)
      +1*CAST(SUBSTR(fnr,4,1) AS INT64)+8*CAST(SUBSTR(fnr,5,1) AS INT64)+9*CAST(SUBSTR(fnr,6,1) AS INT64)
      +4*CAST(SUBSTR(fnr,7,1) AS INT64)+5*CAST(SUBSTR(fnr,8,1) AS INT64)+2*CAST(SUBSTR(fnr,9,1) AS INT64),11)
    , 11)
  AND CAST(SUBSTR(fnr,11,1) AS INT64) = MOD(11-MOD(
    5*CAST(SUBSTR(fnr,1,1) AS INT64)+4*CAST(SUBSTR(fnr,2,1) AS INT64)+3*CAST(SUBSTR(fnr,3,1) AS INT64)
      +2*CAST(SUBSTR(fnr,4,1) AS INT64)+7*CAST(SUBSTR(fnr,5,1) AS INT64)+6*CAST(SUBSTR(fnr,6,1) AS INT64)
      +5*CAST(SUBSTR(fnr,7,1) AS INT64)+4*CAST(SUBSTR(fnr,8,1) AS INT64)+3*CAST(SUBSTR(fnr,9,1) AS INT64)
      +2*CAST(SUBSTR(fnr,10,1) AS INT64),11)
    , 11)
)
SELECT 'total_count' as label, COUNT(*) as count FROM table
UNION ALL SELECT 'valid_format_count', COUNT(*) FROM format_check
UNION ALL SELECT 'valid_control_count', COUNT(*) FROM control_check
UNION ALL SELECT 'valid_date_count', COUNT(*) FROM date_check
UNION ALL SELECT 'fnr', COUNT(*) FROM control_check WHERE is_fnr
UNION ALL SELECT 'dnr', COUNT(*) FROM control_check WHERE NOT is_fnr
"""







id = "65038300827"
result = BQ.valid_and_invalid_fnr(query = get_query(id))
print(result)









