import logging
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")

from datetime import datetime

import pandas
from google.cloud import bigquery


class BigQuery:
    def __init__(self, GCP_project="dev-freg-3896"):
        self.client = bigquery.Client(project=GCP_project)
        self.GCP_project = GCP_project

    def _query_job_dataframe(self, query: str) -> pandas.DataFrame:
        """
        Description: Internal method for this class.
        Parameters: query string.
        Returns: pandas dataframe.
        """
        logger.debug('Retrieving query and converting to dataframe.')
        return self.client.query(query).result().to_dataframe()

    def count_total_and_uniques(
        self,
        database="inndata",
        table="v_identifikasjonsnummer",
        column="folkeregisteridentifikator",
    ) -> dict:
        """
        Description
        -----------
        Count the rows for a column:
        * total number of rows
        * unique number of rows

        Return
        ------
        dict: {
            'total': int (count),
            'unique': int (count)
        }
        """
        query = f"""
            SELECT
                COUNT({column}) AS total,
                COUNT(DISTINCT({column})) AS unique
            FROM `{self.GCP_project}.{database}.{table}`
        """
        df = self._query_job_dataframe(query)
        result = {"total": float(df.total[0]), "unique": float(df.unique[0])}

        return result

    def valid_and_invalid_fnr(
        self,
        database="inndata",
        table="v_identifikasjonsnummer",
        column="folkeregisteridentifikator",
    ) -> dict:
        """
        Description
        -----------
        Check the validity of folkeregisteridentifikator:
        * Check format (11 digits).
        * Check control digits (2 last digits).
        * Check valid date (including D numbers).

        Return
        ------
        dict: {
            'valid_fnr': int (count),
            'valid_dnr': int (count),
            'invalid_format': int (count),
            'invalid_date': int (count),
            'invalid_control': int (count),
        }
        """
        query = self._get_valid_and_invalid_fnr_query(database, table)

        df = self._query_job_dataframe(query)
        df.set_index('label',inplace=True)
        count = df['count']
        result = {
            "valid_fnr": count['fnr'],
            "valid_dnr": count['dnr'],
            "invalid_format": count['total_count']-count['valid_format_count'],
            "invalid_date": count['valid_format_count']-count['valid_date_count'],
            "invalid_control": count['valid_date_count']-count['valid_control_count']
        }
        return result


    def count_hendelsetype(self) -> dict:
        """
        Description
        -----------
        Count the occurence of each hendelsetype in kildedata.hendelse_persondok
        * Group by 'hendelsetype'.
        * Count no. of rows per hendelsetype value.

        Return
        ------
        dict: keys - each value,
              values - int, occurence of the value.
        """

        query = f"""
        SELECT hendelsetype AS key, 
            count(hendelsetype) as occurence
        FROM `{self.GCP_project}.kildedata.hendelse_persondok` 
        GROUP BY hendelsetype
        """
        df = self._query_job_dataframe(query)
        result = {}
        for i, row in df.iterrows():
            result[row.key] = row.occurence

        return result

    def count_statsborgerskap(self) -> dict:
        """
        Description
        -----------
        Count the number of concurrent statsborgerskap

        Return
        ------
        dict: keys - statsborgerskap_1, statsborgerskap_2, etc,
              values - int, occurence of the value.
        """

        query = f"""
            WITH stb AS 
            (
              SELECT folkeregisteridentifikator, statsborgerskap, ROW_NUMBER() OVER (
                          PARTITION BY folkeregisteridentifikator 
                          ORDER BY 
                              CASE WHEN statsborgerskap = 'NOR' THEN 0 ELSE 1 END ASC,
                              statsborgerskap ASC
                      ) AS nbr
              FROM inndata.v_statsborgerskap
            ),
            pivotert AS
            (
              SELECT *
              FROM stb
              PIVOT
              (
                  MAX(statsborgerskap) AS statsborgerskap 
                  FOR nbr IN (1,2,3,4,5)
              )
            )
            SELECT "1" as key, count(*) as occurence FROM pivotert where (statsborgerskap_1 is not null and statsborgerskap_2 is null) 
            UNION ALL 
            SELECT "2" as key, count(*) as occurence FROM pivotert where (statsborgerskap_2 is not null and statsborgerskap_3 is null) 
            UNION ALL 
            SELECT "3" as key, count(*) as occurence FROM pivotert where (statsborgerskap_3 is not null and statsborgerskap_4 is null) 
            UNION ALL 
            SELECT "4" as key, count(*) as occurence FROM pivotert where (statsborgerskap_4 is not null and statsborgerskap_5 is null) 
            UNION ALL 
            SELECT "5" as key, count(*) as occurence FROM pivotert where (statsborgerskap_5 is not null) 
        """
        df = self._query_job_dataframe(query)
        result = {}
        for i, row in df.iterrows():
            result[row.key] = row.occurence

        return result


    def group_by_and_count(
        self, database="inndata", table="v_status", column="status"
    ) -> dict:
        """
        Description
        -----------
        Count the occurence of each 'column' value (e.g., statuses).
        Limit to only one record per person, which is the latest, based
        on gyldighetstidspunkt.
        * Group by 'column'.
        * Count no. of rows per column group.

        Return
        ------
        dict: keys - each value,
              values - int, occurence of the value.
        """
        query = f"""
            WITH ordered_records_per_person AS (
                SELECT t.*, ROW_NUMBER() OVER (
                    PARTITION BY folkeregisteridentifikator 
                    ORDER BY gyldighetstidspunkt DESC
                ) AS row_number
                FROM `{self.GCP_project}.{database}.{table}` AS t
            )
            SELECT
                {column} AS key,
                COUNT({column}) AS occurence
            FROM ordered_records_per_person 
            WHERE row_number = 1
            GROUP BY {column}
        """
        df = self._query_job_dataframe(query)
        result = {}
        for i, row in df.iterrows():
            result[row.key] = row.occurence

        return result

    def latest_timestamp(
            self,
            database="kildedata",
            table="hendelse_persondok",
            column="md_timestamp",
            parse_format="%d-%m-%Y %H:%M:%S", ) -> str:
        """
        Description
        -----------
        Get the latest (max) md_timestamp of a BigQuery table:

        Return
        ------
        str: the latest timestamp on the format 'YYYY-MM-DDTHH:MM:SS.XXXXXXZ'
        """
        query = f"""
            SELECT
                FORMAT_DATETIME("%Y-%m-%d %H:%M:%S", MAX(PARSE_TIMESTAMP("{parse_format}", {column}))) as latest_timestamp,
            FROM `{self.GCP_project}.{database}.{table}`
        """
        df = self._query_job_dataframe(query)
        result = {}
        result["timestamp"] = df["latest_timestamp"][0]
        return result

    def _get_valid_and_invalid_fnr_query(self,database: str,table: str) -> str:
        return f"""
        WITH
format_check AS (
  SELECT folkeregisteridentifikator as fnr FROM `{self.GCP_project}.{database}.{table}`
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
SELECT 'total_count' as label, COUNT(*) as count FROM `{self.GCP_project}.{database}.{table}`
UNION ALL SELECT 'valid_format_count', COUNT(*) FROM format_check
UNION ALL SELECT 'valid_control_count', COUNT(*) FROM control_check
UNION ALL SELECT 'valid_date_count', COUNT(*) FROM date_check
UNION ALL SELECT 'fnr', COUNT(*) FROM control_check WHERE is_fnr
UNION ALL SELECT 'dnr', COUNT(*) FROM control_check WHERE NOT is_fnr
"""

if __name__ == "__main__":
    BQ = BigQuery()
