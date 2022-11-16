import logging
logger = logging.getLogger(__name__)
logger.debug("Logging is configured.")

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
        return self.client.query(query).result().to_dataframe(create_bqstorage_client = False)

    def pre_aggregate_total_and_uniques(self) -> pandas.DataFrame:
        query = f"""
            SELECT datasett, tabell, variabel, totalt, distinkte
            FROM `{self.GCP_project}.kvalitet.metrics_count_total_and_distinct`
        """
        df = self._query_job_dataframe(query)
        return df


    def count_total_and_uniques(self, database, table, column) -> dict:
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

    def pre_aggregated_number_of_citizenships(self) -> pandas.DataFrame:
        query = f"""
            SELECT datasett, gruppe, antall 
            FROM `{self.GCP_project}.kvalitet.metrics_antall_statsborgerskap`
        """
        df = self._query_job_dataframe(query)
        return df

    def pre_aggregated_valid_fnr(self) -> pandas.DataFrame:
        query = f"""
            SELECT datasett, tabell, variabel,
                fnr_total_count, fnr_invalid_format, fnr_invalid_first_digit, fnr_invalid_date, fnr_invalid_control,
                dnr_total_count, dnr_invalid_format, dnr_invalid_first_digit, dnr_invalid_date, dnr_invalid_control
            FROM `{self.GCP_project}.kvalitet.metrics_count_valid_fnr_dnr` 
        """
        df = self._query_job_dataframe(query)
        return df

    def pre_aggregated_count_group_by(self) -> pandas.DataFrame:
        """
        Get pre-aggregated data in kvalitet.metrics_count_group_by

        Return: dataframe
        """

        query = f"""
        SELECT datasett, tabell, variabel, gruppe, antall
        FROM `{self.GCP_project}.kvalitet.metrics_count_group_by` 
        """
        df = self._query_job_dataframe(query)
        return df

    def group_by_and_count(self, database, table, column) -> dict:
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

    def pre_aggregated_latest_timestamp(self) -> pandas.DataFrame:
        query = f"""
            SELECT datasett, tabell, variabel, latest_timestamp
            FROM `{self.GCP_project}.kvalitet.metrics_latest_timestamp`
        """
        df = self._query_job_dataframe(query)
        return df

    def latest_timestamp_from_string(self, database, table, column, parse_format) -> str:
        """
        Description
        -----------
        Get the latest (max) timestamp of a BigQuery table where the timestamp is column of type STRING

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

    def latest_timestamp_from_datetime(self, database, table, column) -> str:
        """
        Description
        -----------
        Get the latest (max) timestamp of a BigQuery table where the timestamp is a column of type DATETIME

        Return
        ------
        str: the latest timestamp on the format 'YYYY-MM-DDTHH:MM:SS.XXXXXXZ'
        """
        query = f"""
            SELECT
                FORMAT_DATETIME("%Y-%m-%d %H:%M:%S", MAX({column})) as latest_timestamp,
            FROM `{self.GCP_project}.{database}.{table}`
        """
        df = self._query_job_dataframe(query)
        result = {}
        result["timestamp"] = df["latest_timestamp"][0]
        return result

    # Functions spesific for DSF_SITUASJONSUTTAK
    def dsfsit_latest_timestamp(self) -> dict:
        """
        Description
        -----------
        Get the latest (max) timestamp for when DSF_SITUASJONSUTTAK was run
        """
        query = f"""
            select FORMAT_TIMESTAMP("%d-%m-%Y %H:%M:%S", max(tidspunkt)) as latest_timestamp
                from `{self.GCP_project}.kvalitet.qa_nullvalue_columns`
                where datasett='klargjort' and tabell='dsf_situasjonsuttak'
        """
        df = self._query_job_dataframe(query)
        result = {}
        result["timestamp"] = df["latest_timestamp"][0]
        return result

    def dsfsit_qa_nullvals_latest(self) -> pandas.DataFrame:
        """
        Description
        -----------
        Gets quality-info on which vairables in dsf_situasjonsuttak that contains missing values, how many rows, and the percentage
        """
        query = f"""
            with ranked_values as 
            (
              select tidspunkt, kolonne, ant_nullvals, pct_nullvals,
              ROW_NUMBER() OVER (PARTITION BY datasett, tabell, kolonne order by tidspunkt desc) as rank
              from `kvalitet.qa_nullvalue_columns` 
              where datasett='klargjort' and tabell='dsf_situasjonsuttak'
            ),
            max_dato as 
            (
              select cast(max(tidspunkt) AS DATE) as dato 
              from ranked_values
            )
            select kolonne, ant_nullvals, pct_nullvals
            from ranked_values a, max_dato d
            where a.rank=1
            and tidspunkt > d.dato  -- This makes sure we only get variables from the latest run
        """
        df = self._query_job_dataframe(query)
        return df

    def dsfsit_qa_nullvals_diff(self) -> pandas.DataFrame:
        """
        Description
        -----------
        Gets quality-info on which vairables in dsf_situasjonsuttak that has a rise or a drop in number of missing values
        Has a filter of at least 0.1 difference
        """
        query = f"""
            with differanser as 
            (
              select tidspunkt, kolonne, ant_nullvals, pct_nullvals,
              IFNULL((pct_nullvals - LAG(pct_nullvals) OVER (PARTITION BY kolonne ORDER BY tidspunkt)), pct_nullvals) as pct_diff
              from `kvalitet.qa_nullvalue_columns`
              where datasett='klargjort' and tabell='dsf_situasjonsuttak'
            ), ranked_nyeste as 
            (
              SELECT *, ROW_NUMBER() OVER (PARTITION BY kolonne ORDER BY tidspunkt desc) as ranked
              from differanser
            ),
            max_dato as 
            (
              select cast(max(tidspunkt) AS DATE) as dato 
              from differanser
            )
            select d.dato, kolonne, ant_nullvals, pct_nullvals, round(pct_diff,2) as pct_diff_last 
            from ranked_nyeste, max_dato d where ranked=1 and tidspunkt > d.dato 
            and (pct_diff >= 0.1 or pct_diff <= -0.1)
        """
        df = self._query_job_dataframe(query)
        return df

if __name__ == "__main__":
    BQ = BigQuery()
