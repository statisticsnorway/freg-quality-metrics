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

    def pre_aggregated_latest_timestamp(self) -> pandas.DataFrame:
        query = f"""
            SELECT datasett, tabell, variabel, latest_timestamp
            FROM `{self.GCP_project}.kvalitet.v_latest_timestamp`
        """
        df = self._query_job_dataframe(query)
        return df

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


if __name__ == "__main__":
    BQ = BigQuery()
