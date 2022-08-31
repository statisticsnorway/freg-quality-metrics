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

    def _valid_fnr_or_dnr(self, number):
        """
        Description
        -----------
        Check the validity of a fnr or dnr:
        * Check format (11 digits).
        * Check control digits (2 last digits).
        * Check valid date (including D numbers).
        * Check if it is fnr or dnr.

        Parameters
        ----------
        number: str of ints, should be 11 digits.

        Returns
        -------
        bool: True if valid number, False if not.
        str: an explanation to the check.
            If the number is valid:
            * 'fnr'
            * 'dnr'
            Else (invalid number):
            * 'format' (not 11 digits)
            * 'date' (wrong date)
            * 'control' (wrong control digits)
        """
        # Check if number is a string of 11 digits
        if not (number.isdigit() & (len(number) == 11)):
            return False, "format"

        # Fnr or Dnr?
        if int(number[0]) > 3:
            no_type = "dnr"
        else:
            no_type = "fnr"

        # Check date
        try:
            day = int(number[0:2])
            if no_type == "dnr":
                day -= 40
            month = int(number[2:4])
            year = int(number[4:6])
            d = datetime(year=year, month=month, day=day)
        except ValueError:
            return False, f"date"

        # Check control digits
        def control(weights, number):
            total = 0
            for i, w in enumerate(weights):
                total += w * int(number[i])
            rest = total % 11
            if rest == 0:
                return "0"
            else:
                return str(11 - rest)

        # Check control digit 1 (second last)
        if number[9] != control([3, 7, 6, 1, 8, 9, 4, 5, 2], number):
            return False, "control"

        # Check control digit 2 (last)
        if number[10] != control([5, 4, 3, 2, 7, 6, 5, 4, 3, 2], number):
            return False, "control"

        return True, no_type

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
        query = f"""
            SELECT 
                folkeregisteridentifikator AS fnr
            FROM `{self.GCP_project}.{database}.{table}`
        """
        df = self._query_job_dataframe(query)
        result = {
            "valid_fnr": 0,
            "valid_dnr": 0,
            "invalid_format": 0,
            "invalid_date": 0,
            "invalid_control": 0,
        }

        # Check all numbers
        for fnr in df["fnr"]:
            valid, msg = self._valid_fnr_or_dnr(fnr)
            if valid:
                if msg == "fnr":
                    result["valid_fnr"] += 1
                elif msg == "dnr":
                    result["valid_dnr"] += 1
            else:
                if msg == "format":
                    result["invalid_format"] += 1
                elif msg == "date":
                    result["invalid_date"] += 1
                elif msg == "control":
                    result["invalid_control"] += 1

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

    def latest_timestamp(self, database="kildedata", table="hendelse_persondok") -> str:
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
                MAX(md_timestamp) as latest_timestamp,
            FROM `{self.GCP_project}.{database}.{table}`
        """
        df = self._query_job_dataframe(query)
        result = {}
        result[table] = df["latest_timestamp"][0]
        return result


if __name__ == "__main__":
    BQ = BigQuery()
