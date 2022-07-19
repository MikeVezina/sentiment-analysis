import datetime

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import schema, SqlTypeNames

from config import project, dataset


class BigQueryConnector:
    def __init__(self):
        # Construct a BigQuery client object.
        self.client = bigquery.Client()

    def remove_where(self, table_id, where_key: str, where_val: str):

        print("Removing:", where_key + "=" + where_val, 'for', table_id)

        if len(where_key) == 0:
            print("Where key:", where_key, 'is invalid')
            return

        query_str = f"""
                                   DELETE
                                   FROM {table_id}
                                   WHERE {where_key} = "{where_val}"
                    """

        print("Delete Query:", query_str)
        query_job = self.client.query(query_str)

        results = query_job.result()  # Waits for job to complete.
        print("Result Stats: ", query_job.dml_stats)
        return results

    def get_by_id(self, table_id, where: dict = None):
        """
        Looks for row with id val for given id field. Return None if no row exists, otherwise an iterator is returned.
        :param table_id:
        :param id_val:
        :param id_field:
        :return:
        """

        if not where:
            where = {}

        where_clause = []

        for k in where:
            v = where[k]
            where_clause.append(str(k) + " = '" + str(v) + "'")

        if not where_clause:
            return None

        where_str = " AND ".join(where_clause)

        query_job = self.client.query(
            f"""
                           SELECT
                               *
                           FROM {table_id}
                           WHERE {where_str}
            """
        )
        results = query_job.result()  # Waits for job to complete.

        if results.total_rows == 0:
            return None

        return results

    def remove(self, table_id, ):
        query_job = self.client.query(
            f"""
                   SELECT
                       channel_id,
                       status,
                       submitted_at,
                       completed_at,
                       job_id
                   FROM {table_id}
                   WHERE status = 'Incomplete'
                   ORDER BY submitted_at DESC"""
        )

        results = query_job.result()  # Waits for job to complete.
        return results

    def write_df(self, table_id, df: pd.DataFrame, tab_schema=None, overwrite_table=False, put_insert_time=True,
                 skip_empty=True):
        '''
        Write DF to table.
        :param table_id: the id of the table: proj.dataset.table
        :param df:
        :param tab_schema:
        :param overwrite_table:
        :return:
        '''

        if skip_empty and df.empty:
            print("Empty df, skipping upload.")
            return True

        if tab_schema is None:
            tab_schema = []

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.
            schema=tab_schema,
            write_disposition="WRITE_TRUNCATE" if overwrite_table else "WRITE_APPEND",
        )

        if put_insert_time and 'insertion_time' not in df.columns:
            df['insertion_time'] = datetime.datetime.now()

        # Fill in missing columns (bq api doesn't like it when df misses cols from schema)
        if tab_schema is not None and len(tab_schema) != 0:
            bq_schema = schema._to_schema_fields(tab_schema)
            bq_schema_index = {field.name: field for field in bq_schema}

            for col_name in bq_schema_index:
                if col_name not in df.columns:
                    df[col_name] = None

                if bq_schema_index[col_name].field_type == SqlTypeNames.DATETIME:
                    df[col_name] = pd.to_datetime(df[col_name])

        job = self.client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        result = job.result()  # Wait for the job to complete.

        if result.errors is not None:
            print("Error inserting:", str(table_id), str(result.errors))

        print("Wrote", result.output_rows, 'rows to:', table_id)

        return result.errors is None

    def get_table_id(self, table):
        return f"{project}.{dataset}.{table}"
