# Loads jobs from big query table 'process_jobs'
import datetime

import pandas

from schemas import playlist_item_schema, comments_schema
from big_query import BigQueryConnector

JOBS_TABLE = "jobs"


class Jobs:
    def __init__(self, bq):
        self.bq: BigQueryConnector = bq

    def load_new_jobs(self):
        table = self.bq.get_table_id(JOBS_TABLE)


        query_job = self.bq.client.query(
            f"""
            SELECT
                channel_id,
                status,
                submitted_at,
                completed_at,
                job_id
            FROM {table}
            WHERE status = 'Incomplete'
            ORDER BY submitted_at DESC"""
        )

        results = query_job.result()  # Waits for job to complete.
        return results


    def complete_job(self, job_id, status="Complete"):
        table = self.bq.get_table_id(JOBS_TABLE)
        query_job = self.bq.client.query(
        f"""
        UPDATE {table} 
        SET completed_at = '{datetime.datetime.now()}',
            status = '{status}'
        WHERE job_id = '{job_id}'
        """)

        results = query_job.result()  # Waits for job to complete.
        return results