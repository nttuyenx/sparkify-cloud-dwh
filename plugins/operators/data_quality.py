from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    @staticmethod
    def check_rows(redshift_hook, table):
        records = redshift_hook.get_records(
            """
            SELECT COUNT(*) FROM {}
            """.format(table))
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed, {} table returned no
                             results".format(table))
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed, {} table contained 0
                             rows".format(table))
        self.log.info("Having rows check on table {} passed with
                      {} records".format(table, num_records))

    @staticmethod
    def check_null(redshift_hook, table):
        table_list = ["songplays", "users", "songs", "artists", "time"]
        check_null_list = [
            "playid IS NULL OR start_time IS NULL OR userid IS NULL OR \
            'level' IS NULL OR songid IS NULL OR artistid IS NULL OR \
            sessionid IS NULL OR location IS NULL OR user_agent IS NULL", \
            "userid IS NULL OR first_name IS NULL OR last_name IS NULL OR \
            gender IS NULL OR 'level' IS NULL",
            "songid IS NULL OR title IS NULL OR artistid IS NULL OR 'year' \
            IS NULL OR duration IS NULL",
            "artistid IS NULL OR name IS NULL OR location IS NULL OR \
            lattitude IS NULL OR longitude IS NULL",
            "start_time IS NULL OR 'hour' IS NULL OR 'day' IS NULL OR week IS \
            NULL OR 'month' IS NULL OR 'year' IS NULL OR weekday IS NULL"
        ]
        check_null_dict = dict(zip(table_list, check_null_list))
        records = redshift_hook.get_records(
            """
            SELECT COUNT(*) FROM {}
            WHERE {}
            """.format(table, check_null_dict[table]))
        if len(records) > 0:
            raise ValueError("Data quality check failed, {} table contains NULL
                             values".format(table))
        self.log.info("Having no NULL value in {} table".format(table))

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            check_rows(redshift_hook, table)

        for table in self.tables:
            check_null(redshift_hook, table)
