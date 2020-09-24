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

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            check_rows(redshift_hook, table)
