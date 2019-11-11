from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_query="",
                 check_method=lambda: True,
                 error_message="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_query = check_query
        self.check_method = check_method
        self.error_message = error_message

    def execute(self, context):
        self.log.info(f"Checking the data quality")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        records = redshift.get_records(self.check_query)
        if (not self.check_method(records)):
            self.log.error(f"Data quality check failed. {self.error_message}")
            raise ValueError(f"Data quality check failed. {self.error_message}")
        
        self.log.info(f"Finished checking the data quality")
        