from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from datetime import timedelta
import logging

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


default_args = {
    'owner': 'akshay',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


with DAG(
    'crypto_etl',
    default_args=default_args,
    description='Secure pipeline using Airflow connections',
    schedule =None,
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
) as dag:

    # Task 1: Trigger Lambda (API → Kafka)
    
    def trigger_lambda():
        hook = LambdaHook(aws_conn_id='aws_default')  # Uses Airflow connection
        hook.invoke_lambda(
            function_name='fetch_api',
            invocation_type='Event'  # Async execution
        )

    t1 = PythonOperator(
        task_id='trigger_lambda',
        python_callable=trigger_lambda
    )


    # Task 2: Run Glue Job (Kafka → S3)

    t2 = GlueJobOperator(
    task_id='trigger_glue_job',
    job_name='kafka-s3job',
    aws_conn_id='aws_default',
    region_name='ap-south-1',
    wait_for_completion=False
)
    

    

    # Task 3: Force Snowpipe Refresh (S3 → Snowflake)
    def snowflake_ingest():
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
               
                try:
                    cursor.execute("CALL SYSTEM$PIPE_FORCE_REFRESH('my_pipe')")
                    logging.info("Snowpipe refresh successful")
                except Exception as e:
                    logging.warning(f"Pipe refresh failed, using direct copy: {e}")
                    cursor.execute("""
                    COPY INTO my_schema.my_table
                    FROM @my_stage
                    FILE_FORMAT = parquet_format
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    PATTERN = '.*[.]parquet'
                    ON_ERROR = 'SKIP_FILE_1%'
                """)
                logging.info("COPY INTO fallback executed successfully.")
        finally:
            conn.close()

    t3 = PythonOperator(
        task_id='force_snowpipe_refresh',
        python_callable=snowflake_ingest
    )

    # Define workflow
    t1 >> t2 >> t3
