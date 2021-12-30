from datetime import datetime
import pandas as pd

from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_BUCKET = 'bucket_name'
S3_FILE_PATH = '</path/to/file/'
SNOWFLAKE_CONN_ID = 'snowflake'
SNOWFLAKE_SCHEMA = 'schema_name'
SNOWFLAKE_STAGE = 'stage_name'
SNOWFLAKE_WAREHOUSE = 'warehouse_name'
SNOWFLAKE_DATABASE = 'database_name'
SNOWFLAKE_ROLE = 'role_name'
SNOWFLAKE_SAMPLE_TABLE = 'sample_table'
SNOWFLAKE_RESULTS_TABLE = 'result_table'

@task(task_id='extract_data')
def extract_data():
    # Join data from two tables and save to dataframe to process
    query = ''''
    SELECT * FROM billing_data
    LEFT JOIN subscription_data
    ON customer_id=customer_id
    '''
    # Make connection to Snowflake and execute query
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)

    results = cur.fetchall()
    column_names = list(map(lambda t: t[0], cur.description))

    df = pd.DataFrame(results)
    df.columns = column_names

    return df.to_json()

@task(task_id='transform_data')
def transform_data(xcom: str) -> str:
    # Transform data by pivoting
    df = pd.read_json(xcom)

    transformed_df = df.pivot_table(index='DATE', 
                                    values='CUSTOMER_NAME', 
                                    columns=['TYPE'], 
                                    aggfunc='count').reset_index()

    transformed_str = transformed_df.to_string()

    # Save results to S3 so they can be loaded back to Snowflake
    s3_hook = S3Hook(aws_conn_id="s3_conn")
    s3_hook.load_string(transformed_str, 'transformed_file_name.csv', bucket_name=S3_BUCKET, replace=True)


@dag(start_date=datetime(2021, 12, 1), schedule_interval='@daily', catchup=False)

def classic_billing_dag():

    load_subscription_data = S3ToSnowflakeOperator(
        task_id='load_subscription_data',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        s3_keys=[S3_FILE_PATH + '/subscription_data.csv'],
        table=SNOWFLAKE_SAMPLE_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV',field_delimiter = ',')",
    )

    load_transformed_data = S3ToSnowflakeOperator(
        task_id='load_transformed_data',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        s3_keys=[S3_FILE_PATH + '/trasnformed_file_name.csv'],
        table=SNOWFLAKE_RESULTS_TABLE,
        schema=SNOWFLAKE_SCHEMA,
        stage=SNOWFLAKE_STAGE,
        file_format="(type = 'CSV',field_delimiter = ',')",
    )

    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_subscription_data >> extracted_data >> transformed_data >> load_transformed_data

classic_billing_dag = classic_billing_dag()
