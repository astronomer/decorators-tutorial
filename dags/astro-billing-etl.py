from airflow.decorators import dag
from astro import sql as aql
from astro.sql.table import Table
from astro import dataframe as df

from datetime import datetime
import pandas as pd

SNOWFLAKE_CONN_ID = "snowflake"
S3_FILE_PATH = '</path/to/file/'

# Start by selecting data from two source tables in Snowflake
@aql.transform()
def extract_data(subscriptions: Table, customer_data: Table):
    return """SELECT * FROM {subscriptions}
    LEFT JOIN {customer_data}
    ON customer_id=customer_id"""

# Switch to Pandas for pivoting transformation
@df
def transform_data(df: pd.DataFrame):
    transformed_df = df.pivot_table(index='DATE', 
                                    values='CUSTOMER_NAME', 
                                    columns=['TYPE'], 
                                    aggfunc='count').reset_index()

    return transformed_df


@dag(start_date=datetime(2021, 12, 1), schedule_interval='@daily', catchup=False)

def astro_billing_dag():
    # Load subscripton data
    load_subscription_data = aql.load_file(
        path=S3_FILE_PATH + '/subscription_data.csv',
        file_conn_id="my_s3_conn",
        output_table=Table(table_name="subscription_data", conn_id=SNOWFLAKE_CONN_ID),
    )
    # Define task dependencies
    extracted_data = extract_data(subscriptions=load_subscription_data,
                                    customer_data=Table('customer_data', conn_id=SNOWFLAKE_CONN_ID, schema='SANDBOX_KENTEND'))

    transformed_data = transform_data(extracted_data, output_table=Table('aggregated_bills', conn_id=SNOWFLAKE_CONN_ID))
    
    # Append transformed data to billing table
    load_transformed_data = aql.append(
        conn_id="snowflake",
        append_table="aggregated_bills",
        columns=["DATE", "CUSTOMER_ID", "AMOUNT"],
        main_table="SANDBOX_KENTEND.billing_reporting",
    )

    transformed_data >> load_transformed_data

astro_billing_dag = astro_billing_dag()