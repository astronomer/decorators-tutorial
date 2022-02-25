from airflow.decorators import dag
from astro.sql import render

from datetime import datetime

"""
This DAG highlights using the render function to execute SQL queries.
Here the render function results in three sequential tasks that run 
queries in the /include/sql directory. The queries, combine, clean, and 
aggregate data in Snowflake.

To use the DAG, you must have two tables called "Homes" and "Homes2" in 
your database. Data to populate these tables are located in csv's in the 
/include directory. 
You must also update the frontmatter in the queries to your own database
connection info.
"""

@dag(start_date=datetime(2022, 2, 1), schedule_interval='@daily', catchup=False)


def astroflow_homes_sql_dag():
    homes_models = render("/usr/local/airflow/include/sql/")

astroflow_homes_sql_dag = astroflow_homes_sql_dag()