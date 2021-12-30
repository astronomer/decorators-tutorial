# decorators-tutorial

This repo contains several example DAGs highlighting the use of decorators, the TaskFlow API, and the [`astro` library](https://github.com/astro-projects/astro) in Airflow 2. There are also several "classic" DAGs to highlight "before" examples of what these workflows looked like prior to the features being highlighted.

Specifically:

 - `taskflow.py`: Shows a basic ETL example using the TaskFlow API and Python decorator to easily pass data between tasks. `classic-python-operator.py` implements the same DAG as it would have been prior to Airflow 2 and the TaskFlow API.
 - `astro-billing-etl.py`: Shows an ETL example for processing billing data by implementing `astro` library functions for SQL and Pandas transformations. `classic-billing-etl.py` implements the same DAG without the use of the `astro` library.
 - `astro-animal-adoptions-etl.py`: Provides another ETL example using `astro` library functions.

## Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:

 1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
 2. Clone this repo somewhere locally and navigate to it in your terminal
 3. Initialize an Astronomer project by running `astro dev init`
 4. Start Airflow locally by running `astro dev start`
 5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there
