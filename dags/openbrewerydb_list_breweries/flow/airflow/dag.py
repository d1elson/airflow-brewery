from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from openbrewerydb_list_breweries.tasks.bronze_list_breweries_task import (
    BronzeListBreweriesTask,
)

from openbrewerydb_list_breweries.tasks.silver_list_breweries_task import (
    SilverListBreweriesTask,
)

from openbrewerydb_list_breweries.tasks.gold_list_breweries_task import (
    GoldListBreweriesTask,
)

from openbrewerydb_list_breweries.tools.pipeline import Pipeline
from openbrewerydb_list_breweries.tools.tasks import Tasks

default_args = {
    'email':['d1elson@zohomail.com'],
    'email_on_failure':True,
    'email_on_retry':True,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id=Pipeline.PIPELINE_NAME,
    default_args=default_args,
    start_date=datetime(2025, 8, 15),
    max_active_runs=1,
    description="ETL from the openbrewerydb API data to analysis-ready data",
    tags=[
        f"Source: {Pipeline.SOURCE}",
        f"Endpoint: {Pipeline.ENDPOINT}"
    ],
    schedule="@daily",
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    bronze_list_breweries_task = PythonOperator(
        task_id=Tasks.BRONZE_LIST_BREWERIES_TASK,
        python_callable=BronzeListBreweriesTask().execute,
    )

    silver_list_breweries_task = PythonOperator(
        task_id=Tasks.SILVER_LIST_BREWERIES_TASK,
        python_callable=SilverListBreweriesTask().execute,
    )

    gold_list_breweries_task = PythonOperator(
        task_id=Tasks.GOLD_LIST_BREWERIES_TASK,
        python_callable=GoldListBreweriesTask().execute,
    )

    end = DummyOperator(task_id="end")

    (
        start
        >> bronze_list_breweries_task
        >> silver_list_breweries_task
        >> gold_list_breweries_task
        >> end
    )
