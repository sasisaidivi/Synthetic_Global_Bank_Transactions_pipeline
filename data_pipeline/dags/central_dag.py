import configparser
from airflow.sdk import dag
from datetime import timedelta
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime

gcs_hook = GCSHook()
bucket_name = "us-central1-aiflowsynthetic-90271e68-bucket"
object_name = "data/config.properties"

# Download the file content as a string
config_string = gcs_hook.download(
    bucket_name=bucket_name, 
    object_name=object_name
).decode("utf-8")

# Parse the string content using configparser
config = configparser.ConfigParser()
config.read_string(config_string)

PROJECT_ID = config.get("project_id","project")
REFINE = config.get("datasets","refine_dataset")
AUTH = config.get("datasets","auth_views")
BUCKET_NAME = config.get("bucket_names","bucket_name")

LIST_DAGS = [
    "categories_dag",
    "clients_dag",
    "subscriptions_dag",
    "transactions_dag"
]

LIST_SCRIPTS = [
    "total_transactions",
    "transactions_count",
    "total_deposit_credits",
    "active_subscription_rate",
    "customer_growth_rate"
]

@dag(
        dag_id="central_dag",
        catchup=False,
        schedule=CronDataIntervalTimetable('@daily',timezone='Asia/Kolkata'),
        default_args={
            "email":"divisasisai@gmail.com",
            "retries" : 3,
            "retry_delay" : timedelta(minutes=5)
        }
)
def central_dag():

    start = EmptyOperator(task_id="start")
    middle = EmptyOperator(task_id="middle")
    end = EmptyOperator(task_id="end")

    for dag in LIST_DAGS:
        trigger_dag = TriggerDagRunOperator(
            task_id = f"dags_trigger_{dag}",
            trigger_dag_id = dag,
            wait_for_completion=True
        )

        start >> trigger_dag >> middle

    for sql_script in LIST_SCRIPTS:

        local_script_path = f"/home/airflow/gcs/data/scripts/sql/{sql_script}.sql"
        with open(local_script_path, "r") as f:
            sql_query_string = f.read()
        
        script_sql = BigQueryInsertJobOperator(
            task_id=f"script_{sql_script}",
            gcp_conn_id="google_cloud_default",
            configuration={
                "query":{
                    "query":sql_query_string,
                    "useLegacySql": False,
                }  
            }
        )

        middle >> script_sql >> end


central_dag()