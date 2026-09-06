import configparser
from airflow.sdk import dag
from datetime import timedelta
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

gcs_hook = GCSHook()
bucket_name = "us-central1-aiflowsynthetic-90271e68-bucket"
object_name = "data/config.properties"

schema_bucket = "us-central1-aiflowsynthetic-90271e68-bucket"
schema_object = "data/schema_string"

# Download the file content as a string
config_string = gcs_hook.download(
    bucket_name=bucket_name, 
    object_name=object_name
).decode("utf-8")

# Parse the string content using configparser
config = configparser.ConfigParser()
config.read_string(config_string)

PROJECT_ID = config.get("project_id","project")
RAW = config.get("datasets","raw_dataset")
REFINE = config.get("datasets","refine_dataset")
AUTH = config.get("datasets","auth_views")
AUDIT = config.get("datasets","audit")
RAW_AUDIT_TABLE = config.get("audit_tables","raw_audit")
REFINE_AUDIT_TABLE = config.get("audit_tables","refine_audit")
BUCKET_NAME = config.get("bucket_names","bucket_name")
BUCKET_OLD_NAME = config.get("bucket_names","bucket_old_name")
FILE_NAME = "subscriptions.csv"
TABLE_NAME = "subscriptions"

@dag(
        dag_id="subscriptions_dag",
        catchup=False,
        schedule=None,
        default_args={
            "email":"divisasisai@gmail.com",
            "retries" : 3,
            "retry_delay" : timedelta(minutes=5)
        }
)
def subscriptions_dag():

    object_sensor = GCSObjectExistenceSensor(
        task_id ="object_sensor",
        google_cloud_conn_id='google_cloud_default',
        bucket=BUCKET_NAME,
        object=FILE_NAME,
        poke_interval=60,
        timeout=1800,
        mode="reschedule"
    )
    
    data_load_raw = GCSToBigQueryOperator(
        task_id= "loading_data",
        gcp_conn_id='google_cloud_default',
        bucket=BUCKET_NAME,
        source_objects=[FILE_NAME],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW}.{TABLE_NAME}",
        autodetect=False,
        schema_object_bucket=schema_bucket,
        schema_object=f"{schema_object}/{TABLE_NAME}_schema_string.json",
        skip_leading_rows=1,
        source_format='csv',
        allow_jagged_rows=True,
        write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines = True,
        max_bad_records = 100,
        ignore_unknown_values = True,
        src_fmt_configs={
            "nullMarker": ""
        },
        field_delimiter=','
    )

    data_move = GCSToGCSOperator(
        task_id = "Data_move_GSC_to_GCS",
        source_bucket=BUCKET_NAME,
        source_object=FILE_NAME,
        destination_bucket=BUCKET_OLD_NAME,
        destination_object=f"{TABLE_NAME}_{'{{ds_nodash}}'}.csv",
        move_object=True,
        gcp_conn_id="google_cloud_default"
    )

    audit_raw = BigQueryInsertJobOperator(
        task_id="audit_insert_raw",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query":{
                "query":f"""
                    select 
                        '{RAW}' as dataset_name,
                        '{TABLE_NAME}' as table_name,
                        current_date() as date_audit,
                        count(1) as total_record_count,
                        'sasi sai divi' as audited_by
                    from 
                        {RAW}.{TABLE_NAME}
                    """,
                "destinationTable": {
                    "projectId": f"{PROJECT_ID}",
                    "datasetId": f"{AUDIT}",
                    "tableId": f"{RAW_AUDIT_TABLE}"
                },
                "writeDisposition": "WRITE_APPEND",
                "useLegacySql": False,
            }  
        }
    )


    spark_job = DataprocSubmitJobOperator(
            task_id="spark_job",
            region="us-central1",
            project_id=PROJECT_ID,
            job={
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": "spark"},
                "pyspark_job": {
                    "main_python_file_uri": f"gs://us-central1-aiflowsynthetic-90271e68-bucket/data/scripts/pyspark/{TABLE_NAME}.py",
                },
            },
    )

    local_script_path = f"/home/airflow/gcs/data/scripts/sql/{TABLE_NAME}.sql"
    with open(local_script_path, "r") as f:
        sql_query_string = f.read()
    
    script_sql = BigQueryInsertJobOperator(
        task_id=f"script_{TABLE_NAME}",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query":{
                "query":sql_query_string,
                "useLegacySql": False,
            }  
        }
    )

    audit_refine = BigQueryInsertJobOperator(
        task_id="audit_insert_refine",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query":{
                "query":f"""
                    select 
                        '{REFINE}' as dataset_name,
                        '{TABLE_NAME}' as table_name,
                        current_date() as date_audit,
                        count(1) as total_record_count,
                        'append' as type_operation,
                        'sasi sai divi' as audited_by
                    from 
                        {RAW}.{TABLE_NAME}_transfer
                    """,
                "destinationTable": {
                    "projectId": f"{PROJECT_ID}",
                    "datasetId": f"{AUDIT}",
                    "tableId": f"{REFINE_AUDIT_TABLE}"
                },
                "writeDisposition": "WRITE_APPEND",
                "useLegacySql": False,
            }  
        }
    )



    object_sensor >> data_load_raw >> data_move >> audit_raw >> spark_job >> script_sql >> audit_refine

subscriptions_dag()