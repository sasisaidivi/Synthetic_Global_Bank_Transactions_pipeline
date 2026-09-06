import configparser
from google.oauth2 import service_account
from google.cloud import storage
from google.cloud import bigquery
import os 
import json

#configur the config file
config = configparser.ConfigParser()
config.read(r'C:\Users\divis\Downloads\GCP\usecases\Synthetic_Global_Bank_Transactions_pipeline\data_pipeline\config\config.properties')

#configure and get the GCS clien of GCP
KEY_PATH = r"C:\Users\divis\Downloads\GCP\service_account_keys\bigquery_access_only_24_08_2026.json"

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
storage_client = storage.Client.from_service_account_json(KEY_PATH)
client = bigquery.Client.from_service_account_json(KEY_PATH)


#create new bucket for brazilian ecom data 
bucket_name = config.get("bucket_names","bucket_name")
print(bucket_name)
bucket = storage_client.bucket(bucket_name)
bucket.location = config.get("bucket_names","location")
bucket.storage_class = config.get("bucket_names","storage_class")

storage_client.create_bucket(bucket)

print("synthetic bucket was created -",bucket_name)

bucket_old_name = config.get("bucket_names","bucket_old_name")
print(bucket_old_name)
bucket = storage_client.bucket(bucket_old_name)
bucket.location = config.get("bucket_names","location")
bucket.storage_class = config.get("bucket_names","storage_class")

storage_client.create_bucket(bucket)

print("synthetic bucket was created -",bucket_old_name)

#push local data to gcs
folder_path = r"C:\Users\divis\Downloads\GCP\usecases\Synthetic_Global_Bank_Transactions_pipeline\data_sets\intial_data" 

bucket = storage_client.bucket(bucket_name)

for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path,file_name)

    if os.path.isfile(file_path):
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_path)

print("data was pushed")
#list blobs
for blob in bucket.list_blobs():
    print(blob.name)


#create datasets for raw , refine ,auth views and audit

dataset_ids = ['raw_synthetic','refine_synthetic','auth_views_synthetic','audit_synthetic']
locations = ["US","US","US","US"]

for dataset_id,location in zip(dataset_ids,locations):
    dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
    dataset.location = location
    client.create_dataset(dataset)

print("datasets was created")
for dataset in client.list_datasets():
    print(dataset)


#create audit table for raw and refine layer 
shema_raw = [
    {"name":"dataset_name","type":"string","mode":"nullable"},
    {"name":"table_name","type":"string","mode":"nullable"},
    {"name":"date_audit","type":"date","mode":"nullable"},
    {"name":"total_record_count","type":"int64","mode":"nullable"},
    {"name":"audited_by","type":"string","mode":"nullable"}
]

table_raw = bigquery.Table(f'{client.project}.audit_synthetic.synthetic_raw_audit',shema_raw)

res = client.create_table(table_raw)

print("raw audit table created.")


query = """
    create table audit_synthetic.synthetic_refine_audit(
        dataset_name string,
        table_name string,
        date_audit date,
        total_record_count int64,
        type_operation string,
        audited_by string
    )
"""
res = client.query(query)
print("refine audit table created.")


#crreating refine tables 
refine_dataset = config.get("datasets","refine_dataset")
schema_folder =r"C:\Users\divis\Downloads\GCP\usecases\Synthetic_Global_Bank_Transactions_pipeline\schemas\schema"

for file_name in os.listdir(schema_folder):

    file_path = os.path.join(schema_folder,file_name)
    print("file path - ",file_path)

    if os.path.isfile(file_path):
        with open(file_path, "r",encoding='utf-8') as f:
            schema_json = json.load(f)
        table_name = file_name.split('.')[0].split('_')[0]
        print("table name - ",table_name)
        table = bigquery.Table(f"{client.project}.{refine_dataset}.{table_name}",schema=schema_json)
        res = client.create_table(table)
        if res :
            print(f"table {table_name} created")