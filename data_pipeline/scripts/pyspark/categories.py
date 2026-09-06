from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
import configparser


spark = SparkSession.builder \
    .appName('AirflowSparkJob') \
    .getOrCreate()


bucket_name = "us-central1-aiflowsynthetic-90271e68-bucket"
object_name = "data/config.properties"

gcs_path = f"gs://{bucket_name}/{object_name}"

spark.conf.set('temporaryGcsBucket', f'gs://{bucket_name}')

config_lines = spark.read.text(gcs_path).collect()
config_string = "\n".join([row.value for row in config_lines])


config = configparser.ConfigParser()
config.read_string(config_string)

PROJECT_ID = config.get("project_id", "project")
RAW = config.get("datasets", "raw_dataset")
REFINE = config.get("datasets", "refine_dataset")
TABLE_NAME = "categories"


df = spark.read.format('bigquery') \
    .option('parentProject', PROJECT_ID) \
    .load(f"{PROJECT_ID}.{RAW}.{TABLE_NAME}")

df.printSchema()

df_trans = df.select(

    F.expr("try_cast( id AS INT)").alias("id"),
    F.col("name").alias("name"),
    F.col("description")
).filter(df.id.isNotNull())

df_trans.printSchema()


df_trans.write.mode("overwrite") \
    .format('bigquery') \
    .option('table', f'{PROJECT_ID}.{REFINE}.{TABLE_NAME}') \
    .option('temporaryGcsBucket', f"gs://{bucket_name}") \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save()
