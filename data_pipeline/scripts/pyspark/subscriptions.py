from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.types import StringType
import configparser
from pyspark.sql.window import Window

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
TABLE_NAME = "subscriptions"

df = spark.read.format('bigquery') \
    .option('parentProject', PROJECT_ID) \
    .load(f"{PROJECT_ID}.{RAW}.{TABLE_NAME}")

df.printSchema()

df_trans = df.select(
    F.expr("try_cast(id AS INT)").alias("id"),
    F.expr("try_cast(client_id AS INT)").alias("client_id"),
    F.expr("try_cast(product_category AS INT)").alias("product_category"),
    F.trim(F.col("product_company")).alias("product_company"),
    F.expr("try_cast(amount AS INT)").alias("amount"),

    F.expr("""
        CASE 
            WHEN date_start LIKE '__-__-____' THEN to_date(date_start, 'dd-MM-yyyy')
            WHEN date_start LIKE '____-__-__' THEN to_date(date_start, 'yyyy-MM-dd')
            WHEN date_start LIKE '__/__/____' THEN to_date(date_start, 'dd-MM-yyyy')
            WHEN date_start LIKE '____/__/__' THEN to_date(date_start, 'yyyy-MM-dd')
            ELSE NULL 
        END
    """).alias("date_start"),
    
    F.expr("""
        CASE 
            WHEN date_end LIKE '__-__-____' THEN to_date(date_end, 'dd-MM-yyyy')
            WHEN date_end LIKE '____-__-__' THEN to_date(date_end, 'yyyy-MM-dd')
            WHEN date_end LIKE '__/__/____' THEN to_date(date_end, 'dd-MM-yyyy')
            WHEN date_end LIKE '____/__/__' THEN to_date(date_end, 'yyyy-MM-dd')
            ELSE NULL 
        END
    """).alias("date_end") 
).filter(df.id.isNotNull())

df_trans.printSchema()

window_spec = Window.partitionBy("id").orderBy(
    F.col("date_start").desc_nulls_last()
)

df_ranked = df_trans.withColumn(
    "row_num",
    F.row_number().over(window_spec)
)

df_valid = (
    df_ranked
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

print("remove the duplicates")

df_duplicates = (
    df_ranked
    .filter(F.col("row_num") > 1)
    .drop("row_num")
)

print("create duplicate table")

df_duplicates.write.mode("overwrite") \
    .format('bigquery') \
    .option('table', f'{PROJECT_ID}.{RAW}.{TABLE_NAME}_transfer_duplicates') \
    .option('temporaryGcsBucket', f"gs://{bucket_name}") \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save()


df_valid.write.mode("overwrite") \
    .format('bigquery') \
    .option('table', f'{PROJECT_ID}.{RAW}.{TABLE_NAME}_transfer') \
    .option('temporaryGcsBucket', f"gs://{bucket_name}") \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save()
