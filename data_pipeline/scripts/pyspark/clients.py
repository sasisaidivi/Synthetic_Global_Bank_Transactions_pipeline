from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.types import StringType
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
TABLE_NAME = "clients"


def transform_phone_number(number):
  n = number.replace(" ","").replace("(","").replace(")","").replace("-","")
  if n[0] == "+":
    new_number = "( "+n[-11::-1][::-1]+" ) "+n[-10:]
  else:
    new_number = "( +"+n[-11::-1][::-1]+" ) "+n[-10:]
  return new_number

def transform_gender(gender):
  g = gender.strip().lower()
  if g[0] == 'm':
    return 'M'
  elif g[0] == 'f' :
    return 'F'
  else:
    if g.find("male") != -1 and g.find("female") != -1:
      return "T"
    elif g.find("male") != -1:
      return "M"
    elif g.find("female") != -1:
      return "F"
    else :
      return "Unknow"

  
trans_gender = F.udf(transform_gender,StringType())
transform_mobile_number = F.udf(transform_phone_number,StringType())


df = spark.read.format('bigquery') \
    .option('parentProject', PROJECT_ID) \
    .load(f"{PROJECT_ID}.{RAW}.{TABLE_NAME}")

df.printSchema()

df_trans = df.select(
   F.expr("try_cast(id AS INT)").alias("id"),
   F.trim(F.col("fullname")).alias("fullname"),
   F.trim(F.col("address")).alias("address"), 
   transform_mobile_number(F.col("phone_number")).alias("phone_number"),
   F.trim(F.col("email")).alias("email"),
   F.trim(F.col("workplace")).alias("workplace"),
   F.to_date(F.col("birthdate"),'yyyy-MM-dd').alias("birthdate"),
   F.to_date(F.col("registration_date")).alias("registration_date"),
   trans_gender(F.col("gender")).alias("gender"),
   F.expr("try_cast(income AS INT)").alias("income"),
   F.expr("try_cast(expenses AS INT)").alias("expenses"),
   F.expr("try_cast(credit AS INT)").alias("credit"),
   F.expr("try_cast(deposit AS INT)").alias("deposit")
).filter(df.id.isNotNull())

df_trans.printSchema()

df_trans.write.mode("overwrite") \
    .format('bigquery') \
    .option('table', f'{PROJECT_ID}.{RAW}.{TABLE_NAME}_transfer') \
    .option('temporaryGcsBucket', f"gs://{bucket_name}") \
    .option('createDisposition', 'CREATE_IF_NEEDED') \
    .save()
