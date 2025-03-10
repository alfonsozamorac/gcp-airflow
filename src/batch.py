from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import argparse

spark = SparkSession.builder \
    .appName("BigqueryInsert") \
    .getOrCreate()

parser = argparse.ArgumentParser(description='Mi aplicación PySpark')
parser.add_argument('--batch_id', type=int, required=True, help='Batch id')
parser.add_argument('--bucket', type=str, required=True, help='bucket tmp')
args = parser.parse_args()

batch_id = args.batch_id
bucket = args.bucket
df = spark.createDataFrame([(batch_id,)], ["BATCH_ID"])

df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", bucket) \
    .option("table", "test_airflow.batches") \
    .mode("append") \
    .save()

spark.stop()

