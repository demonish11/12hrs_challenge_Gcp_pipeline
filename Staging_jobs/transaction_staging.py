# Dataproc PySpark script to clean raw.raw_transaction and write to staging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import FloatType, IntegerType, BooleanType

# Define GCS bucket and BQ dataset
project_id = "nifty-matrix-464708-j2"
gcs_bucket = 'nas_data_dump_africa'
bq_dataset = 'staging'

# Spark session
spark = SparkSession.builder.appName("Staging Raw Transaction").getOrCreate()

# -----------------------------
# Load raw.raw_transaction table
# -----------------------------
df_txn = spark.read.format("bigquery") \
    .option("table", f"{project_id}:raw.raw_transaction").load()

# Convert timestamp fields
timestamp_cols = ["transactioncreatedat", "createdat", "updatedat"]
for col_name in timestamp_cols:
    df_txn = df_txn.withColumn(col_name, to_timestamp(col(col_name)))

# Cast numeric and boolean fields
df_txn = df_txn.withColumn("originalamount", col("originalamount").cast(FloatType())) \
               .withColumn("amount_paid", col("amount_paid").cast(FloatType())) \
               .withColumn("retrycount", col("retrycount").cast(IntegerType())) \
               .withColumn("isrenewalpayment", col("isrenewalpayment").cast(BooleanType())) \
               .withColumn("ischargeback", col("ischargeback").cast(BooleanType()))

# -----------------------------
# Write to staging BigQuery table
# -----------------------------
df_txn.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{bq_dataset}.raw_transaction") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

print("\u2713 Staging table written: raw_transaction")