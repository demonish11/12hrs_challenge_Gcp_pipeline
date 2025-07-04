# Dataproc PySpark script to clean raw.challenge and raw.challenge_participants and write to staging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, size, to_timestamp
from pyspark.sql.types import FloatType, IntegerType, BooleanType

# Define GCS bucket and BQ dataset
project_id = "nifty-matrix-464708-j2"
gcs_bucket = 'nas_data_dump_africa'
bq_dataset = 'staging'

# Spark session without inline configs (for use in Dataproc Web UI with properties set)
spark = SparkSession.builder.appName("Staging Challenge").getOrCreate()

# -----------------------------------------
# Load raw.challenge and prepare for staging
# -----------------------------------------
df_challenge = spark.read.format("bigquery") \
    .option("table", f"{project_id}:raw.challenge").load()

# Alias timestamp columns with clear names
timestamp_cols_challenge = {
    "createdat": "created_date",
    "updatedat": "updated_date",
    "starttime": "start_time",
    "endtime": "end_time"
}
for orig, alias in timestamp_cols_challenge.items():
    df_challenge = df_challenge.withColumnRenamed(orig, alias)

# Convert aliased timestamp columns
for alias in timestamp_cols_challenge.values():
    df_challenge = df_challenge.withColumn(alias, to_timestamp(col(alias)))

# Cast numeric and boolean columns
df_challenge = df_challenge.withColumn("amount", col("amount").cast(FloatType())) \
    .withColumn("suggestedamount", col("suggestedamount").cast(FloatType())) \
    .withColumn("minamount", col("minamount").cast(FloatType())) \
    .withColumn("durationindays", col("durationindays").cast(IntegerType())) \
    .withColumn("checkpointdurationindays", col("checkpointdurationindays").cast(IntegerType())) \
    .withColumn("stepbystepunlocking", col("stepbystepunlocking").cast(BooleanType())) \
    .withColumn("areallcheckpointsnotempty", col("areallcheckpointsnotempty").cast(BooleanType())) \
    .withColumn("enablecheckpointsubmissionafterdeadline", col("enablecheckpointsubmissionafterdeadline").cast(BooleanType()))

# ------------------------------------------------------
# Load raw.challenge_participants and prepare for staging
# ------------------------------------------------------
df_part = spark.read.format("bigquery") \
    .option("table", f"{project_id}:raw.challenge_participants").load()

# Alias timestamp columns with clear names
timestamp_cols_participants = {
    "joineddate": "joined_date",
    "createdat": "created_date",
    "updatedat": "updated_date",
    "completeddate": "completed_date",
    "declaredwinnerdate": "declared_winner_date",
    "kickedoutdate": "kicked_out_date"
}
for orig, alias in timestamp_cols_participants.items():
    df_part = df_part.withColumnRenamed(orig, alias)

# Convert aliased timestamp columns
for alias in timestamp_cols_participants.values():
    df_part = df_part.withColumn(alias, to_timestamp(col(alias)))

# Cast numeric columns
df_part = df_part.withColumn("completionpoint", col("completionpoint").cast(IntegerType())) \
    .withColumn("totalpoints", col("totalpoints").cast(IntegerType()))

# Parse JSON 'items' column and count items completed
item_schema = 'array<struct<itemObjectId:string,completionStatus:string,index:int>>'
df_part = df_part.withColumn("items_parsed", from_json(col("items"), item_schema))
df_part = df_part.withColumn("num_items_completed", size(col("items_parsed")))

# Drop items_parsed column to avoid schema conflict
df_part = df_part.drop("items_parsed")

# -------------------------------------------
# Write cleaned data to staging BigQuery tables
# -------------------------------------------
df_challenge.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{bq_dataset}.challenge") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

df_part.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{bq_dataset}.challenge_participants") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

print("✓ Staging tables written: challenge and challenge_participants")