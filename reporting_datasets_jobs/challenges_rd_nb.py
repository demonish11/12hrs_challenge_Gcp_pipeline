from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff

# GCP config
project_id = "nifty-matrix-464708-j2"
bq_dataset = "staging"
gcs_bucket = "nas_data_dump_africa"

# Initialize Spark session
spark = SparkSession.builder.appName("Report: Challenge Participants").getOrCreate()

# ----------------------------
# Load staging data
# ----------------------------
df_challenge = spark.read.format("bigquery") \
    .option("table", f"{project_id}:{bq_dataset}.challenge").load()

df_participants = spark.read.format("bigquery") \
    .option("table", f"{project_id}:{bq_dataset}.challenge_participants").load()

# Alias DataFrames to avoid column ambiguity
challenge_df = df_challenge.alias("challenge")
participants_df = df_participants.alias("challenge_participants")

# ----------------------------
# Join and enrich
# ----------------------------
report_df = participants_df.join(
    challenge_df,
    participants_df["programobjectid"] == challenge_df["_id"],
    "left"
)

# Add participation duration
report_df = report_df.withColumn(
    "participation_days",
    datediff(col("completed_date"), col("joined_date"))
)

# Derive completion status
report_df = report_df.withColumn(
    "completion_status",
    when(col("completed_date").isNotNull(), "COMPLETED")
    .when(col("kicked_out_date").isNotNull(), "KICKED_OUT")
    .otherwise("ONGOING")
)

# ----------------------------
# Select final report fields
# ----------------------------
final_report_df = report_df.select(
    col("challenge_participants.learnerobjectid"),
    col("challenge_participants.programobjectid").alias("challenge_id"),
    col("challenge.title").alias("challenge_title"),
    col("challenge.currency"),
    col("challenge.minamount"),
    col("challenge.suggestedamount"),
    col("challenge.amount"),
    col("challenge_participants.joined_date"),
    col("challenge_participants.completed_date"),
    col("challenge_participants.status").alias("participant_status"),
    col("challenge_participants.completionpoint"),
    col("challenge_participants.totalpoints"),
    col("challenge_participants.num_items_completed"),
    col("participation_days"),
    col("completion_status")
)

# ----------------------------
# Write to BigQuery
# ----------------------------
final_report_df.write \
    .format("bigquery") \
    .option("table", f"{project_id}:report_datasets.report_challenge_participants") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

print("✓ Report dataset written: report_challenge_participants")
