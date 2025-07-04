from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, count as _count, month, year, date_format

# Setup
project_id = "nifty-matrix-464708-j2"
bq_dataset = "staging"
report_dataset = "report_datasets"
gcs_bucket = "nas_data_dump_africa"

spark = SparkSession.builder.appName("Reporting - Transaction Details").getOrCreate()

# Read from staging
tx_df = spark.read.format("bigquery").option("table", f"{project_id}:{bq_dataset}.raw_transaction").load()
challenge_df = spark.read.format("bigquery").option("table", f"{project_id}:{bq_dataset}.challenge").load()
folder_df = spark.read.format("bigquery").option("table", f"{project_id}:{bq_dataset}.folder").load()

# Join CHALLENGE
tx_challenge = (
    tx_df.filter(col("purchasetype") == "CHALLENGE")
    .join(
        challenge_df.select(
            col("_id").alias("ch_id"),
            col("title").alias("ch_title"),
            col("currency").alias("ch_currency"),
            col("communityobjectid").alias("ch_community"),
            col("minamount").alias("ch_minamount"),
            col("suggestedamount").alias("ch_suggestedamount"),
            col("amount").alias("ch_amount"),
            col("pricetype").alias("ch_pricetype")
        ),
        tx_df["entityobjectid"] == col("ch_id"),
        "left"
    )
    .withColumn("entity_title", col("ch_title"))
    .withColumn("entity_currency", col("ch_currency"))
    .withColumn("entity_community", col("ch_community"))
    .withColumn("entity_minamount", col("ch_minamount"))
    .withColumn("entity_suggestedamount", col("ch_suggestedamount"))
    .withColumn("entity_amount", col("ch_amount"))
    .withColumn("entity_pricetype", col("ch_pricetype"))
    .withColumn("entity_type", when(col("purchasetype") == "CHALLENGE", "CHALLENGE"))
    .drop("ch_id", "ch_title", "ch_currency", "ch_community", "ch_minamount", "ch_suggestedamount", "ch_amount", "ch_pricetype")
)

# Join FOLDER
tx_folder = (
    tx_df.filter(col("purchasetype") == "FOLDER")
    .join(
        folder_df.select(
            col("_id").alias("f_id"),
            col("title").alias("f_title"),
            col("currency").alias("f_currency"),
            col("communityobjectid").alias("f_community"),
            col("minamount").alias("f_minamount"),
            col("suggestedamount").alias("f_suggestedamount"),
            col("amount").alias("f_amount"),
            col("pricetype").alias("f_pricetype")
        ),
        tx_df["entityobjectid"] == col("f_id"),
        "left"
    )
    .withColumn("entity_title", col("f_title"))
    .withColumn("entity_currency", col("f_currency"))
    .withColumn("entity_community", col("f_community"))
    .withColumn("entity_minamount", col("f_minamount"))
    .withColumn("entity_suggestedamount", col("f_suggestedamount"))
    .withColumn("entity_amount", col("f_amount"))
    .withColumn("entity_pricetype", col("f_pricetype"))
    .withColumn("entity_type", when(col("purchasetype") == "FOLDER", "FOLDER"))
    .drop("f_id", "f_title", "f_currency", "f_community", "f_minamount", "f_suggestedamount", "f_amount", "f_pricetype")
)

# Combine both
combined_df = tx_challenge.unionByName(tx_folder)

# Derive extra fields
enriched_df = combined_df.withColumn("transaction_month", month("transactioncreatedat")) \
                                .withColumn("transaction_year", year("transactioncreatedat")) \
                                .withColumn("transaction_date", date_format("transactioncreatedat", "yyyy-MM-dd")) \
                                .withColumn("is_success", when(col("status") == "Success", True).otherwise(False))

# Revenue summary per learner
learner_summary_df = (
    enriched_df.groupBy("learnerobjectid")
    .agg(
        _sum("amount_paid").alias("total_paid"),
        _count("_id").alias("transaction_count"),
        _sum(when(col("status") == "Success", 1).otherwise(0)).alias("successful_tx_count"),
        _sum(when(col("status") != "Success", 1).otherwise(0)).alias("failed_tx_count")
    )
)

# Enrich main data
final_df = enriched_df.join(learner_summary_df, on="learnerobjectid", how="left")

# Final report fields
report_df = final_df.select(
    col("_id").alias("transaction_id"),
    "transactioncreatedat", "transaction_date", "transaction_month", "transaction_year",
    "transactiontype", "purchasetype", "entity_type",
    "entityobjectid", "entity_title", "entity_currency", "entity_community",
    "entity_minamount", "entity_suggestedamount", "entity_amount", "entity_pricetype",
    col("communityobjectid").alias("payer_community"), "learnerobjectid",
    "originalcurrency", col("currency").alias("settlement_currency"),
    "originalamount", "amount_paid", "status", "is_success",
    "paymentprovider", "paymentmethod", "paymentbrand",
    "total_paid", "transaction_count", "successful_tx_count", "failed_tx_count"
)

# Write to BigQuery
report_df.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{report_dataset}.report_transaction_details") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

print("✓ Reporting dataset written: report_transaction_details")
