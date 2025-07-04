project_id = "nifty-matrix-464708-j2"
input_bucket = "nas_data_dump_africa"
temp_bucket = "nas_data_dump_africa"  
base_path = f"gs://{input_bucket}/Raw"

folders = {
    "Challenge": ["Challenge.csv", "Challenge Participants.csv"],
    "Event": ["Events.csv", "Event Attendees.csv"],
    "Folder_access": ["Folder.csv", "Folder Access.csv"],
    "Raw_transaction": ["Raw Transaction.csv"]
}

# -----------------------------
# Initialize Spark
# -----------------------------
spark = SparkSession.builder.appName("Raw to BigQuery").getOrCreate()

# -----------------------------
# Ingest CSVs and Write to BigQuery
# -----------------------------
for folder, files in folders.items():
    for file in files:
        gcs_path = f"{base_path}/{folder}/{file}"
        table_name = file.replace(".csv", "").replace(" ", "_").lower()

        print(f"→ Writing {gcs_path} to BigQuery table raw.{table_name}")


        df = spark.read \
            .option("header", "true") \
            .option("multiLine", "true") \
            .option("escape", "\"") \
            .option("quote", "\"") \
            .option("mode", "PERMISSIVE") \
            .csv(gcs_path)
        
        df.printSchema()

        df.write \
            .format("bigquery") \
            .option("table", f"{project_id}:raw.{table_name}") \
            .option("temporaryGcsBucket", temp_bucket) \
            .mode("overwrite") \
            .save()

print("✓ All raw tables written to BigQuery successfully.")