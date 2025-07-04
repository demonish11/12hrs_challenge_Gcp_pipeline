# Dataproc PySpark script to clean raw.folder and raw.folder_access and write to staging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when, array, to_json, expr, concat_ws, lit
from pyspark.sql.types import StringType, BooleanType, ArrayType, StructType, StructField

# Define GCS bucket and BQ dataset
project_id = "nifty-matrix-464708-j2"
gcs_bucket = 'nas_data_dump_africa'
bq_dataset = 'staging'

# Spark session
spark = SparkSession.builder.appName("Staging Folder").getOrCreate()

# -----------------------------
# Load raw.folder table
# -----------------------------
df_folder = spark.read.format("bigquery") \
    .option("table", f"{project_id}:raw.folder").load()

# Define schema for countrywiseprice
country_price_schema = StructType([
    StructField("country", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("localiseBasePrice", BooleanType(), True)
])

# Parse and flatten countrywiseprice field
if "countrywiseprice" in df_folder.columns:
    df_folder = df_folder.withColumn(
        "parsed_countrywiseprice",
        when(
            col("countrywiseprice").cast("string").rlike(r'^\[.*\]'),
            from_json(col("countrywiseprice"), ArrayType(country_price_schema))
        ).otherwise(
            when(
                col("countrywiseprice").cast("string").rlike(r'^\{.*\}$'),
                array(from_json(col("countrywiseprice"), country_price_schema))
            ).otherwise(None)
        )
    )

    df_folder = df_folder.withColumn("countryprice_countries", concat_ws(",", expr("transform(parsed_countrywiseprice, x -> x.country)")))
    df_folder = df_folder.withColumn("countryprice_currencies", concat_ws(",", expr("transform(parsed_countrywiseprice, x -> x.currency)")))
    df_folder = df_folder.withColumn("countryprice_localiseflags", concat_ws(",", expr("transform(parsed_countrywiseprice, x -> cast(x.localiseBasePrice as string))")))

# Drop parsed JSON fields to avoid schema issues
if "parsed_countrywiseprice" in df_folder.columns:
    df_folder = df_folder.drop("parsed_countrywiseprice")

# -----------------------------
# Load raw.folder_access table
# -----------------------------
df_access = spark.read.format("bigquery") \
    .option("table", f"{project_id}:raw.folder_access").load()

# -----------------------------
# Write to staging BigQuery tables
# -----------------------------
df_folder.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{bq_dataset}.folder") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

df_access.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{bq_dataset}.folder_access") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

print("\u2713 Staging tables written: folder and folder_access")