# Dataproc PySpark script to clean raw.events and raw.event_attendees and write to staging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_json, when, array, to_json, expr, concat_ws, lit
from pyspark.sql.types import IntegerType, BooleanType, FloatType, ArrayType, StructType, StructField, StringType

# Define GCS bucket and BQ dataset
project_id = "nifty-matrix-464708-j2"
gcs_bucket = 'nas_data_dump_africa'
bq_dataset = 'staging'

# Spark session
spark = SparkSession.builder.appName("Staging Event").getOrCreate()

# -----------------------------
# Load raw.events table
# -----------------------------
df_events = spark.read.format("bigquery") \
    .option("table", f"{project_id}:raw.events").load()

# Rename and cast timestamp columns
timestamp_cols_events = {
    "starttime": "start_time",
    "endtime": "end_time"
}
for orig, alias in timestamp_cols_events.items():
    if orig in df_events.columns:
        df_events = df_events.withColumnRenamed(orig, alias)
for alias in timestamp_cols_events.values():
    if alias in df_events.columns:
        df_events = df_events.withColumn(alias, to_timestamp(col(alias)))

# Clip out-of-range timestamps to avoid BigQuery failure
max_ts = "2100-01-01 00:00:00"
min_ts = "1900-01-01 00:00:00"

df_events = df_events.withColumn(
    "start_time",
    when(col("start_time") > lit(max_ts), to_timestamp(lit(max_ts)))
    .when(col("start_time") < lit(min_ts), to_timestamp(lit(min_ts)))
    .otherwise(col("start_time"))
)
df_events = df_events.withColumn(
    "end_time",
    when(col("end_time") > lit(max_ts), to_timestamp(lit(max_ts)))
    .when(col("end_time") < lit(min_ts), to_timestamp(lit(min_ts)))
    .otherwise(col("end_time"))
)

# Cast appropriate fields
for field, dtype in {
    "isactive": BooleanType(),
    "suggestedamount": FloatType(),
    "minamount": FloatType()
}.items():
    if field in df_events.columns:
        df_events = df_events.withColumn(field, col(field).cast(dtype))

# Parse JSON fields: paymentmethods and countrywiseprice
payment_schema = StructType([
    StructField("value", StringType(), True),
    StructField("label", StringType(), True),
    StructField("icon", StringType(), True)
])

country_price_schema = StructType([
    StructField("country", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("localiseBasePrice", BooleanType(), True)
])

if "paymentmethods" in df_events.columns:
    df_events = df_events.withColumn(
        "parsed_paymentmethods",
        when(
            col("paymentmethods").cast("string").rlike(r'^\[.*\]'),
            from_json(col("paymentmethods"), ArrayType(payment_schema))
        ).otherwise(
            when(
                col("paymentmethods").cast("string").rlike(r'^\{.*\}$'),
                array(from_json(col("paymentmethods"), payment_schema))
            ).otherwise(None)
        )
    )

if "countrywiseprice" in df_events.columns:
    df_events = df_events.withColumn(
        "parsed_countrywiseprice",
        when(
            col("countrywiseprice").cast("string").rlike(r'^\[.*\]'),
            from_json(col("countrywiseprice"), ArrayType(country_price_schema))
        ).otherwise(
            when(
                col("countrywiseprice").cast("string").rlike(r'^\{.*\}$'),
                array(from_json(to_json(from_json(col("countrywiseprice"), country_price_schema)), country_price_schema))
            ).otherwise(None)
        )
    )

# Flatten arrays into string columns (no array output)
if "parsed_paymentmethods" in df_events.columns:
    df_events = df_events.withColumn("paymentmethod_values", concat_ws(",", expr("transform(parsed_paymentmethods, x -> x.value)")))
    df_events = df_events.withColumn("paymentmethod_labels", concat_ws(",", expr("transform(parsed_paymentmethods, x -> x.label)")))
    df_events = df_events.withColumn("paymentmethod_icons", concat_ws(",", expr("transform(parsed_paymentmethods, x -> x.icon)")))

if "parsed_countrywiseprice" in df_events.columns:
    df_events = df_events.withColumn("countryprice_countries", concat_ws(",", expr("transform(parsed_countrywiseprice, x -> x.country)")))
    df_events = df_events.withColumn("countryprice_currencies", concat_ws(",", expr("transform(parsed_countrywiseprice, x -> x.currency)")))
    df_events = df_events.withColumn("countryprice_localiseflags", concat_ws(",", expr("transform(parsed_countrywiseprice, x -> cast(x.localiseBasePrice as string))")))

# Ensure all expected columns exist (fill with nulls if missing)
expected_columns = [
    "_id", "title", "description", "start_time", "end_time", "livelink", "recordinglink", 
    "isactive", "status", "type", "communities", "access", "currency", "timezoneid", 
    "iscapacityset", "maxquantityperpurchase", "pricetype", "suggestedamount", "minamount",
    "paymentmethod_values", "paymentmethod_labels", "paymentmethod_icons",
    "countryprice_countries", "countryprice_currencies", "countryprice_localiseflags"
]

for col_name in expected_columns:
    if col_name not in df_events.columns:
        df_events = df_events.withColumn(col_name, lit(None).cast(StringType()))

# -----------------------------
# Load raw.event_attendees table
# -----------------------------
df_attendees = spark.read.format("bigquery") \
    .option("table", f"{project_id}:raw.event_attendees").load()

# Cast numeric fields
for field in ["amount", "quantity"]:
    if field in df_attendees.columns:
        df_attendees = df_attendees.withColumn(field, col(field).cast(IntegerType()))

# -----------------------------
# Write to staging BigQuery tables
# -----------------------------
df_events = df_events.drop("paymentmethods", "countrywiseprice", "parsed_paymentmethods", "parsed_countrywiseprice")


df_events.printSchema()
df_events.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{bq_dataset}.events") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

df_attendees.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{bq_dataset}.event_attendees") \
    .option("temporaryGcsBucket", gcs_bucket) \
    .mode("overwrite") \
    .save()

print("✓ Staging tables written: events and event_attendees")