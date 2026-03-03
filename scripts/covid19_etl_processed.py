import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, year, month, to_date

# Initialize Spark & Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("Starting COVID data processing job...")

# ============================
# 1️⃣ READ RAW DATA
# ============================

raw_df = glueContext.create_dynamic_frame.from_catalog(
    database="covid19_db",
    table_name="covid19_raw"
).toDF()

print("Raw data loaded")

# ============================
# 2️⃣ CLEAN & TRANSFORM
# ============================

# Ensure date column is proper date type
df = raw_df.withColumn("date", to_date(col("date")))

# Add partition column
df = df.withColumn("year", year(col("date")))

# Optional: Remove bad rows
df = df.filter(col("country").isNotNull())

print("Data transformed")

# ============================
# 3️⃣ CONTROL FILE COUNT
# ============================

# Adjust based on size (for showcase 5-10 is clean)
df = df.repartition(10)

# ============================
# 4️⃣ WRITE CLEAN PROCESSED DATA
# ============================

output_path = "s3://covid19-data-lake-mmeli/processed/"

df.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(output_path)

print(f"Data written to {output_path}")

print("Job completed successfully.")