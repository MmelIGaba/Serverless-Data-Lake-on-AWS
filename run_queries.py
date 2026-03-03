import glob
import pandas as pd
from pyathena import connect
import boto3
import io
import os

# ============================
# CONFIG
# ============================
REGION = "us-east-1"
S3_STAGING = "s3://covid19-data-lake-mmeli/athena-results/"
S3_BUCKET = "covid19-data-lake-mmeli"

# ============================
# CONNECT TO ATHENA
# ============================
conn = connect(
    s3_staging_dir=S3_STAGING,
    region_name=REGION,
    schema_name="covid19_db"
)

# ============================
# LOAD ALL SQL FILES
# ============================
sql_files = glob.glob("sql/*.sql")

for sql_file in sql_files:

    print(f"Running query from: {sql_file}")

    # Read SQL file
    with open(sql_file, "r") as f:
        query = f.read()

    # Execute query
    df = pd.read_sql(query, conn)

    print(df.head())

    # Save to S3
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    output_key = f"analytics/{os.path.basename(sql_file).replace('.sql','.csv')}"

    s3 = boto3.client("s3", region_name=REGION)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=output_key,
        Body=csv_buffer.getvalue()
    )

    print(f"Saved to s3://{S3_BUCKET}/{output_key}")

conn.close()
print("All queries executed successfully.")
