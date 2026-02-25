import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import expr

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Load raw dataset from Glue Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="covid19_db",
    table_name="raw"
)

# 2. Rename columns for clarity
renamed = datasource.rename_field("country/region", "country") \
                    .rename_field("province/state", "province") \
                    .rename_field("lat", "latitude") \
                    .rename_field("long", "longitude")

# 3. Handle nulls (replace with 0 for numeric, 'Unknown' for text)
df = renamed.toDF()
df = df.fillna({'province': 'Unknown', 'country': 'Unknown'})
df = df.fillna(0)

# 4. Reshape wide → long format (dates into rows)
unpivotExpr = """
stack(10,
 '1/22/20', `1/22/20`,
 '1/23/20', `1/23/20`,
 '1/24/20', `1/24/20`,
 '1/25/20', `1/25/20`,
 '1/26/20', `1/26/20`,
 '1/27/20', `1/27/20`,
 '1/28/20', `1/28/20`,
 '1/29/20', `1/29/20`,
 '1/30/20', `1/30/20`,
 '1/31/20', `1/31/20`
) as (date, cases)
"""
df_unpivot = df.select("country", "province", "latitude", "longitude", expr(unpivotExpr))

# 5. Write back to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(df_unpivot, glueContext, "unpivoted"),
    connection_type="s3",
    connection_options={"path": "s3://covid19-data-lake-mmeli/processed/"},
    format="parquet"
)

job.commit()
