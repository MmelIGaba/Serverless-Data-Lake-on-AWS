## Phase 2: Data Cataloging

- **Glue Crawler**: `covid19-raw-crawler`
  - Source: `s3://covid19-data-lake-mmeli/raw/`
  - IAM Role: `GlueAthenaS3AccessRole`
  - Output Database: `covid19_db`

- **Glue Table Schema**: `covid datalake table`
  - Columns inferred: 
    - province/state (STRING)
    - country/region (STRING)
    - lat (DOUBLE)
    - long (DOUBLE)
    - daily counts (BIGINT, 509 columns total)

### Querying with Athena
- Database: covid19_db
- Table: raw
- Example Query: SELECT "country/region", SUM("1/22/20") AS cases_day1, SUM("1/31/20") AS cases_day10 FROM raw GROUP BY "country/region" ORDER BY cases_day10 DESC LIMIT 10;
- Results stored in: s3://covid19-data-lake-mmeli/athena-results/
- Note: Queries not explicitly saved appear under "Unsaved/".

## Phase 3: Serverless ETL Pipeline
- Job name: covid19_etl_processed
- Source: covid19_db.raw
- Transformations:
  - Renamed columns
  - Handled null values
  - Reshaped wide → long format
  - Normalized data types
- Target: s3://covid19-data-lake-mmeli/processed/ (Parquet format)
- Registered new table: covid19_processed


