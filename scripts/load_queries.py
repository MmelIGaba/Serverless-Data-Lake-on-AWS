from pyathena import connect
import pandas as pd
import glob

conn = connect(s3_staging_dir='s3://covid19-data-lake-mmeli/query-results/',
               region_name='us-east-1')  

sql_files = glob.glob("sql/*.sql")

for sql_file in sql_files:
    with open(sql_file, 'r') as f:
        query = f.read()
    
    df = pd.read_sql(query, conn)
    
    table_name = sql_file.split('/')[-1].replace('.sql','')
    output_path = f"s3://covid19-data-lake-mmeli/analytics/{table_name}.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Saved {table_name} results to {output_path}")