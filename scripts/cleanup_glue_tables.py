import boto3
import time
from botocore.config import Config
from botocore.exceptions import ClientError

DATABASE_NAME = "covid19_db"
PREFIX = "covid19_processedpart"
BATCH_SIZE = 25
MAX_RETRIES = 5

# Retry configuration for throttling
config = Config(
    retries={
        "max_attempts": 10,
        "mode": "adaptive"
    }
)

client = boto3.client("glue", config=config)

def get_all_matching_tables():
    paginator = client.get_paginator("get_tables")
    tables = []

    for page in paginator.paginate(DatabaseName=DATABASE_NAME):
        for table in page["TableList"]:
            if table["Name"].startswith(PREFIX):
                tables.append(table["Name"])

    return tables


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def batch_delete(tables):
    for batch in chunked(tables, BATCH_SIZE):
        for attempt in range(MAX_RETRIES):
            try:
                response = client.batch_delete_table(
                    DatabaseName=DATABASE_NAME,
                    TablesToDelete=batch
                )

                errors = response.get("Errors", [])
                if errors:
                    print(f"Errors deleting some tables: {errors}")

                print(f"Deleted batch of {len(batch)} tables")
                break

            except ClientError as e:
                if e.response["Error"]["Code"] == "ThrottlingException":
                    wait = 2 ** attempt
                    print(f"Throttled. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise


def main():
    print("Fetching tables...")
    tables = get_all_matching_tables()
    print(f"Found {len(tables)} tables to delete")

    if not tables:
        print("Nothing to delete.")
        return

    batch_delete(tables)
    print("Cleanup complete.")


if __name__ == "__main__":
    main()