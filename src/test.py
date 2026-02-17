from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound, Forbidden

PROJECT_ID = "devx-tsc"
DATASET_NAME = "sample_dataset_4"   # change if needed

credentials = service_account.Credentials.from_service_account_file(
    "credentials/sample_dataset_4.json"
)

client = bigquery.Client(
    credentials=credentials,
    project=PROJECT_ID
)


def list_tables_in_dataset():
    dataset_id = f"{PROJECT_ID}.{DATASET_NAME}"

    try:
        # Check dataset exists
        client.get_dataset(dataset_id)
        print(f"\n✅ Dataset '{DATASET_NAME}' exists.\n")

        # List tables
        tables = list(client.list_tables(dataset_id))

        if not tables:
            print("📭 No tables found in dataset.")
            return

        print("📋 Tables found:\n")
        for table in tables:
            print("-", table.table_id)

        print(f"\n🔢 Total Tables: {len(tables)}")

    except NotFound:
        print("❌ Dataset does NOT exist.")

    except Forbidden as e:
        print("🚨 Permission Error")
        print(e)

def check_table_creation_time(table_name):
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"

    try:
        table = client.get_table(table_id)

        print(f"\n📌 Table: {table_name}")
        print("Created at:", table.created)
        print("Last modified:", table.modified)
        print("Number of rows:", table.num_rows)

    except NotFound:
        print(f"❌ Table '{table_name}' not found.")
def preview_table_data(table_name, limit=5):
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"

    query = f"""
        SELECT *
        FROM `{table_id}`
        LIMIT {limit}
    """

    print(f"\n📊 Previewing data from {table_name}:\n")

    try:
        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            print(dict(row))

    except Exception as e:
        print("❌ Error while querying table")
        print(e)

    
if __name__ == "__main__":
    list_tables_in_dataset()
    # check_table_creation_time("users_staging")
    preview_table_data("users_staging", 5)
