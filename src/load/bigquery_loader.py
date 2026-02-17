from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound


# ---------------------------------------------------
# Create BigQuery Client
# ---------------------------------------------------
def get_client(project_id):
    credentials = service_account.Credentials.from_service_account_file(
        "credentials/sample_dataset_4.json"   # change if needed
    )

    return bigquery.Client(
        credentials=credentials,
        project=project_id
    )


# ---------------------------------------------------
# Ensure Target Table Exists
# ---------------------------------------------------
def ensure_table_exists(client, dataset_name, table_name, dataframe):
    project = client.project
    table_id = f"{project}.{dataset_name}.{table_name}"

    try:
        client.get_table(table_id)
        print(f"Table '{table_name}' exists.")
    except NotFound:
        print(f"Creating table '{table_name}'...")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_EMPTY",
            autodetect=True,
        )

        load_job = client.load_table_from_dataframe(
            dataframe,
            table_id,
            job_config=job_config
        )

        load_job.result()
        print(f"Table '{table_name}' created.")


# ---------------------------------------------------
# Merge Table (Idempotent Load)
# ---------------------------------------------------
def merge_table(client, dataset_name, table_name, dataframe, key_column):
    project = client.project

    # 1️⃣ Ensure main table exists
    ensure_table_exists(client, dataset_name, table_name, dataframe)

    target_table = f"{project}.{dataset_name}.{table_name}"
    staging_table = f"{project}.{dataset_name}.{table_name}_staging"

    incoming_rows = len(dataframe) 
    # 2️⃣ Load data into staging table (overwrite staging)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    load_job = client.load_table_from_dataframe(
        dataframe,
        staging_table,
        job_config=job_config
    )

    load_job.result()

    # 3️⃣ Get columns dynamically
    table = client.get_table(staging_table)
    columns = [field.name for field in table.schema]

    update_clause = ", ".join(
        [f"T.{col} = S.{col}" for col in columns if col != key_column]
    )

    insert_columns = ", ".join(columns)
    insert_values = ", ".join([f"S.{col}" for col in columns])

    # 4️⃣ MERGE query
    merge_query = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.{key_column} = S.{key_column}
    WHEN MATCHED THEN
      UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """

    query_job = client.query(merge_query)
    query_job.result()

    # 5️⃣ Delete staging table
    client.delete_table(staging_table, not_found_ok=True)

    print(f"Merge completed for table '{table_name}' | Incoming rows: {incoming_rows}")

    return incoming_rows

# ---------------------------------------------------
# Append Table (For System Logs)
# ---------------------------------------------------
def append_table(client, dataset_name, table_name, dataframe):
    project = client.project
    table_id = f"{project}.{dataset_name}.{table_name}"

    # Ensure table exists
    ensure_table_exists(client, dataset_name, table_name, dataframe)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    load_job = client.load_table_from_dataframe(
        dataframe,
        table_id,
        job_config=job_config
    )

    load_job.result()

    print(f"Data appended to table '{table_name}'.")
