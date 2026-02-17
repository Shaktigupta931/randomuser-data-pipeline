import uuid
from src.utils.helpers import load_config
from src.ingestion.api_client import fetch_users
from src.transform.transformer import transform_data
from src.system.system_tables import create_metadata, create_audit, create_metrics
from src.load.bigquery_loader import get_client, merge_table,append_table


def main():
    run_id = str(uuid.uuid4())
    config = load_config()

    print("Fetching API data...")
    raw = fetch_users(
        config["api"]["url"],
        config["api"]["retries"],
        config["api"]["timeout"]
    )

    print("Transforming data...")
    transformed = transform_data(raw)

    print("Connecting to BigQuery...")
    client = get_client(config["project_id"])

    for table_name, df in transformed.items():
        print(f"Merging {table_name}...")

        rows =merge_table(
            client,
            config["dataset_name"],
            table_name,
            df,
            key_column="user_id"
        )
        print(f"{table_name} merged with {rows} rows.")

    metadata_df = create_metadata(run_id, config["dataset_name"], len(transformed["users"]))
    audit_df = create_audit(run_id, "pipeline_run", "SUCCESS", "Pipeline completed")
    metrics_df = create_metrics(run_id, len(transformed["users"]))

    append_table(client, config["dataset_name"], "metadata_info", metadata_df)
    append_table(client, config["dataset_name"], "pipeline_audit_logs", audit_df)
    append_table(client, config["dataset_name"], "pipeline_metrics", metrics_df)


    print("Pipeline completed successfully!")


if __name__ == "__main__":
    main()
