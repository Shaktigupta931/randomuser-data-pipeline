from datetime import datetime
import pandas as pd

def create_metadata(run_id, dataset_name, record_count):
    return pd.DataFrame([{
        "run_id": run_id,
        "dataset_name": dataset_name,
        "run_timestamp": datetime.utcnow(),
        "status": "SUCCESS",
        "records_fetched": record_count
    }])

def create_audit(run_id, step, status, message):
    return pd.DataFrame([{
        "run_id": run_id,
        "step_name": step,
        "status": status,
        "message": message,
        "timestamp": datetime.utcnow()
    }])

def create_metrics(run_id, total_records):
    return pd.DataFrame([{
        "run_id": run_id,
        "records_loaded": total_records,
        "timestamp": datetime.utcnow()
    }])
