import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator, BigQueryDeleteTableOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
dataset_file = "green_tripdata_2021-02.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'taxi_data_all')

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id = "green_taxi_ingestion",
    schedule_interval = "@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs = 1
) as dag:

    download_data = BashOperator(
        task_id = "download_data",
        bash_command = f"curl -sSl {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{dataset_file}"],
            },
        },
    )

    insert_query = (
    f"""
    INSERT INTO {BIGQUERY_DATASET}.green
    SELECT * FROM {BIGQUERY_DATASET}.external_table;
    """
    )

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.green \
        PARTITION BY DATE(lpep_pickup_datetime) \
        AS \
        SELECT * FROM {BIGQUERY_DATASET}.external_table;"
    )

    # Create a partitioned table from external table
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_partitioned_table_job",
        configuration={
            "query": {
                "query": insert_query,
                "useLegacySql": False,
            }
        }
    )

    delete_external_table = BigQueryDeleteTableOperator(
        task_id = "detele_external_table",
        deletion_dataset_table = f"{PROJECT_ID}.{BIGQUERY_DATASET}.external_table"
    )

    download_data >> local_to_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job >> delete_external_table
