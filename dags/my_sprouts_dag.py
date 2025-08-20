import logging
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from utils.GCPUtils import BigQueryClient, GCSClient
from utils.SportsEventsAPI import SportsEventsAPI


def download_blob_move_to_archive(
    project_id: str,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    archive_bucket_name: str,
    blob_name: str,
) -> None:
    """Download blob from GCS bucket, upload to BigQuery, and move blob to archive bucket."""

    gcs_client = GCSClient(project_id=project_id)
    bq_client = BigQueryClient(project_id=project_id)

    sales = gcs_client.gcs_blob_to_df(bucket_name=bucket_name, blob_name=blob_name)

    sales["Date"] = pd.to_datetime(sales["Date"]).dt.date
    sales["Order_id"] = sales["Order_id"].astype(str)
    sales["transaction_ID"] = sales["transaction_ID"].astype(str)
    sales["product"] = sales["product"].astype(str)
    sales["customer_id"] = sales["customer_id"].astype(str)
    sales["cost"] = sales["cost"].astype(float)

    bq_client.upload_dataframe_to_bigquery(
        df=sales,
        dataset_id=dataset_id,
        table_id=table_id,
        replace=False,
        table_schema=[
            {"name": "Date", "type": "DATE"},
            {"name": "Order_id", "type": "STRING"},
            {"name": "transaction_ID", "type": "STRING"},
            {"name": "product", "type": "STRING"},
            {"name": "customer_id", "type": "STRING"},
            {"name": "cost", "type": "FLOAT64"},
        ],
    )

    gcs_client.delete_blob_from_gcs(bucket_name=bucket_name, blob_name=blob_name)
    gcs_client.df_to_gcs_blob(
        df=sales, bucket_name=archive_bucket_name, blob_name=blob_name
    )

    logging.info(
        "%s blob uploaded to BigQuery %s.%s and moved from %s to archive bucket %s",
        blob_name,
        dataset_id,
        table_id,
        bucket_name,
        archive_bucket_name,
    )


@dag(
    dag_id="my_sprouts_dag",
    default_args={"start_date": datetime(2025, 8, 19)},
    schedule=None,
    catchup=False,
)
def my_sprouts_dag():
    @task
    def retrieve_events_upload_bq(
        project_id: str, dataset_id: str, table_id: str
    ) -> None:
        """Retrieve data from Sports Events API and upload to BigQuery."""
        api = SportsEventsAPI()
        events = api.get_events()

        bq_client = BigQueryClient(project_id=project_id)
        bq_client.upload_dataframe_to_bigquery(
            df=events,
            dataset_id=dataset_id,
            table_id=table_id,
            replace=True,
        )

    @task
    def get_blob_names(
        project_id: str, bucket_name: str, pattern: str | None = None
    ) -> list[str]:
        """Find the blob names matching a pattern in the bucket."""
        gcs_client = GCSClient(project_id=project_id)
        if pattern:
            return gcs_client.get_blob_names(bucket_name=bucket_name, pattern=pattern)
        return gcs_client.get_blob_names(bucket_name=bucket_name)

    @task
    def archive_blobs(
        project_id: str,
        dataset_id: str,
        table_id: str,
        bucket_name: str,
        archive_bucket_name: str,
        blob_name: str,
    ) -> None:
        """Archive blobs and upload to BigQuery."""
        download_blob_move_to_archive(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_name=bucket_name,
            archive_bucket_name=archive_bucket_name,
            blob_name=blob_name,
        )

    PROJECT_ID = "sprouts-469516"
    BUCKET_NAME = "ecommerce-sales-sprout"
    ARCHIVE_BUCKET_NAME = "ecommerce-sales-sprout-archive"
    DATASET_ID = "ecommerce_sales"
    TABLE_ID = "daily_sales"

    # Tasks
    sports_events = retrieve_events_upload_bq(
        project_id=PROJECT_ID, dataset_id="sports_events", table_id="events"
    )

    retrieve_blob_names = get_blob_names(project_id=PROJECT_ID, bucket_name=BUCKET_NAME)

    parallel_archive = archive_blobs.expand(
        project_id=[PROJECT_ID],
        dataset_id=[DATASET_ID],
        table_id=[TABLE_ID],
        bucket_name=[BUCKET_NAME],
        archive_bucket_name=[ARCHIVE_BUCKET_NAME],
        blob_name=retrieve_blob_names,
    )
    return sports_events >> retrieve_blob_names >> parallel_archive
