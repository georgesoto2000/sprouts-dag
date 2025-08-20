import logging
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.decorators import task

from config import daily_sales_schema
from dags.utils.GCPUtils import BigQueryClient, GCSClient
from dags.utils.SportsEventsAPI import SportsEventsAPI


def download_blob_move_to_archive(
    project_id: str,
    dataset_id: str,
    table_id: str,
    bucket_name,
    archive_bucket_name: str,
    blob_name: str,
) -> None:
    """download blob from gcs bucket, upload to bigquery, and move blob to archive bucket

    Args:
        blob_name (str): name of blob to mvoe
    """
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
        table_schema=daily_sales_schema,
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


with DAG(
    dag_id="ecommerce_sales_dag",
    start_date=datetime(2025, 8, 20),
    schedule_interval="30 8 * * *",
    catchup=False,
    tags=["ecommerce", "sales"],
) as dag:

    @task
    def retrieve_events_upload_bq(
        project_id: str, dataset_id: str, table_id: str
    ) -> None:
        """retrieve data from sports events API and upload to BigQuery

        Args:
            project_id (str): GCP project
            dataset_id (str): dataset to upload to
            table_id (str): table to upload to
        """
        api = SportsEventsAPI()
        events = api.get_events()

        bq_client = BigQueryClient(project_id="sprouts-469516")
        bq_client.upload_dataframe_to_bigquery(
            df=events,
            dataset_id="sports_events",
            table_id="events",
            replace=True,
        )

    @task
    def get_blob_names(
        project_id: str, bucket_name: str, pattern: str | None = None
    ) -> list[str]:
        """Find the blob names following the pattern in the bucket

        Args:
            project_id (str): GCP project
            bucket_name (str): bucket to look for blobs in
            pattern (str | None, optional): regex pattern of blob name. Defaults to None.

        Returns:
            list[str]: _description_
        """
        gcs_client = GCSClient(project_id=project_id)
        if pattern:
            blob_names = gcs_client.get_blob_names(
                bucket_name=bucket_name, pattern=pattern
            )
        else:
            blob_names = gcs_client.get_blob_names(bucket_name="ecommerce-sales-sprout")
        return blob_names

    @task
    def archive_blobs(
        project_id: str,
        dataset_id: str,
        table_id: str,
        bucket_name: str,
        archive_bucket_name: str,
        blob_name: str,
    ) -> None:
        """expandable task, archives blobs and uploads to bq

        Args:
            project_id (str): GCP project
            dataset_id (str): BQ dataset
            table_id (str): BQ table
            bucket_name (str): bucket name to look for blob in
            archive_bucket_name (str): arhcive blob to move to
            blob_name (str): name of blob to look for
        """
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
    sports_events >> retrieve_blob_names >> parallel_archive
