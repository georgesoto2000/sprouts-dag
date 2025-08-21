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
    def upload_sales_data(
        project_id: str,
        dataset_id: str,
        table_id: str,
        bucket_name: str,
        archive_bucket_name: str,
        pattern: str | None = None,
    ) -> list[str]:
        """Find the blob names matching a pattern in the bucket."""
        gcs_client = GCSClient(project_id=project_id)
        if pattern:
            names = gcs_client.get_blob_names(bucket_name=bucket_name, pattern=pattern)
        else:
            names = gcs_client.get_blob_names(bucket_name=bucket_name)
        for name in names:
            download_blob_move_to_archive(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                bucket_name=bucket_name,
                archive_bucket_name=archive_bucket_name,
                blob_name=name,
            )

    @task
    def trigger_dbt_pipleine():
        pass

    project_id = "sprouts-469516"
    bucket_name = "ecommerce-sales-sprout"
    archive_bucket_name = "ecommerce-sales-sprout-archive"
    dataset_id = "ecommerce_sales"
    table_id = "daily_sales"

    # Tasks
    sports_events = retrieve_events_upload_bq(
        project_id=project_id, dataset_id="sports_events", table_id="events"
    )

    sales_data_task = upload_sales_data(
        project_id=project_id,
        dataset_id=dataset_id,
        bucket_name=bucket_name,
        archive_bucket_name=archive_bucket_name,
        table_id=table_id,
        pattern=r"^sales_\d{8}_\d{6}\.csv$",
    )

    dbt_task = trigger_dbt_pipleine()

    return sports_events >> sales_data_task >> dbt_task


dag = my_sprouts_dag()
