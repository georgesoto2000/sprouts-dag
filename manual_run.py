import os
import logging
import pandas as pd

from dotenv import load_dotenv

from config import daily_sales_schema
from dags.utils.GCPUtils import BigQueryClient, GCSClient
from dags.utils.SportsEventsAPI import SportsEventsAPI

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,  # Set minimum logging level
        format="%(asctime)s - %(levelname)s - %(message)s",  # Add datetime
        datefmt="%Y-%m-%d %H:%M:%S",  # Datetime format
    )
    load_dotenv("./env/.env")
    bucket_name = os.getenv("BUCKET_NAME")
    archive_bucket_name = os.getenv("ARCHIVE_BUCKET_NAME")

    api = SportsEventsAPI()
    events = api.get_events()

    bq_client = BigQueryClient(dotenvpath="./env/.env")
    bq_client.upload_dataframe_to_bigquery(
        df=events,
        dataset_id="sports_events",
        table_id="events",
        replace=True,
    )

    gcs_client = GCSClient(dotenvpath="./env/.env")
    pattern = (
        r"^sales_\d{8}_\d{6}\.csv$"  # Assuming the format is sales_DDMMYYYY._HHMMSS.csv
    )
    blob_names = gcs_client.get_blob_names(bucket_name=bucket_name, pattern=pattern)
    print(blob_names)
    dataset_id = "ecommerce"
    table_id = "daily_sales"

    for blob_name in blob_names:
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
