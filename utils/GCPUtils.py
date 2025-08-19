import logging
import os
from io import BytesIO, StringIO

import pandas as pd
import re
import pandas_gbq
from dotenv import load_dotenv
from google.cloud import bigquery, storage


class BigQueryClient:
    """Retrieve and write data to BigQuery"""

    def __init__(self, dotenvpath: str) -> None:
        logging.info("Loading google credentials")
        load_dotenv(dotenvpath)
        google_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not google_credentials or not os.path.exists(google_credentials):
            raise ValueError("Google credentials file not found")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials
        self.project_id = os.getenv("PROJECT_ID")
        if not self.project_id:
            raise ValueError("Project ID not found")
        self.client = bigquery.Client(project=self.project_id)
        logging.info("BigQuery client created successfully.")

    def upload_dataframe_to_bigquery(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        table_id: str,
        replace: bool,
        table_schema: list[dict] | None = None,
    ) -> None:
        """Takes dataframe into BigQuery

        Args:
            df (pd.DataFrame): dataframe to be uploaded to BQ
            project_id (str): BQ project id
            dataset_id (str):BQ dataset id
            table_id (str): BQ table ID
            replace (bool): if it already exists, should it be replaced
        """
        if replace:
            if_exists = "replace"
        else:
            if_exists = "append"
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        if table_schema:
            pandas_gbq.to_gbq(
                dataframe=df,
                destination_table=table_ref,
                if_exists=if_exists,
                location="europe-west2",
                table_schema=table_schema,
            )
        else:
            pandas_gbq.to_gbq(
                dataframe=df,
                destination_table=table_ref,
                if_exists=if_exists,
                location="europe-west2",
            )


class GCSClient:
    """Retrieve, write, and delete files from bucket"""

    def __init__(self, dotenvpath: str) -> None:
        logging.info("Loading google credentials")
        load_dotenv(dotenvpath)
        google_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if not google_credentials or not os.path.exists(google_credentials):
            raise ValueError("Google credentials file not found")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credentials
        self.client = storage.Client()
        logging.info("Storage client created successfully.")

    def get_blob_names(self, bucket_name: str, pattern: str | None = None) -> list[str]:
        """retrieves the names of all the blobs in a bucket. If a regex
        pattern is passed in, will only return those matching the pattern.

        Args:
            bucket_name (str): bucket to retrieve blob names from
            pattern (str | None, optional): regex pattern for blob names to match.
                Defaults to None.

        Returns:
            list[str]: list of blob names in bucket, matching pattern if specified
        """
        bucket = self.client.bucket(bucket_name)
        blobs = bucket.list_blobs()
        logging.info("Blob names in %s: %s", bucket_name, blobs)
        if pattern:
            regex = re.compile(pattern)
            blob_names = [blob.name for blob in blobs if regex.match(blob.name)]
            logging.info("Filtered blob names: %s", blob_names)
        else:
            blob_names = [blob.name for blob in blobs]
        return blob_names

    def gcs_blob_to_df(self, bucket_name: str, blob_name: str) -> pd.DataFrame:
        """Download blob to bytes, load into dataframe and return

        Args:
            bucket (str): bucket to download from
            blob (str): blob name to download

        Returns:
            pd.DataFrame: blob in dataframe format
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        logging.info("Downloading blob: %s.%s", bucket_name, blob_name)
        blob_as_bytes = blob.download_as_bytes()
        df = pd.read_csv(BytesIO(blob_as_bytes))
        logging.info("Blob converted to dataframe")
        return df

    def df_to_gcs_blob(
        self, df: pd.DataFrame, bucket_name: str, blob_name: str
    ) -> None:
        """Upload dataframe to bucket as a csv

        Args:
            df (pd.DataFrame): dataframe to upload
            bucket_name (str): bucket to upload to
            blob_name (str): blob name to save as
        """
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(buffer.getvalue(), content_type="text/csv")

    def delete_blob_from_gcs(self, bucket_name: str, blob_name: str) -> None:
        """Delete blob from gcs bucket

        Args:
            bucket_name (str): name of bucket blob is in
            blob_name (str): blob to delete
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        logging.info("%s.%s deleted", bucket_name, blob_name)
