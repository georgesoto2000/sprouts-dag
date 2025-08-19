import pandas as pd
from google.cloud import bigquery


class BigQueryClient:
    """Retrieve and write data to BigQuery"""

    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def upload_dataframe_to_bigquery(
        self, df: pd.DataFrame, dataset_id: str, table_id: str, replace: bool
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
        df.to_gbq(
            destination_table=table_ref,
            project_id=self.project_id,
            if_exists=if_exists,
            location="EU",
        )
