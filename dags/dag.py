from utils.GCPUtils import BigQueryClient
from utils.SportsEventsAPI import SportsEventsAPI
import sys
import os


# if __name__ == "__main__":
#     # sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
#     api = SportsEventsAPI()
#     df = api.get_events()
#     client = BigQueryClient(dotenvpath="./env/.env")
#     client.upload_dataframe_to_bigquery(
#         df=df,
#         dataset_id="sports_events",
#         table_id="events",
#         replace=True,
#     )
