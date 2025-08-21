from dags.utils.SportsEventsAPI import SportsEventsAPI
import time

api = SportsEventsAPI()
response = api.get_events_paginated(page=1)
total_pages = response.body["total_pages"]
responses = []
for page in range(1, total_pages + 1):
    response = api.get_events_paginated(page=page)
    if response.status_code == 429:
        retry_time = int(response.body["retry_after"])
        time.sleep(retry_time)
print(responses)
