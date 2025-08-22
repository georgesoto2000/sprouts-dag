"""Used to imitate fetching data from a Sports Events API"""

import pandas as pd
from typing import Tuple
from dataclasses import dataclass


@dataclass
class APIResponse:
    status_code: int
    body: dict


class SportsEventsAPI:
    """Placeholder class to fetch data from 'Sports Events API'"""

    def __init__(self) -> None:
        """Initiate class"""
        pass

    def get_events(self, **kwargs) -> pd.DataFrame:
        """Psuedo function to retrieve data from API. Actually returns below data.

        Returns:
            pd.DataFrame: Data from API.
        """
        events_dict = {
            "Date": ["2025-02-10", "2025-06-14"],
            "Sport": ["Football", "Tennis"],
            "Event": ["FA Cup Final", "Roland Garros: Nadal vs Djokovic"],
        }
        events = pd.DataFrame.from_dict(events_dict)
        return events

    def get_events_paginated(
        self, successful: bool = True, **kwargs
    ) -> Tuple[dict, int]:
        """psuedo function to retrieve paginated data from API. Actually returns below data.

        Returns:
            dict: json response
        """
        if successful:
            sample_response = {
                "page": 1,
                "per_page": 1,
                "total_pages": 1,
                "total_events": 1,
                "events": [
                    {
                        "id": "evt12345",
                        "Date": "2025-05-17",
                        "Sport": "Football",
                        "Event": "FA Cup Final",
                    }
                ],
            }
            sample_response_code = 200
        else:
            sample_response = {
                "error": "Rate limit exceeded",
                "retry_after": 60,
            }
            sample_response_code = 429
        response = APIResponse(status_code=sample_response_code, body=sample_response)
        return response
