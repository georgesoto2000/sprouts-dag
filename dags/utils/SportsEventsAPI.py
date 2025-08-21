import pandas as pd


class SportsEventsAPI:
    """Placeholder class to fetch data from 'Sports Events API'"""

    def __init__(self) -> None:
        """Initiate class"""
        pass

    def get_events(self) -> pd.DataFrame:
        """Psuedo function to retrieve data from API. Actually reads data from csv.

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
