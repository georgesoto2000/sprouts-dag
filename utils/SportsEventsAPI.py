import pandas as pd


class SportsEventsAPI:
    """Placeholder class to fetch data from 'Sports Events API'"""

    def __init__(self):
        """Initiate class"""
        pass

    def get_events(self) -> pd.DataFrame:
        """Psuedo function to retrieve data from API. Actually reads psuedo data from csv.

        Returns:
            pd.DataFrame: Data from API.
        """
        events = pd.read_csv("../data/sports_events.csv")
        return events
