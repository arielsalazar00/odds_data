import requests 
import json 
import pandas as pd

class OddsAccessor:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.the-odds-api.com"

    def get_sports(self) -> pd.DataFrame:
        """
        Returns a list of in-season sport objects. 
        The sport key can be used as the sport parameter in other endpoints. 
        This endpoint does not count against the usage quota.
        """

        endpoint = self.base_url + f"/v4/sports/?apiKey={self.api_key}"
        response = requests.get(endpoint)
        return pd.DataFrame(response.json())
    
    def get_odds(self, sport: str, markets: list[str], regions: list[str]) -> dict:
        """
        Returns a list of upcoming and live games with recent odds for a given sport, 
        region and markets
        """
        
        endpoint = self.base_url + f"/v4/sports/{sport}/odds?apiKey={self.api_key}&regions={regions}&markets={markets}"
        response = requests.get(endpoint)
        return pd.DataFrame(response.json())
    
    def get_events(self, sport:str) -> pd.DataFrame:
        """
        Returns a list of in-play and pre-match events for a specified sport or league. 
        The response includes event id, home and away teams, and the commence time for each event. 
        Odds are not included in the response. 
        This endpoint does not count against the usage quota.
        """

        endpoint = self.base_url + f"/v4/sports/{sport}/events?apiKey={self.api_key}"
        response = requests.get(endpoint)
        return pd.DataFrame(response.json())
    
    def get_odds_by_event(self, event_id: str) -> pd.DataFrame:
        """
        Returns odds for a single event. 
        Accepts any available betting markets using the markets parameter. 
        
        Coverage of non-featured markets is currently limited to selected bookmakers and 
        sports, and expanding over time.

        When to use this endpoint: Use this endpoint to access odds for any supported market. 
        Since the volume of data returned can be large, 
        these requests will only query one event at a time. 
        If you are only interested in the most popular betting markets, 
        including head-to-head (moneyline), point spreads (handicap), over/under (totals), 
        the main /odds endpoint is simpler to integrate and more cost-effective.
        """

        endpoint = self.base_url + f"/v4/events/{event_id}/odds?apiKey={self.api_key}"
        response = requests.get(endpoint)
        return pd.DataFrame(response.json())
    
    def get_historical_odds(self, sport: str, regions: list[str], markets: list[str], date: str) -> pd.DataFrame:
        """
        Returns a snapshot of games with bookmaker odds for a given sport, region and market, 
        at a given historical timestamp. 
        Historical odds data is available from June 6th 2020, 
        with snapshots taken at 10 minute intervals. 
        From September 2022, historical odds snapshots are available at 5 minute intervals. 
        This endpoint is only available on paid usage plans.

        date: The timestamp of the data snapshot to be returned, specified in ISO 8601 format. 
        For example, 2024-01-01T00:00:00Z. The historical odds API will return the closest snapshot
        equal to or earlier than the provided date parameter.
        """

        endpoint = self.base_url + f"/v4/historical/sports/{sport}/odds?apiKey={self.api_key}&regions={regions}&markets={markets}&date={date}"
        response = requests.get(endpoint)
        return response.json()
    
    def get_historical_events(self, sport: str, date: str, commence_after: str, commence_before: str) -> pd.DataFrame:
        """
        Returns a list of historical events at a specified timestamp for a given sport. 
        The response includes event id, home and away teams, and the commence time for each event. 
        Odds are not included in the response. 
        This endpoint can be used to find historical event ids to be used in the historical event odds endpoint. 
        This endpoint is only available on paid usage plans.
        """

        if commence_after and commence_before:
            endpoint = self.base_url + f"/v4/historical/sports/{sport}/events?apiKey={self.api_key}&date={date}&commence_after={commence_after}&commence_before={commence_before}"
        else:   
            endpoint = self.base_url + f"/v4/historical/sports/{sport}/events?apiKey={self.api_key}&date={date}"
        response = requests.get(endpoint)
        return response.json()

    def get_historical_event_odds(self, sport: str, event_id: str, regions: list[str], markets: list[str], date: str) -> pd.DataFrame:
        """
        Returns historical odds for a single event as they appeared at a specified timestamp. 
        Accepts any available betting markets using the markets parameter. 
        Historical data for additional markets (player props, alternate lines, period markets) 
        are available after 2023-05-03T05:30:00Z. 
        This endpoint is only available on paid usage plans.
        """

        endpoint = self.base_url + \
            f"/v4/historical/sports/{sport}/events/{event_id}/odds?apiKey={self.api_key}&regions={regions}&markets={markets}&date={date}"
        
        response = requests.get(endpoint)
        return pd.DataFrame(response.json())
    
def flatten_odds_data(hist_results):
    # Initialize empty lists to store flattened data
    flattened_data = []
    
    # Extract timestamp info
    timestamp = hist_results['timestamp']
    previous_timestamp = hist_results['previous_timestamp']
    next_timestamp = hist_results['next_timestamp']
    
    # Iterate through each event in the data
    for event in hist_results['data']:
        event_id = event['id']
        sport_key = event['sport_key']
        sport_title = event['sport_title']
        commence_time = event['commence_time']
        home_team = event['home_team']
        away_team = event['away_team']
        
        # Iterate through bookmakers
        for bookmaker in event['bookmakers']:
            bookmaker_key = bookmaker['key']
            bookmaker_title = bookmaker['title']
            bookmaker_last_update = bookmaker['last_update']
            
            # Iterate through markets
            for market in bookmaker['markets']:
                market_key = market['key']
                market_last_update = market['last_update']
                
                # Iterate through outcomes
                for outcome in market['outcomes']:
                    row = {
                        'timestamp': timestamp,
                        'previous_timestamp': previous_timestamp,
                        'next_timestamp': next_timestamp,
                        'id': event_id,
                        'sport_key': sport_key,
                        'sport_title': sport_title,
                        'commence_time': commence_time,
                        'home_team': home_team,
                        'away_team': away_team,
                        'bookmaker_key': bookmaker_key,
                        'bookmaker_title': bookmaker_title,
                        'bookmaker_last_update': bookmaker_last_update,
                        'market_key': market_key,
                        'market_last_update': market_last_update,
                        'outcome_name': outcome['name'],
                        'outcome_price': outcome['price'],
                        'outcome_point': outcome.get('point', None)  # Some markets (like h2h) don't have points
                    }
                    flattened_data.append(row)
    
    # Create DataFrame from flattened data
    df = pd.DataFrame(flattened_data)
    
    # Convert timestamp columns to datetime
    timestamp_cols = ['timestamp', 'previous_timestamp', 'next_timestamp', 
                     'commence_time', 'bookmaker_last_update', 'market_last_update']
    for col in timestamp_cols:
        df[col] = pd.to_datetime(df[col])
        
    return df
    