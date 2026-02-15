from sleeperapi.api import SleeperConn
from sleeperapi.league import League
from functools import cached_property
from datetime import datetime

class User(object):
    def __init__(self, username, debug=False):
        self.username = username
        self.api       = SleeperConn(debug=debug)
        self.debug     = debug

    def __repr__(self):
        return f"User({self.username}, {self.leagues})"

    @cached_property
    def user_data(self):
        endpoint = endpoint = f"/user/{self.username}"
        user_data = self.api._get(endpoint)

        return user_data

    @cached_property
    def user_id(self):
        return self.user_data.get('user_id')

    @cached_property
    def leagues_metadata(self):
        current_year = datetime.now().year
        last_year = current_year - 1

        endpoint = f"/user/{self.user_id}/leagues/nfl/{current_year}"
        leagues_metadata = self.api._get(endpoint)

        endpoint = f"/user/{self.user_id}/leagues/nfl/{last_year}"
        leagues_metadata.extend(self.api._get(endpoint))

        return leagues_metadata

    @cached_property
    def leagues(self):
        leagues = []
        represented_leagues = []

        for league in self.leagues_metadata:
            league_id = league['league_id']
            if league_id not in represented_leagues:
                newleague = League(league_id, debug=self.debug)
                hist_leagues = newleague.historical_leagues
                leagues.append({'league':newleague, 'seasons':len(hist_leagues)})
                represented_leagues.extend([histleague.league_id for histleague in hist_leagues])

        return leagues
