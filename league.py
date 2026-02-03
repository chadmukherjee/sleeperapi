import requests
import json
import polars as pl
from functools import cached_property

class SleeperConn(object):
    def __init__(self, base_url="https://api.sleeper.app/v1"):

        self.base_url=base_url

    def _get(self, endpoint):
        """
        Helper method to make GET requests to the Sleeper API.

        :param endpoint: str, API endpoint (relative to the base URL)
        :return: dict, JSON response from the API
        """
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching data from {url}: {e}")
            return None


class League(object):
    def __init__(self, league_id):
        """
        Initialize the League object.

        :param league_id: str, The unique identifier for the league
        :param base_url: str, Base URL for the Sleeper API
        """
        self.league_id = league_id
        self.api       = SleeperConn()

    def __repr__(self):
        return f"League({self.league_name}, {self.league_year}, league_id={self.league_id})"

    @cached_property
    def league_data(self):
        """
        Fetches the league data from the Sleeper API and stores it in the object.
        """
        endpoint = f"/league/{self.league_id}"
        self.league_data = self.api._get(endpoint)
        return self.league_data

    @cached_property
    def league_name(self):
        """
        Get the name of the league from the fetched league data.

        :return: str, League name or None if data is not fetched
        """
        return self.league_data.get("name")

    @cached_property
    def league_year(self):
        return str(self.league_data['season'])

    @cached_property
    def league_settings(self):
        """
        Get the league's settings (e.g., scoring system, roster size).

        :return: dict, League settings or None if data is not fetched
        """
        return self.league_data.get("settings")

    @cached_property
    def has_results(self):
        return bool(self.league_settings.get('last_scored_leg'))

    @cached_property
    def members(self):
        """
        Get the members of the league.

        :return: list, A list of league members or None if data is not fetched
        """
        endpoint = f"/league/{self.league_id}/users"

        return self.api._get(endpoint)

    @cached_property
    def rosters(self):
        """
        Get the rosters of the league.

        :return: list, A list of league rosters or None if data is not fetched
        """
        endpoint = f"/league/{self.league_id}/rosters"

        return self.api._get(endpoint)

    @cached_property
    def league_average_match(self):
        return bool(self.league_data['settings'].get('league_average_match'))

    @cached_property
    def previous_league(self):
        if self.league_data['previous_league_id'] is not None:
            return League(self.league_data['previous_league_id'])
        else:
            return None

    @cached_property
    def historical_leagues(self):
        historical_leagues = [self]
        if self.previous_league is not None:
            historical_leagues += self.previous_league.historical_leagues

        return historical_leagues


    @cached_property
    def roster_map(self):
        roster_map = []

        for roster in self.rosters:
            roster_metadata = {}
            roster_id = roster['roster_id']
            roster_metadata['roster_id'] = roster_id

            for member in self.members:
                if member['user_id'] == roster['owner_id']:
                    roster_metadata['team_name'] = member['metadata'].get('team_name', member['display_name'])
                    roster_metadata['wins'] = roster['settings']['wins']

                    roster_map.append(roster_metadata)
                    break

        pl_roster_map = pl.DataFrame(roster_map)

        return pl_roster_map

    @cached_property
    def historical_results(self):

        return pl.concat([self.get_week_results(week).df for week in range(1, self.latest_reg_season_week)])

    @cached_property
    def power_rankings(self):

        # Aggregate weekly metrics including cumulative sums
        agg_df = self.historical_results.group_by('roster_id').agg([
            pl.col('natural_wins').sum().alias('natural_wins'),
            pl.col('expected_wins').sum().alias('expected_wins'),
            pl.col('luck_index').sum().alias('luckstat'),
            pl.col('luck_index').cum_sum().alias('cumulative_luck')
        ])

        final_agg_df = agg_df.join(self.roster_map,
                                   on='roster_id',
                                   how='inner').drop('roster_id')

        return final_agg_df.sort('expected_wins', descending=True)


        return complete_power_rankings.drop(columns=['roster_id']).sort_values(by="expected_wins", ascending=False)

    @cached_property
    def latest_reg_season_week(self):
        return min(self.league_data['settings']['playoff_week_start'], self.league_data['settings']['leg'])

    def get_week_results(self, week):
        """
        Get the matchups for a given week in the league

        :return: list, A list of matchup metadata for the requested week
        """

        endpoint = f"/league/{self.league_id}/matchups/{week}"

        matchups_data = self.api._get(endpoint)

        return WeekResults(week, performances= [Performance(perf, matchups_data) for perf in matchups_data])


class WeekResults(object):
    def __init__(self, week, performances):
        self.performances = performances
        self.week = week

    @cached_property
    def df(self):

        num_teams = len(self.performances)

        raw_df = pl.DataFrame([(self.week, perf.roster_id, perf.points, perf.natural_wins) for perf in self.performances],
                              schema = {
                                  'week': pl.Int32,
                                  'roster_id': pl.Int32,
                                  'points': pl.Float64,
                                  'natural_wins': pl.Float64
                              },
                              orient='row')

        # Calculate Expected Wins for the week
        df = raw_df.with_columns(
            ((pl.col('points').rank(method='min') - 1)/(num_teams-1)).alias('expected_wins')
        )

        # Calculate Luck index for the week
        df = df.with_columns(
            (pl.col('natural_wins') - pl.col('expected_wins')).alias('luck_index')
        )

        return df


class Performance(object):
    def __init__(self, matchup_data, reference_data):
        self.matchup_data = matchup_data
        self.reference_data = reference_data
        self.matchup_id = matchup_data.get("matchup_id")
        self.roster_id = matchup_data.get("roster_id")
        self.points = matchup_data.get("points")
        self.starters = matchup_data.get("starters")
        self.starters_points = matchup_data.get("starters_points")
        self.players_points = matchup_data.get("players_points")


    def __repr__(self):
        return (
            f"Performance(matchup_id={self.matchup_id}, roster_id={self.roster_id}, points={self.points}, "
            f"starters={self.starters}, starters_points={self.starters_points},"
            f"opponent_roster_id={self.opponent_roster_id}, natural_wins={self.natural_wins})"
        )

    @cached_property
    def opponent_matchup_data(self):
        return [perf for perf in self.reference_data if perf['matchup_id'] == self.matchup_id and perf['roster_id'] != self.roster_id][0]

    @cached_property
    def opponent_roster_id(self):
        return self.opponent_matchup_data['roster_id']

    @cached_property
    def opponent_points(self):
        return self.opponent_matchup_data['points']

    @cached_property
    def natural_wins(self):
        if self.points > self.opponent_points:
            natural_wins = 1
        elif self.points < self.opponent_points:
            natural_wins = 0
        elif self.points == self.opponent_points:
            natural_wins = .5

        return natural_wins

class Matchup():
    def __init__(self, matchup_data):
        pass

class Roster(object):
    def __init__(self, roster_data):
        self.roster_id = roster_data.get("roster_id")
        self.owner_id = roster_data.get("owner_id")
        self.players = roster_data.get("players")
        self.starters = roster_data.get("starters")
        self.taxi = roster_data.get("taxi")
        self.reserve = roster_data.get("reserve")
        self.settings = roster_data.get("settings")
