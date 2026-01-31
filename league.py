import requests
import json
import numpy as np
import pandas as pd
from functools import cached_property

class League(object):
    def __init__(self, league_id, base_url="https://api.sleeper.app/v1"):
        """
        Initialize the League object.

        :param league_id: str, The unique identifier for the league
        :param base_url: str, Base URL for the Sleeper API
        """
        self.league_id = league_id
        self.base_url = base_url

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

    @cached_property
    def league_data(self):
        """
        Fetches the league data from the Sleeper API and stores it in the object.
        """
        endpoint = f"/league/{self.league_id}"
        self.league_data = self._get(endpoint)
        return self.league_data

    @cached_property
    def league_name(self):
        """
        Get the name of the league from the fetched league data.

        :return: str, League name or None if data is not fetched
        """
        return self.league_data.get("name")

    @cached_property
    def league_settings(self):
        """
        Get the league's settings (e.g., scoring system, roster size).

        :return: dict, League settings or None if data is not fetched
        """
        return self.league_data.get("settings")

    @cached_property
    def members(self):
        """
        Get the members of the league.

        :return: list, A list of league members or None if data is not fetched
        """
        endpoint = f"/league/{self.league_id}/users"

        return self._get(endpoint)

    @cached_property
    def rosters(self):
        """
        Get the rosters of the league.

        :return: list, A list of league rosters or None if data is not fetched
        """
        endpoint = f"/league/{self.league_id}/rosters"

        return self._get(endpoint)

    @cached_property
    def league_average_match(self):
        return bool(self.league_data['settings'].get('league_average_match'))

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

        pd_roster_map = pd.DataFrame(roster_map).reset_index(drop=True)
        return pd_roster_map

    @cached_property
    def historical_results(self):

        return np.stack([self.get_week_results(week).array for week in range(1, self.latest_reg_season_week)])

    @cached_property
    def power_rankings(self):

        roster_ids = self.historical_results[0,:,0].reshape(-1, 1)
        expected_wins = np.sum(self.historical_results[:,:,3], axis=0).reshape(-1, 1)
        natural_wins = np.sum(self.historical_results[:,:,2], axis=0).reshape(-1, 1)

        roster_wins = np.hstack((roster_ids, expected_wins, natural_wins))
        pd_roster_wins = pd.DataFrame(roster_wins, columns=['roster_id', 'expected_wins', 'natural_wins'])

        complete_power_rankings = pd.merge(self.roster_map,
                                           pd_roster_wins,
                                           on="roster_id",
                                           how="inner")

        complete_power_rankings['luckstat'] = complete_power_rankings['natural_wins'] - complete_power_rankings['expected_wins']

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

        matchups_data = self._get(endpoint)

        return WeekResults(performances= [Performance(perf, matchups_data) for perf in matchups_data])


class WeekResults(object):
    def __init__(self, performances):
        self.performances = performances

    @cached_property
    def array(self):
        raw_array = np.array([(perf.roster_id, perf.points, perf.natural_wins) for perf in self.performances])
        num_teams = len(raw_array)

        # Order by roster ID
        ordered_indices = np.argsort(raw_array[:, 0])
        ordered_array = raw_array[ordered_indices]

        # Assign conceptual wins against the rest of the league
        expected_wins = np.argsort(np.argsort(ordered_array[:,1]))/(num_teams - 1)
        final_array = np.hstack((ordered_array, expected_wins.reshape(-1, 1)))

        return final_array


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

