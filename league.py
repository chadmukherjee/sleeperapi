import polars as pl
from sleeperapi.api import SleeperConn
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property

class League(object):
    def __init__(self, league_id, debug=False):
        """
        Initialize the League object.

        :param league_id: str, The unique identifier for the league
        :param base_url: str, Base URL for the Sleeper API
        """
        self.league_id = league_id
        self.api       = SleeperConn(debug=debug)
        self.debug     = debug

    def __repr__(self):
        return f"League({self.league_name}, {self.league_year}, league_id={self.league_id})"

    @cached_property
    def league_data(self):
        """
        Fetches the league data from the Sleeper API and stores it in the object.
        """
        endpoint = f"/league/{self.league_id}"
        league_data = self.api._get(endpoint)
        return league_data

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
    def num_teams(self):

        return len(self.members)

    @cached_property
    def rosters(self):
        """
        Get the rosters of the league.

        :return: list, A list of league rosters or None if data is not fetched
        """
        endpoint = f"/league/{self.league_id}/rosters"

        return self.api._get(endpoint)

    @cached_property
    def rosters_df(self) -> pl.DataFrame:
        """
        Dataframe of  rosters in the league.

        :return: pl.DataFrame, where each row is a player from a roster in the league
        """

        rosters_df = pl.DataFrame(
                        [
                            {
                                "roster_id": r["roster_id"],
                                "owner_id": r["owner_id"],
                                "players": r["players"] or [],
                            }
                            for r in self.rosters
                        ],
                        schema={
                            "roster_id": pl.Int64,
                            "owner_id": pl.String,
                            "players": pl.List(pl.String),
                        },
                    ) \
                    .explode("players") \
                    .rename({"players": "player_id"}) \
                    .drop_nulls("player_id")

        return rosters_df

    @cached_property
    def draft_id(self):
        """
        Get league's draft ID

        :return: int, the sleeper draft ID for the league's draft
        """

        return self.league_data.get('draft_id')

    @cached_property
    def draft(self):
        """
        Get league's draft

        :return: list, a list of draft picks
        """

        endpoint = f"/draft/{self.draft_id}/picks"

        return self.api._get(endpoint)

    @cached_property
    def draft_df(self) -> pl.DataFrame:
        """One row per draft pick."""
        return pl.DataFrame(
            [
                {
                    "roster_id": p["roster_id"],
                    "player_id": p["player_id"],
                    "round": p["round"],
                    "pick_no": p["pick_no"],
                    "is_keeper": bool(p.get("is_keeper")),
                    "first_name": p["metadata"].get("first_name"),
                    "last_name": p["metadata"].get("last_name"),
                    "position": p["metadata"].get("position"),
                }
                for p in self.draft
            ],
            schema={
                "roster_id": pl.Int64,
                "player_id": pl.String,
                "round": pl.Int64,
                "pick_no": pl.Int64,
                "is_keeper": pl.Boolean,
                "first_name": pl.String,
                "last_name": pl.String,
                "position": pl.String,
            },
        )



    @cached_property
    def drafted_roster_coverage_df(self) -> pl.DataFrame:
        """Share of each team's current roster that the team itself drafted."""
        drafted = self.draft_df.select("roster_id", "player_id").with_columns(
            pl.lit(True).alias("drafted_by_team")
        )

        return (
            self.rosters_df
            .join(drafted, on=["roster_id", "player_id"], how="left")
            .with_columns(pl.col("drafted_by_team").fill_null(False))
            .group_by("roster_id")
            .agg(
                pl.len().alias("roster_size"),
                pl.col("drafted_by_team").sum().alias("drafted_players"),
                pl.col("drafted_by_team").mean().alias("drafted_pct"),
            )
            .join(self.roster_map.select("roster_id", "team_name"), on="roster_id", how="left")
            .select("roster_id", "team_name", "roster_size", "drafted_players", "drafted_pct")
            .sort("drafted_pct", descending=True))


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

        # Use ThreadPoolExecutor for I/O-bound API calls
        with ThreadPoolExecutor(max_workers=self.latest_reg_season_week) as executor:
            # Submit all weeks in parallel
            futures = [executor.submit(self.get_week_results, week) for week in range(1, self.latest_reg_season_week)]

            # Collect results as they complete
            week_dfs = [future.result().df for future in futures]

        # Concatenate all results
        historical_results = pl.concat(week_dfs)
        return historical_results

    @cached_property
    def dominance_df(self):

        # Pivot to get ranks in matrix form: rows = weeks, columns = teams
        rank_matrix = self.historical_results.pivot(
            index='week',
            columns='roster_id',
            values='expected_wins'
        )

        roster_ids = list(map(str, self.historical_results['roster_id'].unique().to_list()))

        # Convert to matrix form
        ranks = rank_matrix.select(roster_ids).to_numpy()  # shape: (num_weeks, num_teams)

        # Calculate weekly wins against each other opponent (for every team)
        wins = (ranks[:, :, None] > ranks[:, None, :]).astype(float)  # shape: (weeks, teams, teams)

        # Calculate a win %age for each cross-team matchup across the weeks
        dominance = wins.mean(axis=0)  # shape: (teams, teams)

        # Convert back to polars dataframe
        dominance_df = pl.DataFrame(
            dominance,
            schema={str(roster_id): pl.Float64 for roster_id in roster_ids}
        ).with_columns(
            pl.Series('roster_id', roster_ids).cast(pl.Int32)
        ).select(['roster_id'] + [str(rid) for rid in roster_ids])

        indexable_dominance_df = dominance_df.with_columns(pl.concat_arr(*[pl.col(str(i)) for i in range(1,self.num_teams + 1)]).alias('dominance_array')).select('roster_id', 'dominance_array')

        return indexable_dominance_df

    @cached_property
    def power_rankings(self):

        historical_results = self.historical_results.join(self.dominance_df,
                                                          on='roster_id',
                                                          how='left')

        historical_results = historical_results.with_columns((pl.col('natural_wins') - pl.col('dominance_array').arr.get(pl.col('opponent_roster_id') - 1)).alias('upset_aware_luckstat'))

        # Aggregate weekly metrics including cumulative sums
        agg_df = historical_results.group_by('roster_id').agg([
            pl.col('natural_wins').sum().alias('natural_wins'),
            pl.col('expected_wins').sum().alias('expected_wins'),
            pl.col('upset_aware_luckstat').sum().alias('upset_aware_luckstat'),
            pl.col('luck_index').sum().alias('luckstat'),
            pl.col('luck_index').cum_sum().alias('cumulative_luck')
        ])

        final_agg_df = agg_df.join(self.roster_map,
                                   on='roster_id',
                                   how='inner') \
                              .join(self.drafted_roster_coverage_df.select('roster_id', 'drafted_pct'),
                                    on='roster_id',
                                    how='inner') \
                              .drop('roster_id')

        return final_agg_df.sort('expected_wins', descending=True)

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

        raw_df = pl.DataFrame([(self.week, perf.roster_id, perf.opponent_roster_id, perf.points, perf.natural_wins) for perf in self.performances],
                              schema = {
                                  'week': pl.Int32,
                                  'roster_id': pl.Int32,
                                  'opponent_roster_id': pl.Int32,
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
