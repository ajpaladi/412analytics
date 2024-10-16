import pandas as pd
import requests
from tqdm import tqdm
from pprint import pprint
import re

class Fetch():

    def completed_games(self, year, week=None, season_type=None, team=None):

        if team is not None and not isinstance(team, list):
            team = [team]

        if not season_type and not week:
            season_type = 2
            weeks = list(range(1, 19)) if year > 2020 else list(range(1, 18))
        elif week and not season_type:
            weeks = str(week)
            season_type = 2
        elif season_type == 1:
            weeks = list(range(1, 5))
        elif season_type == 2:
            weeks = list(range(1, 19)) if year > 2020 else list(range(1, 18))
        elif season_type == 3:
            weeks = list(range(1, 6))
        else:
            season_type = season_type
            weeks = week

        total_completed = pd.DataFrame()

        for w in weeks:
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?limit=1000&dates={year}&seasontype={season_type}&week={w}'
            print(url)
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                df = pd.json_normalize(data['events'])
                df.to_csv('/Users/andrewpaladino/Downloads/events.csv')
                df = df[(df['status.type.name'] == 'STATUS_FINAL')]
                df['id'] = df['id'].astype(str)


                extracted = {'id': [], 'attendance': [], 'conference_competition': [], 'home_team': [], 'away_team': [],
                             'home_score': [],
                             'away_score': [], 'passing_leader': [], 'rushing_leader': [], 'receiving_leader': [],
                             'venue': [], 'city':[], 'indoor': [], 'long_headline': [], 'short_headline': [], 'highlight_link': []}

                for x in df.competitions:

                    id = x[0]['id']
                    attendance = x[0]['attendance']
                    conference_competition = x[0]['conferenceCompetition']
                    home_team = x[0]['competitors'][0]['team']['displayName']
                    away_team = x[0]['competitors'][1]['team']['displayName']
                    home_score = x[0]['competitors'][0]['score']
                    away_score = x[0]['competitors'][1]['score']
                    passing_leader = x[0]['leaders'][0]['leaders'][0]['athlete']['displayName']
                    rushing_leader = x[0]['leaders'][1]['leaders'][0]['athlete']['displayName']
                    receiving_leader = x[0]['leaders'][2]['leaders'][0]['athlete']['displayName']
                    venue = x[0]['venue']['fullName']
                    city = x[0]['venue']['address']['city']
                    indoor = x[0]['venue']['indoor']
                    long_headline = x[0].get('headlines', [{}])[0].get('description', 'NA')
                    short_headline = x[0].get('headlines', [{}])[0].get('shortLinkText', 'NA')
                    highlight_link = (x[0].get('headlines', [{}])[0]
                                      .get('video', [{}])[0]
                                      .get('links', {})
                                      .get('source', {})
                                      .get('mezzanine', {})
                                      .get('href', 'NA'))
                    extracted['id'].append(id)
                    extracted['attendance'].append(attendance)
                    extracted['conference_competition'].append(conference_competition)
                    extracted['home_team'].append(home_team)
                    extracted['away_team'].append(away_team)
                    extracted['home_score'].append(home_score)
                    extracted['away_score'].append(away_score)
                    extracted['passing_leader'].append(passing_leader)
                    extracted['rushing_leader'].append(rushing_leader)
                    extracted['receiving_leader'].append(receiving_leader)
                    extracted['venue'].append(venue)
                    extracted['city'].append(city)
                    extracted['indoor'].append(indoor)
                    extracted['long_headline'].append(long_headline)
                    extracted['short_headline'].append(short_headline)
                    extracted['highlight_link'].append(highlight_link)

                extracted_df = pd.DataFrame(extracted)
                extracted_df['id'] = extracted_df['id'].astype(str)
                events_merged = df.merge(extracted_df, on='id')

                if team:
                    events_merged = events_merged[(events_merged.home_team.isin(team)) |
                                                  (events_merged.away_team.isin(team))]

                total_completed = pd.concat([events_merged, total_completed])



        return total_completed

        ### need to do team, opponent, points scored, points allowed

    def scheduled_games(self, year, week, season_type=None):

        if not season_type:
            season_type = 2

            # year, season_type, #week
        url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?limit=1000&dates={year}&seasontype={season_type}&week={week}'
        print(url)

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.json_normalize(data['events'])
            df = df[(df['status.type.name'] == 'STATUS_SCHEDULED')]

            extracted = {'id': [], 'attendance': [], 'conference_competition': [], 'home_team': [], 'away_team': [],
                         'home_score': [],
                         'away_score': [], 'passing_leader': [], 'rushing_leader': [], 'receiving_leader': [],
                         'venue_name': [],
                         'venue_address': [], 'indoor': []}

            for x in df.competitions:
                # print('\n')
                # pprint(x)
                id = x[0]['id']
                attendance = x[0]['attendance']
                conference_competition = x[0]['conferenceCompetition']
                home_team = x[0]['competitors'][0]['team']['displayName']
                away_team = x[0]['competitors'][1]['team']['displayName']
                home_score = x[0]['competitors'][0]['score']
                away_score = x[0]['competitors'][1]['score']
                passing_leader = x[0]['leaders'][0]['leaders'][0]['athlete']['displayName']
                rushing_leader = x[0]['leaders'][1]['leaders'][0]['athlete']['displayName']
                receiving_leader = x[0]['leaders'][2]['leaders'][0]['athlete']['displayName']
                venue_name = x[0]['venue']['fullName']
                venue_address = x[0]['venue']['address']
                indoor = x[0]['venue']['indoor']
                extracted['id'].append(id)
                extracted['attendance'].append(attendance)
                extracted['conference_competition'].append(conference_competition)
                extracted['home_team'].append(home_team)
                extracted['away_team'].append(away_team)
                extracted['home_score'].append(home_score)
                extracted['away_score'].append(away_score)
                extracted['passing_leader'].append(passing_leader)
                extracted['rushing_leader'].append(rushing_leader)
                extracted['receiving_leader'].append(receiving_leader)
                extracted['venue_name'].append(venue_name)
                extracted['venue_address'].append(venue_address)
                extracted['indoor'].append(indoor)

            extracted_df = pd.DataFrame(extracted)
            events_merged = df.merge(extracted_df, on='id')
            # #events_merged = events_merged.drop(columns=['competitions', 'links', 'weather.displayValue',
            #                                             'weather.temperature', 'weather.highTemperature',
            #                                             'weather.conditionId', 'weather.link.language',
            #                                             'weather.link.rel', 'weather.link.href', 'weather.link.text',
            #                                             'weather.link.shortText', 'weather.link.isExternal',
            #                                             'weather.link.isPremium', 'status.clock',
            #                                             'status.displayClock', 'status.type.altDetail'])

        return events_merged

    def in_progress_games(self, year, week, season_type=None):

        if not season_type:
            season_type = 2

            # year, season_type, #week
        url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?limit=1000&dates={year}&seasontype={season_type}&week={week}'
        print(url)

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.json_normalize(data['events'])
            df = df[(df['status.type.name'] == 'STATUS_IN_PROGRESS')]

            extracted = {'id': [], 'attendance': [], 'conference_competition': [], 'home_team': [], 'away_team': [],
                         'home_score': [],
                         'away_score': [], 'passing_leader': [], 'rushing_leader': [], 'receiving_leader': [],
                         'venue_name': [],
                         'venue_address': [], 'indoor': []}

            for x in df.competitions:
                # print('\n')
                # pprint(x)
                id = x[0]['id']
                attendance = x[0]['attendance']
                conference_competition = x[0]['conferenceCompetition']
                home_team = x[0]['competitors'][0]['team']['displayName']
                away_team = x[0]['competitors'][1]['team']['displayName']
                home_score = x[0]['competitors'][0]['score']
                away_score = x[0]['competitors'][1]['score']
                passing_leader = x[0]['leaders'][0]['leaders'][0]['athlete']['displayName']
                rushing_leader = x[0]['leaders'][1]['leaders'][0]['athlete']['displayName']
                receiving_leader = x[0]['leaders'][2]['leaders'][0]['athlete']['displayName']
                venue_name = x[0]['venue']['fullName']
                venue_address = x[0]['venue']['address']
                indoor = x[0]['venue']['indoor']
                extracted['id'].append(id)
                extracted['attendance'].append(attendance)
                extracted['conference_competition'].append(conference_competition)
                extracted['home_team'].append(home_team)
                extracted['away_team'].append(away_team)
                extracted['home_score'].append(home_score)
                extracted['away_score'].append(away_score)
                extracted['passing_leader'].append(passing_leader)
                extracted['rushing_leader'].append(rushing_leader)
                extracted['receiving_leader'].append(receiving_leader)
                extracted['venue_name'].append(venue_name)
                extracted['venue_address'].append(venue_address)
                extracted['indoor'].append(indoor)

            extracted_df = pd.DataFrame(extracted)
            events_merged = df.merge(extracted_df, on='id')
            # #events_merged = events_merged.drop(columns=['competitions', 'links', 'weather.displayValue',
            #                                             'weather.temperature', 'weather.highTemperature',
            #                                             'weather.conditionId', 'weather.link.language',
            #                                             'weather.link.rel', 'weather.link.href', 'weather.link.text',
            #                                             'weather.link.shortText', 'weather.link.isExternal',
            #                                             'weather.link.isPremium', 'status.clock',
            #                                             'status.displayClock', 'status.type.altDetail'])

        return events_merged

    def team_boxscore(self, year, week=None, season_type=None, team=None, pivot=None):

        #### need to convert time of possession
        #### need to do more with defensive pivot
        #### need to add completion %

        boxscore_team = {'id': [], 'date': [], 'city': [], 'venue': [], 'team': [], 'home_team': [], 'away_team': [],
                         'home_score': [], 'away_score': [], 'nickname': [], 'abbv': [],
                         'first_downs': [], 'passing_first_downs': [], 'rushing_first_downs': [],
                         'penalty_first_downs': [], 'third_down_eff': [], 'fourth_down_eff': [],
                         'total_plays': [], 'total_yards': [], 'yards_per_play': [], 'total_drives': [],
                         'net_passing_yards': [], 'completions': [], 'attempts': [],
                         'yards_per_pass': [], 'interceptions_thrown': [], 'sacks_yards_lost': [], 'rushing_yards': [],
                         'rushing_attempts': [], 'yards_per_rush': [],
                         'red_zone_success': [], 'penalties_yards': [], 'turnovers': [], 'fumbles_lost': [],
                         'def_st_touchdowns': [], 'time_of_possession': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
            zip(completed_games['id'].unique(),
                completed_games['date'],
                completed_games['city'],
                completed_games['venue'],
                completed_games['home_team'],
                completed_games['away_team'],
                completed_games['home_score'],
                completed_games['away_score']),
            total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                for team in data['boxscore']['teams']:

                    team_name = team['team']['displayName']
                    nickname = team['team']['shortDisplayName']
                    abbv = team['team']['abbreviation']
                    statistics = team.get('statistics', [])
                    stats_values = {stat.get('name'): stat.get('displayValue', 'NA') for stat in statistics}
                    first_downs = stats_values.get('firstDowns', 'NA')
                    passing_first_downs = stats_values.get('firstDownsPassing', 'NA')
                    rushing_first_downs = stats_values.get('firstDownsRushing', 'NA')
                    penalty_first_downs = stats_values.get('firstDownsPenalty', 'NA')
                    third_down_eff = stats_values.get('thirdDownEff', None)
                    fourth_down_eff = stats_values.get('fourthDownEff', None)
                    total_plays = stats_values.get('totalOffensivePlays', 'NA')
                    total_yards = stats_values.get('totalYards', 'NA')
                    yards_per_play = stats_values.get('yardsPerPlay', 'NA')
                    total_drives = stats_values.get('totalDrives', 'NA')
                    net_passing_yards = stats_values.get('netPassingYards', 'NA')
                    # For statistics requiring special handling or parsing
                    completions_attempts = stats_values.get('completionAttempts', '0-0')
                    if '/' in completions_attempts:
                        completions, attempts = [int(x) for x in completions_attempts.split('/')]
                    elif '-' in completions_attempts:
                        completions, attempts = [int(x) for x in completions_attempts.split('-')]
                    else:
                        completions, attempts = 0, 0  # Fallback if no valid separator is found
                    yards_per_pass = stats_values.get('yardsPerPass', 'NA')
                    interceptions_thrown = stats_values.get('interceptions', 'NA')
                    sacks_yards_lost = stats_values.get('sacksYardsLost', 'NA')
                    rushing_yards = stats_values.get('rushingYards', 'NA')
                    rushing_attempts = stats_values.get('rushingAttempts', 'NA')
                    yards_per_rush = stats_values.get('yardsPerRushAttempt', 'NA')
                    red_zone_success = stats_values.get('redZoneAttempts', None)
                    penalties_yards = stats_values.get('totalPenaltiesYards', 'NA')
                    turnovers = stats_values.get('turnovers', 'NA')
                    fumbles_lost = stats_values.get('fumblesLost', 'NA')
                    def_st_touchdowns = stats_values.get('defensiveTouchdowns', 'NA')
                    time_of_possession = stats_values.get('possessionTime', 'NA')

                    boxscore_team['id'].append(id)
                    boxscore_team['date'].append(date)
                    boxscore_team['city'].append(city)
                    boxscore_team['venue'].append(venue)
                    boxscore_team['team'].append(team_name)
                    boxscore_team['home_team'].append(home_team)
                    boxscore_team['away_team'].append(away_team)
                    boxscore_team['home_score'].append(home_score)
                    boxscore_team['away_score'].append(away_score)
                    boxscore_team['nickname'].append(nickname)
                    boxscore_team['abbv'].append(abbv)
                    boxscore_team['first_downs'].append(first_downs)
                    boxscore_team['passing_first_downs'].append(passing_first_downs)
                    boxscore_team['rushing_first_downs'].append(rushing_first_downs)
                    boxscore_team['penalty_first_downs'].append(penalty_first_downs)
                    boxscore_team['third_down_eff'].append(third_down_eff)
                    boxscore_team['fourth_down_eff'].append(fourth_down_eff)
                    boxscore_team['total_plays'].append(total_plays)
                    boxscore_team['total_yards'].append(total_yards)
                    boxscore_team['yards_per_play'].append(yards_per_play)
                    boxscore_team['total_drives'].append(total_drives)
                    boxscore_team['net_passing_yards'].append(net_passing_yards)
                    boxscore_team['completions'].append(completions)
                    boxscore_team['attempts'].append(attempts)
                    boxscore_team['yards_per_pass'].append(yards_per_pass)
                    boxscore_team['interceptions_thrown'].append(interceptions_thrown)
                    boxscore_team['sacks_yards_lost'].append(sacks_yards_lost)
                    boxscore_team['rushing_yards'].append(rushing_yards)
                    boxscore_team['rushing_attempts'].append(rushing_attempts)
                    boxscore_team['yards_per_rush'].append(yards_per_rush)
                    boxscore_team['red_zone_success'].append(red_zone_success)
                    boxscore_team['penalties_yards'].append(penalties_yards)
                    boxscore_team['turnovers'].append(turnovers)
                    boxscore_team['fumbles_lost'].append(fumbles_lost)
                    boxscore_team['def_st_touchdowns'].append(def_st_touchdowns)
                    boxscore_team['time_of_possession'].append(time_of_possession)

        boxscore_team_df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in boxscore_team.items()]))

        def assign_points_and_opponent(row):
            if row['team'] == row['home_team']:
                row['points'] = row['home_score']
                row['points_allowed'] = row['away_score']
                row['opponent'] = row['away_team']
            else:
                row['points'] = row['away_score']
                row['points_allowed'] = row['home_score']
                row['opponent'] = row['home_team']
            return row

        boxscore_team_df = boxscore_team_df.apply(assign_points_and_opponent, axis=1)
        boxscore_team_df.drop(columns={'home_team', 'away_team', 'home_score', 'away_score'})
        column_reorder = ['id', 'date', 'city', 'venue', 'team', 'nickname', 'abbv', 'opponent', 'points',
                          'points_allowed', 'first_downs', 'passing_first_downs',
                          'rushing_first_downs', 'penalty_first_downs', 'third_down_eff', 'fourth_down_eff',
                          'total_plays', 'total_yards', 'yards_per_play',
                          'total_drives', 'net_passing_yards', 'completions', 'attempts', 'yards_per_pass',
                          'interceptions_thrown', 'sacks_yards_lost',
                          'rushing_yards', 'rushing_attempts', 'yards_per_rush', 'red_zone_success', 'penalties_yards',
                          'turnovers', 'fumbles_lost',
                          'def_st_touchdowns', 'time_of_possession']

        boxscore_team_df = boxscore_team_df.reindex(columns=column_reorder)

        def calculate_efficiency(eff):
            if isinstance(eff, str) and '-' in eff:
                try:
                    successful, total = map(int, eff.split('-'))
                    return (successful / total) * 100 if total != 0 else None
                except ValueError:
                    # Handles cases where eff cannot be split into two integers
                    return None
            else:
                # Handles cases where eff is 'NA', None, or any other non-string value
                return None

        # Apply this function to each efficiency column
        boxscore_team_df['third_down_success_%'] = boxscore_team_df['third_down_eff'].apply(calculate_efficiency)
        boxscore_team_df['fourth_down_success_%'] = boxscore_team_df['fourth_down_eff'].apply(calculate_efficiency)
        boxscore_team_df['red_zone_success_%'] = boxscore_team_df['red_zone_success'].apply(calculate_efficiency)
        boxscore_team_df[['times_sacked', 'sack_yards']] = boxscore_team_df['sacks_yards_lost'].str.split('-',
                                                                                                          expand=True)
        boxscore_team_df[['penalties', 'penalty_yards']] = boxscore_team_df['penalties_yards'].str.split('-',
                                                                                                         expand=True)

        columns_to_int = ['points', 'points_allowed', 'first_downs', 'passing_first_downs', 'rushing_first_downs',
                             'penalty_first_downs', 'total_plays', 'total_yards', 'total_drives',
                             'net_passing_yards', 'interceptions_thrown', 'rushing_yards', 'rushing_attempts',
                          'turnovers', 'fumbles_lost', 'times_sacked', 'sack_yards', 'penalties', 'penalty_yards']

        columns_to_float = ['yards_per_play', 'yards_per_pass', 'yards_per_rush']

        boxscore_team_df[columns_to_int] = boxscore_team_df[columns_to_int].astype(int)
        boxscore_team_df[columns_to_float] = boxscore_team_df[columns_to_float].astype(float)

        boxscore_team_df['point_differential'] = boxscore_team_df['points'] - boxscore_team_df['points_allowed']
        boxscore_team_df['result'] = ['W' if points > points_allowed else 'L' for points, points_allowed in zip(boxscore_team_df['points'], boxscore_team_df['points_allowed'])]

        boxscore_offense_pivot = boxscore_team_df.groupby('team').agg({'points': ['mean', 'sum'],
                                                           'points_allowed': ['mean', 'sum'],
                                                           'point_differential': ['mean', 'sum'],
                                                           'first_downs': ['mean', 'sum'],
                                                           'passing_first_downs':  ['mean', 'sum'],
                                                           'rushing_first_downs': ['mean', 'sum'],
                                                           'penalty_first_downs': ['mean', 'sum'],
                                                           'total_plays': ['sum'],
                                                           'total_yards':  ['mean', 'sum'],
                                                           'yards_per_play': ['mean'],
                                                           'total_drives': ['mean', 'sum'],
                                                           'net_passing_yards':  ['mean', 'sum'],
                                                           'completions': ['mean', 'sum'],
                                                           'attempts': ['mean', 'sum'],
                                                           'yards_per_pass': ['mean'],
                                                           'interceptions_thrown': ['mean', 'sum'],
                                                           'rushing_yards': ['mean', 'sum'],
                                                           'rushing_attempts': ['mean', 'sum'],
                                                           'yards_per_rush': ['mean', 'sum'],
                                                           'turnovers': ['mean', 'sum'],
                                                           'fumbles_lost': ['mean', 'sum'],
                                                           'third_down_success_%': ['mean'],
                                                           'fourth_down_success_%': ['mean'],
                                                           'red_zone_success_%': ['mean'],
                                                           'times_sacked': ['mean', 'sum'],
                                                           'sack_yards': ['mean', 'sum'],
                                                           'penalties': ['mean', 'sum'],
                                                           'penalty_yards': ['mean', 'sum']}).reset_index()

        boxscore_offense_pivot.columns = ['_'.join(col).strip() for col in boxscore_offense_pivot.columns.values]

        boxscore_defense_pivot = boxscore_team_df.groupby('opponent').agg({'points': ['mean', 'sum'],
                                                           'points_allowed': ['mean', 'sum'],
                                                           'first_downs': ['mean', 'sum'],
                                                           'passing_first_downs':  ['mean', 'sum'],
                                                           'rushing_first_downs': ['mean', 'sum'],
                                                           'penalty_first_downs': ['mean', 'sum'],
                                                           'total_plays': ['sum'],
                                                           'total_yards':  ['mean', 'sum'],
                                                           'yards_per_play': ['mean'],
                                                           'total_drives': ['mean', 'sum'],
                                                           'net_passing_yards':  ['mean', 'sum'],
                                                           'completions': ['mean', 'sum'],
                                                           'attempts': ['mean', 'sum'],
                                                           'yards_per_pass': ['mean'],
                                                           'interceptions_thrown': ['mean', 'sum'],
                                                           'rushing_yards': ['mean', 'sum'],
                                                           'rushing_attempts': ['mean', 'sum'],
                                                           'yards_per_rush': ['mean', 'sum'],
                                                           'turnovers': ['mean', 'sum'],
                                                           'fumbles_lost': ['mean', 'sum'],
                                                           'third_down_success_%': ['mean'],
                                                           'fourth_down_success_%': ['mean'],
                                                           'red_zone_success_%': ['mean'],
                                                           'times_sacked': ['mean', 'sum'],
                                                           'sack_yards': ['mean', 'sum'],
                                                           'penalties': ['mean', 'sum'],
                                                           'penalty_yards': ['mean', 'sum']}).reset_index()

        boxscore_defense_pivot.columns = ['_'.join(col).strip() for col in boxscore_defense_pivot.columns.values]

        if pivot == 'defense':
            return boxscore_defense_pivot
        elif pivot == 'offense':
            return boxscore_offense_pivot
        else:
            return boxscore_team_df

    def passing_boxscore(self, year, week=None, season_type=None, team=None, pivot=None):

        #### need to add in touchdown %

        passing_dict = {'date':[], 'team': [], 'name': [], 'athlete_id': [], 'number': [], 'completions_attempts': [], 'yards': [],
                        'yards_per_attempt': [], 'touchdowns': [], 'interceptions': [], 'sacks': [], 'adjusted_qbr': [],
                        'passer_rating': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                team_list = []

                for x in data['boxscore']['players']:
                    team = x['team']['displayName']
                    team_list.append(team)

                for x in data['boxscore']['players'][0]['statistics'][0]['athletes']:
                    team = team_list[0]
                    name = x['athlete']['displayName']
                    athlete_id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    completions_attempts = x['stats'][0]
                    yards = x['stats'][1]
                    yards_per_attempt = x['stats'][2]
                    touchdowns = x['stats'][3]
                    interceptions = x['stats'][4]
                    sacks = x['stats'][5]
                    adjusted_qbr = x['stats'][6]
                    passer_rating = x['stats'][7]
                    passing_dict['date'].append(date)
                    passing_dict['team'].append(team)
                    passing_dict['name'].append(name)
                    passing_dict['athlete_id'].append(athlete_id)
                    passing_dict['number'].append(number)
                    passing_dict['completions_attempts'].append(completions_attempts)
                    passing_dict['yards'].append(yards)
                    passing_dict['yards_per_attempt'].append(yards_per_attempt)
                    passing_dict['touchdowns'].append(touchdowns)
                    passing_dict['interceptions'].append(interceptions)
                    passing_dict['sacks'].append(sacks)
                    passing_dict['adjusted_qbr'].append(adjusted_qbr)
                    passing_dict['passer_rating'].append(passer_rating)

                for x in data['boxscore']['players'][1]['statistics'][0]['athletes']:
                    team = team_list[1]
                    name = x['athlete']['displayName']
                    athlete_id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    completions_attempts = x['stats'][0]
                    yards = x['stats'][1]
                    yards_per_attempt = x['stats'][2]
                    touchdowns = x['stats'][3]
                    interceptions = x['stats'][4]
                    sacks = x['stats'][5]
                    adjusted_qbr = x['stats'][6]
                    passer_rating = x['stats'][7]
                    passing_dict['date'].append(date)
                    passing_dict['team'].append(team)
                    passing_dict['name'].append(name)
                    passing_dict['athlete_id'].append(athlete_id)
                    passing_dict['number'].append(number)
                    passing_dict['completions_attempts'].append(completions_attempts)
                    passing_dict['yards'].append(yards)
                    passing_dict['yards_per_attempt'].append(yards_per_attempt)
                    passing_dict['touchdowns'].append(touchdowns)
                    passing_dict['interceptions'].append(interceptions)
                    passing_dict['sacks'].append(sacks)
                    passing_dict['adjusted_qbr'].append(adjusted_qbr)
                    passing_dict['passer_rating'].append(passer_rating)

        passing_df = pd.DataFrame(passing_dict)
        passing_df[['completions', 'attempts']] = passing_df['completions_attempts'].str.split('/', expand=True)
        passing_df[['times_sacked', 'sack_yards']] = passing_df['sacks'].str.split('-', expand=True)
        passing_df[['yards_per_attempt', 'adjusted_qbr', 'passer_rating']] = passing_df[['yards_per_attempt', 'adjusted_qbr', 'passer_rating']].replace('--', 0)

        columns_to_int = ['completions', 'attempts', 'yards', 'touchdowns', 'interceptions', 'times_sacked','sack_yards', ]
        columns_to_float = ['yards_per_attempt', 'adjusted_qbr', 'passer_rating']
        passing_df[columns_to_int] = passing_df[columns_to_int].astype(int)
        passing_df[columns_to_float] = passing_df[columns_to_float].astype(float)

        passing_df['completion_%'] = passing_df['completions'] / passing_df['attempts'] * 100
        passing_df['yards_per_completion'] = passing_df['yards'] / passing_df['completions']
        passing_df = passing_df.drop(columns='completions_attempts')

        passing_df['touchdown_%'] = (passing_df['touchdowns'] / passing_df['attempts']) * 100

        passing_pivot = passing_df.groupby('name').agg({'yards': ['mean', 'sum'],
                                                           'yards_per_attempt': ['mean'],
                                                           'yards_per_completion':['mean'],
                                                           'touchdowns':  ['mean', 'sum'],
                                                           'touchdown_%': ['mean'],
                                                           'interceptions': ['mean', 'sum'],
                                                           'adjusted_qbr': ['mean'],
                                                           'passer_rating': ['mean'],
                                                           'completions':  ['mean', 'sum'],
                                                           'attempts': ['mean', 'sum'],
                                                           'completion_%': ['mean'],
                                                           'times_sacked':  ['mean', 'sum'],
                                                           'sack_yards': ['mean', 'sum']}).reset_index()

        passing_pivot.columns = ['_'.join(col).strip() for col in passing_pivot.columns.values]

        if pivot == True:
            return passing_pivot
        else:
            return passing_df

    def rushing_boxscore(self, year, week=None, season_type=None, team=None, pivot=None):

        #### need to add in touchdown %

        rushing_dict = {'date': [], 'team': [], 'name': [], 'id': [], 'number': [], 'attempts': [],
                        'yards': [], 'yards_per_attempt': [], 'touchdowns': [], 'longest_run': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                team_list = []

                for x in data['boxscore']['players']:
                    team = x['team']['displayName']
                    team_list.append(team)

                for x in data['boxscore']['players'][0]['statistics'][1]['athletes']:
                    team = team_list[0]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    attempts = x['stats'][0]
                    yards = x['stats'][1]
                    yards_per_attempt = x['stats'][2]
                    touchdowns = x['stats'][3]
                    longest_run = x['stats'][4]
                    rushing_dict['date'].append(date)
                    rushing_dict['team'].append(team)
                    rushing_dict['name'].append(name)
                    rushing_dict['id'].append(id)
                    rushing_dict['number'].append(number)
                    rushing_dict['attempts'].append(attempts)
                    rushing_dict['yards'].append(yards)
                    rushing_dict['yards_per_attempt'].append(yards_per_attempt)
                    rushing_dict['touchdowns'].append(touchdowns)
                    rushing_dict['longest_run'].append(longest_run)


                for x in data['boxscore']['players'][1]['statistics'][1]['athletes']:
                    team = team_list[1]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    attempts = x['stats'][0]
                    yards = x['stats'][1]
                    yards_per_attempt = x['stats'][2]
                    touchdowns = x['stats'][3]
                    longest_run = x['stats'][4]
                    rushing_dict['date'].append(date)
                    rushing_dict['team'].append(team)
                    rushing_dict['name'].append(name)
                    rushing_dict['id'].append(id)
                    rushing_dict['number'].append(number)
                    rushing_dict['attempts'].append(attempts)
                    rushing_dict['yards'].append(yards)
                    rushing_dict['yards_per_attempt'].append(yards_per_attempt)
                    rushing_dict['touchdowns'].append(touchdowns)
                    rushing_dict['longest_run'].append(longest_run)


        rushing_df = pd.DataFrame(rushing_dict)

        columns_to_int = ['attempts', 'yards', 'touchdowns', 'longest_run']
        columns_to_float = ['yards_per_attempt']
        rushing_df[columns_to_int] = rushing_df[columns_to_int].astype(int)
        rushing_df[columns_to_float] = rushing_df[columns_to_float].astype(float)

        rushing_pivot = rushing_df.groupby('name').agg({'attempts': ['mean', 'sum'],
                                                           'yards': ['mean', 'sum'],
                                                           'yards_per_attempt':['mean'],
                                                           'touchdowns':  ['mean', 'sum'],
                                                           'longest_run': ['mean', 'sum']}).reset_index()

        rushing_pivot.columns = ['_'.join(col).strip() for col in rushing_pivot.columns.values]

        if pivot == True:
            return rushing_pivot
        else:
            return rushing_df

    def receiving_boxscore(self, year, week=None, season_type=None, team=None, pivot=None, team_pivot=None):

        #### need to add in touchdown %
        receiving_dict = {'date': [], 'team': [], 'name': [], 'id': [], 'number': [], 'receptions': [],
                          'yards': [], 'yards_per_reception': [], 'touchdowns': [], 'longest_reception': [],
                          'targets':[]}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                team_list = []

                for x in data['boxscore']['players']:
                    team = x['team']['displayName']
                    team_list.append(team)


                for x in data['boxscore']['players'][0]['statistics'][2]['athletes']:
                    team = team_list[0]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    receptions = x['stats'][0]
                    yards = x['stats'][1]
                    yards_per_reception = x['stats'][2]
                    touchdowns = x['stats'][3]
                    longest_reception = x['stats'][4]
                    targets = x['stats'][5]
                    receiving_dict['date'].append(date)
                    receiving_dict['team'].append(team)
                    receiving_dict['name'].append(name)
                    receiving_dict['id'].append(id)
                    receiving_dict['number'].append(number)
                    receiving_dict['receptions'].append(receptions)
                    receiving_dict['yards'].append(yards)
                    receiving_dict['yards_per_reception'].append(yards_per_reception)
                    receiving_dict['touchdowns'].append(touchdowns)
                    receiving_dict['longest_reception'].append(longest_reception)
                    receiving_dict['targets'].append(targets)

                for x in data['boxscore']['players'][1]['statistics'][2]['athletes']:
                    team = team_list[1]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    receptions = x['stats'][0]
                    yards = x['stats'][1]
                    yards_per_reception = x['stats'][2]
                    touchdowns = x['stats'][3]
                    longest_reception = x['stats'][4]
                    targets = x['stats'][5]
                    receiving_dict['date'].append(date)
                    receiving_dict['team'].append(team)
                    receiving_dict['name'].append(name)
                    receiving_dict['id'].append(id)
                    receiving_dict['number'].append(number)
                    receiving_dict['receptions'].append(receptions)
                    receiving_dict['yards'].append(yards)
                    receiving_dict['yards_per_reception'].append(yards_per_reception)
                    receiving_dict['touchdowns'].append(touchdowns)
                    receiving_dict['longest_reception'].append(longest_reception)
                    receiving_dict['targets'].append(targets)


        receiving_df = pd.DataFrame(receiving_dict)

        columns_to_int = ['receptions', 'yards', 'touchdowns', 'longest_reception', 'targets']
        columns_to_float = ['yards_per_reception']
        receiving_df[columns_to_int] = receiving_df[columns_to_int].astype(int)
        receiving_df[columns_to_float] = receiving_df[columns_to_float].astype(float)

        receiving_df['catch_%'] = receiving_df['receptions'] / receiving_df['targets'] * 100

        ###### player pivot #######

        receiving_pivot = receiving_df.groupby('name').agg({'receptions': ['mean', 'sum'],
                                                            'yards': ['mean', 'sum'],
                                                            'yards_per_reception': ['mean'],
                                                            'touchdowns': ['mean', 'sum'],
                                                            'longest_reception': ['mean', 'sum'],
                                                            'targets': ['mean', 'sum'],
                                                            'catch_%': ['mean']
                                                            }).reset_index()

        receiving_pivot.columns = ['_'.join(col).strip() for col in receiving_pivot.columns.values]

        ####### team pivot #####

        receiving_team_pivot = receiving_df.groupby('team').agg({'receptions': ['mean', 'sum'],
                                                            'yards': ['mean', 'sum'],
                                                            'yards_per_reception': ['mean'],
                                                            'touchdowns': ['mean', 'sum'],
                                                            'longest_reception': ['mean', 'sum'],
                                                            'targets': ['mean', 'sum'],
                                                            'catch_%': ['mean']
                                                            }).reset_index()

        if pivot == True:
            return receiving_pivot
        elif team_pivot == True:
            return receiving_team_pivot
        else:
            return receiving_df

    def fumble_boxscore(self, year, week=None, season_type=None, team=None, pivot=None):

        fumble_dict = {'date': [], 'team': [], 'name': [], 'id': [], 'number': [], 'fumbles': [],
                          'fumbles_lost': [], 'fumbles_recovered': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                team_list = []

                for x in data['boxscore']['players']:
                    team = x['team']['displayName']
                    team_list.append(team)

                for x in data['boxscore']['players'][0]['statistics'][3]['athletes']:
                    team = team_list[0]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    fumbles = x['stats'][0]
                    fumbles_lost = x['stats'][1]
                    fumbles_recovered = x['stats'][2]
                    fumble_dict['date'].append(date)
                    fumble_dict['team'].append(team)
                    fumble_dict['name'].append(name)
                    fumble_dict['id'].append(id)
                    fumble_dict['number'].append(number)
                    fumble_dict['fumbles'].append(fumbles)
                    fumble_dict['fumbles_lost'].append(fumbles_lost)
                    fumble_dict['fumbles_recovered'].append(fumbles_recovered)

                for x in data['boxscore']['players'][1]['statistics'][3]['athletes']:
                    team = team_list[1]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    fumbles = x['stats'][0]
                    fumbles_lost = x['stats'][1]
                    fumbles_recovered = x['stats'][2]
                    fumble_dict['date'].append(date)
                    fumble_dict['team'].append(team)
                    fumble_dict['name'].append(name)
                    fumble_dict['id'].append(id)
                    fumble_dict['number'].append(number)
                    fumble_dict['fumbles'].append(fumbles)
                    fumble_dict['fumbles_lost'].append(fumbles_lost)
                    fumble_dict['fumbles_recovered'].append(fumbles_recovered)

        fumble_df = pd.DataFrame(fumble_dict)

        columns_to_int = ['fumbles', 'fumbles_lost', 'fumbles_recovered']
        fumble_df[columns_to_int] = fumble_df[columns_to_int].astype(int)

        fumble_pivot = fumble_df.groupby('name').agg({'fumbles': ['mean', 'sum'],
                                                            'fumbles_lost': ['mean', 'sum'],
                                                            'fumbles_recovered': ['mean', 'sum']
                                                            }).reset_index()

        fumble_pivot.columns = ['_'.join(col).strip() for col in fumble_pivot.columns.values]

        if pivot == True:
            return fumble_pivot
        else:
            return fumble_df

    def defensive_boxscore(self, year, week=None, season_type=None, team=None, pivot=None):

        defensive_dict = {'date': [], 'team': [], 'name': [], 'id': [], 'number': [], 'total_tackles': [],
                       'solo_tackles': [], 'sacks': [], 'tackles_for_loss':[], 'passes_defended':[],
                       'qb_hits':[], 'defensive_touchdowns':[]}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                team_list = []

                for x in data['boxscore']['players']:
                    team = x['team']['displayName']
                    team_list.append(team)

                for x in data['boxscore']['players'][0]['statistics'][4]['athletes']:
                    team = team_list[0]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    total_tackles = x['stats'][0]
                    solo_tackles = x['stats'][1]
                    sacks = x['stats'][2]
                    tackles_for_loss = x['stats'][3]
                    passes_defended = x['stats'][4]
                    qb_hits = x['stats'][5]
                    defensive_touchdowns = x['stats'][6]

                    defensive_dict['date'].append(date)
                    defensive_dict['team'].append(team)
                    defensive_dict['name'].append(name)
                    defensive_dict['id'].append(id)
                    defensive_dict['number'].append(number)
                    defensive_dict['total_tackles'].append(total_tackles)
                    defensive_dict['solo_tackles'].append(solo_tackles)
                    defensive_dict['sacks'].append(sacks)
                    defensive_dict['tackles_for_loss'].append(tackles_for_loss)
                    defensive_dict['passes_defended'].append(passes_defended)
                    defensive_dict['qb_hits'].append(qb_hits)
                    defensive_dict['defensive_touchdowns'].append(defensive_touchdowns)


                for x in data['boxscore']['players'][1]['statistics'][4]['athletes']:
                    team = team_list[1]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    total_tackles = x['stats'][0]
                    solo_tackles = x['stats'][1]
                    sacks = x['stats'][2]
                    tackles_for_loss = x['stats'][3]
                    passes_defended = x['stats'][4]
                    qb_hits = x['stats'][5]
                    defensive_touchdowns = x['stats'][6]

                    defensive_dict['date'].append(date)
                    defensive_dict['team'].append(team)
                    defensive_dict['name'].append(name)
                    defensive_dict['id'].append(id)
                    defensive_dict['number'].append(number)
                    defensive_dict['total_tackles'].append(total_tackles)
                    defensive_dict['solo_tackles'].append(solo_tackles)
                    defensive_dict['sacks'].append(sacks)
                    defensive_dict['tackles_for_loss'].append(tackles_for_loss)
                    defensive_dict['passes_defended'].append(passes_defended)
                    defensive_dict['qb_hits'].append(qb_hits)
                    defensive_dict['defensive_touchdowns'].append(defensive_touchdowns)

        defensive_df = pd.DataFrame(defensive_dict)

        columns_to_float = ['total_tackles', 'solo_tackles', 'sacks', 'tackles_for_loss', 'passes_defended', 'qb_hits', 'defensive_touchdowns']
        defensive_df[columns_to_float] = defensive_df[columns_to_float].astype(float)

        defensive_df['assisted_tackles'] = defensive_df['total_tackles'] - defensive_df['solo_tackles']

        defensive_pivot = defensive_df.groupby('name').agg({'total_tackles': ['mean', 'sum'],
                                                            'solo_tackles': ['mean', 'sum'],
                                                            'assisted_tackles': ['mean'],
                                                            'sacks': ['mean', 'sum'],
                                                            'tackles_for_loss': ['mean', 'sum'],
                                                            'passes_defended': ['mean', 'sum'],
                                                            'qb_hits': ['mean', 'sum'],
                                                            'defensive_touchdowns': ['mean', 'sum']
                                                            }).reset_index()

        defensive_pivot.columns = ['_'.join(col).strip() for col in defensive_pivot.columns.values]

        if pivot == True:
            return defensive_pivot
        else:
            return defensive_df

    def interception_boxscore(self, year, week=None, season_type=None, team=None, pivot=None):

        # 'keys': ['interceptions',
        #          'interceptionYards',
        #          'interceptionTouchdowns'],

        interception_dict = {'date': [], 'team': [], 'name': [], 'id': [], 'number': [], 'interceptions': [],
                          'interception_yards': [], 'interception_touchdowns': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                team_list = []

                for x in data['boxscore']['players']:
                    team = x['team']['displayName']
                    team_list.append(team)

                for x in data['boxscore']['players'][0]['statistics'][5]['athletes']:
                    team = team_list[0]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    interceptions = x['stats'][0]
                    interception_yards = x['stats'][1]
                    interception_touchdowns = x['stats'][2]

                    interception_dict['date'].append(date)
                    interception_dict['team'].append(team)
                    interception_dict['name'].append(name)
                    interception_dict['id'].append(id)
                    interception_dict['number'].append(number)
                    interception_dict['interceptions'].append(interceptions)
                    interception_dict['interception_yards'].append(interception_yards)
                    interception_dict['interception_touchdowns'].append(interception_touchdowns)

                for x in data['boxscore']['players'][1]['statistics'][5]['athletes']:
                    team = team_list[1]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    interceptions = x['stats'][0]
                    interception_yards = x['stats'][1]
                    interception_touchdowns = x['stats'][2]

                    interception_dict['date'].append(date)
                    interception_dict['team'].append(team)
                    interception_dict['name'].append(name)
                    interception_dict['id'].append(id)
                    interception_dict['number'].append(number)
                    interception_dict['interceptions'].append(interceptions)
                    interception_dict['interception_yards'].append(interception_yards)
                    interception_dict['interception_touchdowns'].append(interception_touchdowns)


        interception_df = pd.DataFrame(interception_dict)

        columns_to_int = ['interceptions', 'interception_yards', 'interception_touchdowns']
        interception_df[columns_to_int] = interception_df[columns_to_int].astype(int)


        interception_pivot = interception_df.groupby('name').agg({'interceptions': ['mean', 'sum'],
                                                            'interception_yards': ['mean', 'sum'],
                                                            'interception_touchdowns': ['mean', 'sum']
                                                            }).reset_index()

        interception_pivot.columns = ['_'.join(col).strip() for col in interception_pivot.columns.values]

        if pivot == True:
            return interception_pivot
        else:
            return interception_df

    def kick_return_boxscore(self, year, week=None, season_type=None, team=None, pivot=None):

        # 'keys': ['kickReturns',
        #          'kickReturnYards',
        #          'yardsPerKickReturn',
        #          'longKickReturn',
        #          'kickReturnTouchdowns'],

        kick_return_dict = {'date': [], 'team': [], 'name': [], 'id': [], 'number': [], 'kick_returns': [],
                             'kick_return_yards': [], 'yards_per_kick_return': [], 'longest_kick_return': [],
                             'kick_return_touchdowns':[]}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                team_list = []

                for x in data['boxscore']['players']:
                    team = x['team']['displayName']
                    team_list.append(team)

                for x in data['boxscore']['players'][0]['statistics'][6]['athletes']:
                    team = team_list[0]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    kick_returns = x['stats'][0]
                    kick_return_yards = x['stats'][1]
                    yards_per_kick_return = x['stats'][2]
                    longest_kick_return = x['stats'][3]
                    kick_return_touchdowns = x['stats'][4]

                    kick_return_dict['date'].append(date)
                    kick_return_dict['team'].append(team)
                    kick_return_dict['name'].append(name)
                    kick_return_dict['id'].append(id)
                    kick_return_dict['number'].append(number)
                    kick_return_dict['kick_returns'].append(kick_returns)
                    kick_return_dict['kick_return_yards'].append(kick_return_yards)
                    kick_return_dict['yards_per_kick_return'].append(yards_per_kick_return)
                    kick_return_dict['longest_kick_return'].append(longest_kick_return)
                    kick_return_dict['kick_return_touchdowns'].append(kick_return_touchdowns)

                for x in data['boxscore']['players'][1]['statistics'][6]['athletes']:
                    team = team_list[1]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    kick_returns = x['stats'][0]
                    kick_return_yards = x['stats'][1]
                    yards_per_kick_return = x['stats'][2]
                    longest_kick_return = x['stats'][3]
                    kick_return_touchdowns = x['stats'][4]

                    kick_return_dict['date'].append(date)
                    kick_return_dict['team'].append(team)
                    kick_return_dict['name'].append(name)
                    kick_return_dict['id'].append(id)
                    kick_return_dict['number'].append(number)
                    kick_return_dict['kick_returns'].append(kick_returns)
                    kick_return_dict['kick_return_yards'].append(kick_return_yards)
                    kick_return_dict['yards_per_kick_return'].append(yards_per_kick_return)
                    kick_return_dict['longest_kick_return'].append(longest_kick_return)
                    kick_return_dict['kick_return_touchdowns'].append(kick_return_touchdowns)

        kick_return_df = pd.DataFrame(kick_return_dict)

        columns_to_int = ['kick_returns', 'kick_return_yards', 'longest_kick_return', 'kick_return_touchdowns']
        columns_to_float = ['yards_per_kick_return']
        kick_return_df[columns_to_int] = kick_return_df[columns_to_int].astype(int)
        kick_return_df[columns_to_float] = kick_return_df[columns_to_float].astype(float)

        kick_return_pivot = kick_return_df.groupby('name').agg({'kick_returns': ['mean', 'sum'],
                                                                'kick_return_yards': ['mean', 'sum'],
                                                                'yards_per_kick_return': ['mean'],
                                                                'longest_kick_return': ['mean', 'sum'],
                                                                'kick_return_touchdowns': ['mean', 'sum']
                                                                  }).reset_index()

        kick_return_pivot.columns = ['_'.join(col).strip() for col in kick_return_pivot.columns.values]

        if pivot == True:
            return kick_return_pivot
        else:
            return kick_return_df

    def punt_return_boxscore(self, year, week=None, season_type=None, team=None, pivot=None):

        # 'keys': ['puntReturns',
        #          'puntReturnYards',
        #          'yardsPerPuntReturn',
        #          'longPuntReturn',
        #          'puntReturnTouchdowns'],

        punt_return_dict = {'date': [], 'team': [], 'name': [], 'id': [], 'number': [], 'punt_returns': [],
                            'punt_return_yards': [], 'yards_per_punt_return': [], 'longest_punt_return': [],
                            'punt_return_touchdowns': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                team_list = []

                for x in data['boxscore']['players']:
                    team = x['team']['displayName']
                    team_list.append(team)

                for x in data['boxscore']['players'][0]['statistics'][7]['athletes']:
                    team = team_list[0]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    punt_returns = x['stats'][0]
                    punt_return_yards = x['stats'][1]
                    yards_per_punt_return = x['stats'][2]
                    longest_punt_return = x['stats'][3]
                    punt_return_touchdowns = x['stats'][4]

                    punt_return_dict['date'].append(date)
                    punt_return_dict['team'].append(team)
                    punt_return_dict['name'].append(name)
                    punt_return_dict['id'].append(id)
                    punt_return_dict['number'].append(number)
                    punt_return_dict['punt_returns'].append(punt_returns)
                    punt_return_dict['punt_return_yards'].append(punt_return_yards)
                    punt_return_dict['yards_per_punt_return'].append(yards_per_punt_return)
                    punt_return_dict['longest_punt_return'].append(longest_punt_return)
                    punt_return_dict['punt_return_touchdowns'].append(punt_return_touchdowns)

                for x in data['boxscore']['players'][1]['statistics'][7]['athletes']:
                    team = team_list[1]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    punt_returns = x['stats'][0]
                    punt_return_yards = x['stats'][1]
                    yards_per_punt_return = x['stats'][2]
                    longest_punt_return = x['stats'][3]
                    punt_return_touchdowns = x['stats'][4]

                    punt_return_dict['date'].append(date)
                    punt_return_dict['team'].append(team)
                    punt_return_dict['name'].append(name)
                    punt_return_dict['id'].append(id)
                    punt_return_dict['number'].append(number)
                    punt_return_dict['punt_returns'].append(punt_returns)
                    punt_return_dict['punt_return_yards'].append(punt_return_yards)
                    punt_return_dict['yards_per_punt_return'].append(yards_per_punt_return)
                    punt_return_dict['longest_punt_return'].append(longest_punt_return)
                    punt_return_dict['punt_return_touchdowns'].append(punt_return_touchdowns)


        punt_return_df = pd.DataFrame(punt_return_dict)

        columns_to_int = ['punt_returns', 'punt_return_yards', 'longest_punt_return', 'punt_return_touchdowns']
        columns_to_float = ['yards_per_punt_return']
        punt_return_df[columns_to_int] = punt_return_df[columns_to_int].astype(int)
        punt_return_df[columns_to_float] = punt_return_df[columns_to_float].astype(float)

        punt_return_pivot = punt_return_df.groupby('name').agg({'punt_returns': ['mean', 'sum'],
                                                                'punt_return_yards': ['mean', 'sum'],
                                                                'yards_per_punt_return': ['mean'],
                                                                'longest_punt_return': ['mean', 'sum'],
                                                                'punt_return_touchdowns': ['mean', 'sum']
                                                                }).reset_index()

        punt_return_pivot.columns = ['_'.join(col).strip() for col in punt_return_pivot.columns.values]

        if pivot == True:
            return punt_return_pivot
        else:
            return punt_return_df

    def field_goal_boxscore(self, year, week=None, season_type=None, team=None, pivot=None):

        field_goal_dict = {'date': [], 'team': [], 'name': [], 'id': [], 'number': [], 'field_goals_made': [],
                            'field_goals_attempted': [], 'field_goal_%': [], 'long_field_goal': [],
                            'extra_points_made': [], 'extra_points_attempted':[], 'total_kicking_points':[]}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                team_list = []

                for x in data['boxscore']['players']:
                    team = x['team']['displayName']
                    team_list.append(team)

                for x in data['boxscore']['players'][0]['statistics'][8]['athletes']:
                    team = team_list[0]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')

                    field_goals_made_attempted = x['stats'][0]
                    if '/' in field_goals_made_attempted:
                        field_goals_made, field_goals_attempted = [int(x) for x in field_goals_made_attempted.split('/')]
                    elif '-' in field_goals_made_attempted:
                        field_goals_made, field_goals_attempted = [int(x) for x in field_goals_made_attempted.split('-')]
                    else:
                        field_goals_made, field_goals_attempted = 0, 0  # Fallback if no valid separator is found

                    field_goal_percent = x['stats'][1]
                    long_field_goal = x['stats'][2]

                    extra_points_made_attempted = x['stats'][3]
                    if '/' in extra_points_made_attempted:
                        extra_points_made, extra_points_attempted = [int(x) for x in
                                                                   extra_points_made_attempted.split('/')]
                    elif '-' in extra_points_made_attempted:
                        extra_points_made, extra_points_attempted = [int(x) for x in
                                                                   extra_points_made_attempted.split('-')]
                    else:
                        extra_points_made, extra_points_attempted = 0, 0  # Fallback if no valid separator is found

                    total_kicking_points = x['stats'][4]

                    field_goal_dict['date'].append(date)
                    field_goal_dict['team'].append(team)
                    field_goal_dict['name'].append(name)
                    field_goal_dict['id'].append(id)
                    field_goal_dict['number'].append(number)
                    field_goal_dict['field_goals_made'].append(field_goals_made)
                    field_goal_dict['field_goals_attempted'].append(field_goals_attempted)
                    field_goal_dict['field_goal_%'].append(field_goal_percent)
                    field_goal_dict['long_field_goal'].append(long_field_goal)
                    field_goal_dict['extra_points_made'].append(extra_points_made)
                    field_goal_dict['extra_points_attempted'].append(extra_points_attempted)
                    field_goal_dict['total_kicking_points'].append(total_kicking_points)


                for x in data['boxscore']['players'][1]['statistics'][8]['athletes']:
                    team = team_list[1]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')

                    field_goals_made_attempted = x['stats'][0]
                    if '/' in field_goals_made_attempted:
                        field_goals_made, field_goals_attempted = [int(x) for x in
                                                                   field_goals_made_attempted.split('/')]
                    elif '-' in field_goals_made_attempted:
                        field_goals_made, field_goals_attempted = [int(x) for x in
                                                                   field_goals_made_attempted.split('-')]
                    else:
                        field_goals_made, field_goals_attempted = 0, 0  # Fallback if no valid separator is found

                    field_goal_percent = x['stats'][1]
                    long_field_goal = x['stats'][2]

                    extra_points_made_attempted = x['stats'][3]
                    if '/' in extra_points_made_attempted:
                        extra_points_made, extra_points_attempted = [int(x) for x in
                                                                     extra_points_made_attempted.split('/')]
                    elif '-' in extra_points_made_attempted:
                        extra_points_made, extra_points_attempted = [int(x) for x in
                                                                     extra_points_made_attempted.split('-')]
                    else:
                        extra_points_made, extra_points_attempted = 0, 0  # Fallback if no valid separator is found

                    total_kicking_points = x['stats'][4]

                    field_goal_dict['date'].append(date)
                    field_goal_dict['team'].append(team)
                    field_goal_dict['name'].append(name)
                    field_goal_dict['id'].append(id)
                    field_goal_dict['number'].append(number)
                    field_goal_dict['field_goals_made'].append(field_goals_made)
                    field_goal_dict['field_goals_attempted'].append(field_goals_attempted)
                    field_goal_dict['field_goal_%'].append(field_goal_percent)
                    field_goal_dict['long_field_goal'].append(long_field_goal)
                    field_goal_dict['extra_points_made'].append(extra_points_made)
                    field_goal_dict['extra_points_attempted'].append(extra_points_attempted)
                    field_goal_dict['total_kicking_points'].append(total_kicking_points)


        field_goal_df = pd.DataFrame(field_goal_dict)

        columns_to_int = ['field_goals_made', 'field_goals_attempted', 'long_field_goal', 'extra_points_made', 'extra_points_attempted', 'total_kicking_points']
        columns_to_float = ['field_goal_%']
        field_goal_df[columns_to_int] = field_goal_df[columns_to_int].astype(int)
        field_goal_df[columns_to_float] = field_goal_df[columns_to_float].astype(float)

        field_goal_df['extra_point_%'] = field_goal_df['extra_points_made'] / field_goal_df['extra_points_attempted']

        field_goal_pivot = field_goal_df.groupby('name').agg({'field_goals_made': ['mean', 'sum'],
                                                                'field_goals_attempted': ['mean', 'sum'],
                                                                'field_goal_%': ['mean'],
                                                                'long_field_goal': ['mean', 'sum'],
                                                                'extra_points_made': ['mean', 'sum'],
                                                                'extra_points_attempted': ['mean', 'sum'],
                                                                'extra_point_%': ['mean'],
                                                                'total_kicking_points': ['mean', 'sum']
                                                                }).reset_index()

        field_goal_pivot.columns = ['_'.join(col).strip() for col in field_goal_pivot.columns.values]

        if pivot == True:
            return field_goal_pivot
        else:
            return field_goal_df

    def punt_boxscore(self, year, week=None, season_type=None, team=None, pivot=None):

        # 'keys': ['punts',
        #          'puntYards',
        #          'grossAvgPuntYards',
        #          'touchbacks',
        #          'puntsInside20',
        #          'longPunt']

        punt_dict = {'date': [], 'team': [], 'name': [], 'id': [], 'number': [], 'punts': [],
                            'punt_yards': [], 'gross_avg_punt_yards': [], 'touchbacks': [],
                            'punts_inside_20': [], 'longest_punt':[]}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]
            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                team_list = []

                for x in data['boxscore']['players']:
                    team = x['team']['displayName']
                    team_list.append(team)

                for x in data['boxscore']['players'][0]['statistics'][9]['athletes']:
                    team = team_list[0]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    punts = x['stats'][0]
                    punt_yards = x['stats'][1]
                    gross_avg_punt_yards = x['stats'][2]
                    touchbacks = x['stats'][3]
                    punts_inside_20 = x['stats'][4]
                    longest_punt = x['stats'][5]

                    punt_dict['date'].append(date)
                    punt_dict['team'].append(team)
                    punt_dict['name'].append(name)
                    punt_dict['id'].append(id)
                    punt_dict['number'].append(number)
                    punt_dict['punts'].append(punts)
                    punt_dict['punt_yards'].append(punt_yards)
                    punt_dict['gross_avg_punt_yards'].append(gross_avg_punt_yards)
                    punt_dict['touchbacks'].append(touchbacks)
                    punt_dict['punts_inside_20'].append(punts_inside_20)
                    punt_dict['longest_punt'].append(longest_punt)

                for x in data['boxscore']['players'][1]['statistics'][9]['athletes']:
                    team = team_list[1]
                    name = x['athlete']['displayName']
                    id = x['athlete']['id']
                    number = x['athlete'].get('jersey', 'NA')
                    punts = x['stats'][0]
                    punt_yards = x['stats'][1]
                    gross_avg_punt_yards = x['stats'][2]
                    touchbacks = x['stats'][3]
                    punts_inside_20 = x['stats'][4]
                    longest_punt = x['stats'][5]

                    punt_dict['date'].append(date)
                    punt_dict['team'].append(team)
                    punt_dict['name'].append(name)
                    punt_dict['id'].append(id)
                    punt_dict['number'].append(number)
                    punt_dict['punts'].append(punts)
                    punt_dict['punt_yards'].append(punt_yards)
                    punt_dict['gross_avg_punt_yards'].append(gross_avg_punt_yards)
                    punt_dict['touchbacks'].append(touchbacks)
                    punt_dict['punts_inside_20'].append(punts_inside_20)
                    punt_dict['longest_punt'].append(longest_punt)

        punt_df = pd.DataFrame(punt_dict)

        columns_to_int = ['punts', 'punt_yards', 'touchbacks', 'punts_inside_20', 'longest_punt']
        columns_to_float = ['gross_avg_punt_yards']
        punt_df[columns_to_int] = punt_df[columns_to_int].astype(int)
        punt_df[columns_to_float] = punt_df[columns_to_float].astype(float)

        punt_pivot = punt_df.groupby('name').agg({'punts': ['mean', 'sum'],
                                                    'punt_yards': ['mean', 'sum'],
                                                    'gross_avg_punt_yards':['mean'],
                                                    'touchbacks': ['mean'],
                                                    'punts_inside_20': ['mean', 'sum'],
                                                    'longest_punt': ['mean', 'sum']
                                                                }).reset_index()

        punt_pivot.columns = ['_'.join(col).strip() for col in punt_pivot.columns.values]

        if pivot == True:
            return punt_pivot
        else:
            return punt_df

    def drives(self,  year, week=None, season_type=None, team=None, pivot=None):

        drive_dict = {'team': [], 'home_team':[], 'away_team':[], 'date':[], 'drive_id': [], 'description': [], 'start_time': [], 'end_time': [],
                      'start_yardline': [], 'end_yardline': [], 'start_period': [], 'end_period': [],
                      'time_elapsed': [], 'yards': [], 'score': [], 'offensive_plays': [],
                      'result': [], 'display_result': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]

            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                for x in data['drives']['previous']:
                    team = x['team']['displayName']
                    drive_id = x['id']
                    description = x['description']
                    start_time = x['start']['clock']['displayValue']
                    end_time = x.get('end', {}).get('clock', {}).get('displayValue', 'NA')
                    start_yardline = x['start']['text']
                    end_yardline = x['end']['text']
                    start_period = x['start']['period']['number']
                    end_period = x['end']['period']['number']
                    time_elapsed = x['timeElapsed']['displayValue']
                    yards = x['yards']
                    score = x['isScore']
                    offensive_plays = x['offensivePlays']
                    result = x['result']
                    display_result = x['displayResult']

                    drive_dict['team'].append(team)
                    drive_dict['home_team'].append(home_team)
                    drive_dict['away_team'].append(away_team)
                    drive_dict['date'].append(date)
                    drive_dict['drive_id'].append(drive_id)
                    drive_dict['description'].append(description)
                    drive_dict['start_time'].append(start_time)
                    drive_dict['end_time'].append(end_time)
                    drive_dict['start_yardline'].append(start_yardline)
                    drive_dict['end_yardline'].append(end_yardline)
                    drive_dict['start_period'].append(start_period)
                    drive_dict['end_period'].append(end_period)

                    drive_dict['time_elapsed'].append(time_elapsed)
                    drive_dict['yards'].append(yards)
                    drive_dict['score'].append(score)
                    drive_dict['offensive_plays'].append(offensive_plays)
                    drive_dict['result'].append(result)
                    drive_dict['display_result'].append(display_result)

        drive_df = pd.DataFrame(drive_dict)

        def assign_points_and_opponent(row):
            if row['team'] == row['home_team']:
                row['opponent'] = row['away_team']
            else:
                row['opponent'] = row['home_team']
            return row

        drive_df = drive_df.apply(assign_points_and_opponent, axis=1)
        drive_df = drive_df.drop(columns = ['home_team', 'away_team'])
        drive_df = drive_df.sort_values(by = 'team', ascending = True)


        columns_to_int = ['yards', 'offensive_plays']
        drive_df[columns_to_int] = drive_df[columns_to_int].astype(int)
        drive_df['time_elapsed'] = '00:' + drive_df['time_elapsed']
        drive_df['time_elapsed'] = pd.to_timedelta(drive_df['time_elapsed'])

        drive_pivot = drive_df.groupby('team').agg({'yards': ['mean', 'sum'],
                                                  'offensive_plays': ['mean', 'sum'],
                                                  'time_elapsed': ['mean', 'sum']
                                                  }).reset_index()

        drive_pivot.columns = ['_'.join(col).strip() for col in drive_pivot.columns.values]

        if pivot == True:
            return drive_pivot
        else:
            return drive_df

    def offensive_drive_summary(self, year=None, week=None, season_type=None, team=None, agg_method=None):


        drives = self.drives(year=year, week=week, season_type=season_type, team=None)

        # Determine whether to aggregate weekly or yearly
        if agg_method == 'weekly':
            # Assume you have a 'date' or 'week' column in the dataframe
            group_cols = ['team', 'date']  # Using 'week' as an example, replace with 'date' if needed
        else:
            group_cols = ['team']

        result_count = drives.groupby(group_cols + ['display_result']).size().reset_index(name='count')
        total_drives = drives.groupby(group_cols).size().reset_index(name='total_drives')
        result_percent = pd.merge(result_count, total_drives, on=group_cols)
        result_percent['percentage'] = (result_percent['count'] / result_percent['total_drives']) * 100
        pivot_table = result_percent.pivot_table(index=group_cols, columns='display_result', values='percentage',
                                                 fill_value=0)

        drives['is_scoring'] = drives['display_result'].isin(['Touchdown', 'Field Goal'])
        scoring_drives = drives.groupby(group_cols)['is_scoring'].mean().reset_index(name='point_%')
        scoring_drives['point_%'] *= 100  # Convert to percentage

        drives['is_turnover'] = drives['display_result'].isin(['Fumble', 'Fumble Return Touchdown', 'Fumble Touchdown', 'Interception', 'Interception Touchdown'])
        turnover_drives = drives.groupby(group_cols)['is_turnover'].mean().reset_index(name='turnover_%')
        turnover_drives['turnover_%'] *= 100  # Convert to percentage

        drives['splash_allowed_%'] = drives['display_result'].isin(['Fumble', 'Interception', 'Blocked FG Touchdown', 'Safety', 'Fumble Touchdown', 'Punt Return Touchdown', 'Blocked FG, Downs', 'Interception Touchdown', 'Blocked Punt', 'Fumble Return Touchdown', 'Blocked Punt Touchdown'])
        splash_drives = drives.groupby(group_cols)['splash_allowed_%'].mean().reset_index()
        splash_drives['splash_allowed_%'] *= 100

        merged_table = pd.merge(pivot_table, scoring_drives, on=group_cols)
        merged_table_two = pd.merge(merged_table, turnover_drives)
        final_table = pd.merge(merged_table_two, splash_drives)

        final_table = final_table.rename(columns={'Blocked Punt': 'blocked_punt_%',
                                                  'Blocked Punt Touchdown': 'blocked_punt_touchdown_%',
                                                  'Downs': 'turnover_on_downs_%',
                                                  'Field Goal': 'field_goal_%',
                                                  'Fumble': 'fumble_%',
                                                  'Fumble Touchdown': 'fumble_touchdown_%',
                                                  'Interception': 'interception_%',
                                                  'Interception Touchdown': 'interception_touchdown_%',
                                                  'Missed FG': 'missed_fg_%',
                                                  'Punt': 'punt_%',
                                                  'Punt Return Touchdown': 'punt_return_touchdown_%',
                                                  'Safety': 'safety_%',
                                                  'Touchdown': 'touchdown_%'})

        return final_table

    def defensive_drive_summary(self, year=None, week=None, season_type=None, team=None, agg_method=None):

        drives = self.drives(year=year, week=week, season_type=season_type, team=None)

        # Determine whether to aggregate weekly or yearly
        if agg_method == 'weekly':
            # Assume you have a 'date' or 'week' column in the dataframe
            group_cols = ['opponent', 'date']  # Using 'week' as an example, replace with 'date' if needed
        else:
            group_cols = ['opponent']

        result_count = drives.groupby(group_cols + ['display_result']).size().reset_index(name='count')
        total_drives = drives.groupby(group_cols).size().reset_index(name='total_drives')
        result_percent = pd.merge(result_count, total_drives, on=group_cols)
        result_percent['percentage'] = (result_percent['count'] / result_percent['total_drives']) * 100
        pivot_table = result_percent.pivot_table(index=group_cols, columns='display_result', values='percentage',
                                                 fill_value=0)

        drives['is_scoring'] = drives['display_result'].isin(['Touchdown', 'Field Goal'])
        scoring_drives = drives.groupby(group_cols)['is_scoring'].mean().reset_index(name='%_scoring_drives')
        scoring_drives['%_scoring_drives'] *= 100  # Convert to percentage

        drives['is_turnover'] = drives['display_result'].isin(['Fumble', 'Fumble Return Touchdown', 'Fumble Touchdown', 'Interception', 'Interception Touchdown'])
        turnover_drives = drives.groupby(group_cols)['is_turnover'].mean().reset_index(name='%_turnover')
        turnover_drives['%_turnover'] *= 100  # Convert to percentage

        drives['splash_forced_%'] = drives['display_result'].isin(['Fumble', 'Interception', 'Blocked FG Touchdown', 'Safety', 'Fumble Touchdown', 'Punt Return Touchdown', 'Blocked FG, Downs', 'Interception Touchdown', 'Blocked Punt', 'Fumble Return Touchdown', 'Blocked Punt Touchdown'])
        splash_drives = drives.groupby(group_cols)['splash_forced_%'].mean().reset_index()
        splash_drives['splash_forced_%'] *= 100

        merged_table = pd.merge(pivot_table, scoring_drives, on=group_cols)
        merged_table_two = pd.merge(merged_table, turnover_drives)
        final_table = pd.merge(merged_table_two, splash_drives)

        final_table = final_table.rename(columns={'opponent': 'defense',
                                                  'Blocked Punt': 'blocked_punt_%',
                                                  'Blocked Punt Touchdown': 'blocked_punt_touchdown_%',
                                                  'Downs': 'turnover_on_downs_%',
                                                  'Field Goal': 'field_goal_prevention_%',
                                                  'Fumble': 'forced_fumble_%',
                                                  'Fumble Touchdown': 'fumble_return_touchdown_%',
                                                  'Interception': 'interception_%',
                                                  'Interception Touchdown': 'interception_touchdown_%',
                                                  'Missed FG': 'missed_fg_%',
                                                  'Punt': 'forced_punt_%',
                                                  'Punt Return Touchdown': 'punt_return_touchdown_%',
                                                  'Safety': 'forced_safety %',
                                                  'Touchdown': 'touchdown_prevention_%',
                                                  '%_scoring_drives': 'point_prevention_%',
                                                  '%_turnover': 'forced_turnover_%'})

        final_table = final_table.drop(columns = ['End of Game', 'End of Half'])
        final_table['field_goal_prevention_%'] = (1 - (final_table['field_goal_prevention_%'] / 100)) * 100
        final_table['touchdown_prevention_%'] = (1 - (final_table['touchdown_prevention_%'] / 100)) * 100
        final_table['point_prevention_%'] = (1 - (final_table['point_prevention_%'] / 100)) * 100

        return final_table

    def plays(self, year, week=None, season_type=None, team=None, pivot=None):

        play_dict = {'team': [], 'play_id': [], 'play_type_id': [], 'play_type': [], 'away_score': [], 'home_score': [],
                     'clock': [], 'start_distance': [], 'start_down': [], 'start_yardline': [], 'start_yards_to_endzone': [],
                     'end_distance': [], 'end_down': [], 'end_yardline': [], 'end_yards_to_endzone': [], 'modified': [], 'period': [],
                     'priority': [], 'scoring_play': [], 'start_yardage': [], 'text': [], 'wall_clock': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):
            completed_games = completed_games[completed_games.id == id]

            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()

                for x in data['drives']['previous']:
                    team = x['team']['displayName']
                    for play in x['plays']:
                        play_id = play['id']
                        play_type_id = play.get('type', {}).get('id', 'NA')
                        play_type = play.get('type', {}).get('text', 'NA')
                        away_score = play['awayScore']
                        home_score = play['homeScore']
                        clock = play['clock']['displayValue']
                        start_distance = play['start']['distance']
                        start_down = play['start']['down']
                        start_yardline = play['start']['yardLine']
                        start_yards_to_endzone = play['start']['yardsToEndzone']
                        end_distance = play['end']['distance']
                        end_down = play['end']['down']
                        end_yardline = play['end']['yardLine']
                        end_yards_to_endzone = play['end']['yardsToEndzone']
                        modified = play['modified']
                        period = play['period']
                        priority = play['priority']
                        scoring_play = play['scoringPlay']
                        start_yardage = play['statYardage']
                        text = play['text']
                        wall_clock = play.get('wallclock', 'NA')
                        play_dict['team'].append(team)
                        play_dict['play_id'].append(play_id)
                        play_dict['play_type_id'].append(play_type_id)
                        play_dict['play_type'].append(play_type)
                        play_dict['away_score'].append(away_score)
                        play_dict['home_score'].append(home_score)
                        play_dict['clock'].append(clock)
                        play_dict['start_distance'].append(start_distance)
                        play_dict['start_down'].append(start_down)
                        play_dict['start_yardline'].append(start_yardline)
                        play_dict['start_yards_to_endzone'].append(start_yards_to_endzone)
                        play_dict['end_distance'].append(end_distance)
                        play_dict['end_down'].append(end_down)
                        play_dict['end_yardline'].append(end_yardline)
                        play_dict['end_yards_to_endzone'].append(end_yards_to_endzone)
                        play_dict['modified'].append(modified)
                        play_dict['period'].append(period)
                        play_dict['priority'].append(priority)
                        play_dict['scoring_play'].append(scoring_play)
                        play_dict['start_yardage'].append(start_yardage)
                        play_dict['text'].append(text)
                        play_dict['wall_clock'].append(wall_clock)

        plays_df = pd.DataFrame(play_dict)

        return plays_df

    def videos(self, year, week=None, season_type=None, team=None):

        video_dict = {'id': [], 'video_id':[], 'date':[], 'home_team':[], 'away_team':[], 'headline': [], 'description': [], 'link': [],
                      'duration': [], 'publish_date': [],'source': [], 'thumbnail': [], 'coverage_type': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):
            completed_games = completed_games[completed_games.id == id]

            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                for x in data['videos']:
                    description = x['description']
                    duration = x['duration']
                    headline = x['headline']
                    video_id = x['id']
                    link = x['links']['source']['mezzanine']['href']
                    publish_date = x['originalPublishDate']
                    source = x['source']
                    thumbnail = x['thumbnail']
                    coverage_type = x['tracking']['coverageType']
                    video_dict['id'].append(id)
                    video_dict['video_id'].append(video_id)
                    video_dict['date'].append(date)
                    video_dict['home_team'].append(home_team)
                    video_dict['away_team'].append(away_team)
                    video_dict['headline'].append(headline)
                    video_dict['description'].append(description)
                    video_dict['link'].append(link)
                    video_dict['duration'].append(duration)
                    video_dict['publish_date'].append(publish_date)
                    video_dict['source'].append(source)
                    video_dict['thumbnail'].append(thumbnail)
                    video_dict['coverage_type'].append(coverage_type)

        video_df = pd.DataFrame(video_dict)
        return video_df

    def images(self, year, week=None, season_type=None, team=None):
        pass

    def officials(self, year, week=None, season_type=None, team=None):

        ### retrieves game officials

        officiating_dict = {'id': [], 'date': [], 'home_team': [], 'away_team': [], 'name': [], 'position': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):
            completed_games = completed_games[completed_games.id == id]

            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                for x in data['gameInfo']['officials']:
                    name = x['displayName']
                    position = x['position']['name']
                    officiating_dict['id'].append(id)
                    officiating_dict['date'].append(date)
                    officiating_dict['home_team'].append(home_team)
                    officiating_dict['away_team'].append(away_team)
                    officiating_dict['name'].append(name)
                    officiating_dict['position'].append(position)

        officiating_df = pd.DataFrame(officiating_dict)
        return officiating_df

    def officiating(self, year, week=None, season_type=None, team=None):

        officials = self.officials(year=year, week=week, season_type=season_type, team=team)
        boxscore = self.boxscore(year=year, week=week, season_type=season_type, team=team)

        ### retrieves officiating statistics by refereee, by team, spinoff of boxscore + officials methods (marrying the two)

        pass

    def win_probability(self, year, week=None, season_type=None, team=None):

        w_prob_dict = {'id': [], 'date': [], 'home_team': [], 'away_team': [], 'home_win_%': [], 'play_id': []}

        completed_games = self.completed_games(year=year, week=week, season_type=season_type, team=team)

        for id, date, city, venue, home_team, away_team, home_score, away_score in tqdm(
                zip(completed_games['id'].unique(),
                    completed_games['date'],
                    completed_games['city'],
                    completed_games['venue'],
                    completed_games['home_team'],
                    completed_games['away_team'],
                    completed_games['home_score'],
                    completed_games['away_score']),
                total=completed_games.shape[0]):

            completed_games = completed_games[completed_games.id == id]

            url = f'https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={id}'

            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                for x in data['winprobability']:
                    home_win_percentage = x['homeWinPercentage']
                    play_id = x['playId']
                    w_prob_dict['id'].append(id)
                    w_prob_dict['date'].append(date)
                    w_prob_dict['home_team'].append(home_team)
                    w_prob_dict['away_team'].append(away_team)
                    w_prob_dict['home_win_%'].append(home_win_percentage)
                    w_prob_dict['play_id'].append(play_id)

        w_prob_df = pd.DataFrame(w_prob_dict)
        w_prob_df['away_win_%'] = 1 - w_prob_df['home_win_%']
        w_prob_df['play_number'] = w_prob_df.groupby('date').cumcount() + 1

        plays = self.plays(year=year, week=week, season_type=season_type, team=team)
        plays_probability = w_prob_df.merge(plays, on='play_id')

        def assign_points_and_opponent(row):
            if row['team'] == row['home_team']:
                row['team_win_%'] = row['home_win_%']
                row['opponent_win_%'] = row['away_win_%']
            else:
                row['team_win_%'] = row['away_win_%']
                row['opponent_win_%'] = row['home_win_%']
            return row

        plays_probability = plays_probability.apply(assign_points_and_opponent, axis=1)

        return plays_probability

    def game_news(self, year, week=None, season_type=None, team=None):
        pass

    def teams(self):
        pass

    def players(self):
        pass
    def qb_epa_glossary(self):

        glossary_dict = {'name': [], 'description': []}

        base_url = f'https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2024/types/2/weeks/1/qbr/10000'

        response = requests.get(base_url)
        if response.status_code == 200:
            data = response.json()
            for i in data['items'][0]['splits']['categories'][0]['stats']:
                name = i['displayName']
                description = i['description']
                glossary_dict['name'].append(name)
                glossary_dict['description'].append(description)

        glossary_df = pd.DataFrame(glossary_dict)

        return glossary_df

    def qb_epa(self, year=None, season_type=None, week=None, pivot=None):

        athlete_dict = {'name': [], 'athlete_id': []}
        pb = self.passing_boxscore(year=year, season_type=season_type, week=week)
        for name, id in zip(pb.name.unique(), pb.athlete_id.unique()):
            athlete_dict['name'].append(name)
            athlete_dict['athlete_id'].append(id)

        athlete_df = pd.DataFrame(athlete_dict)

        epa_dict = {'athlete_id': [], 'id': [], 'points_added': [], 'pass_epa': [], 'rush_epa': [], 'sack_epa': [],
                    'penalty_epa': [], 'total_epa': [], 'qb_plays': [], 'cwa': [], 'raw_qbr': [],
                    'expected_pass_epa': [],
                    'int_epa': [], 'yac_epa': [], 'run_epa': [], 'scramble_epa': [], 'expected_sack_epa': [],
                    'fumble_epa': [],
                    'defense_faced_epa': [], 'total_qbr': [], 'unq_rank': []}

        if not season_type and not week:
            season_type = 2
            weeks = list(range(1, 19)) if year > 2020 else list(range(1, 18))
        elif week and not season_type:
            weeks = str(week)
            season_type = 2
        elif season_type == 1:
            weeks = list(range(1, 5))
        elif season_type == 2:
            weeks = list(range(1, 19)) if year > 2020 else list(range(1, 18))
        elif season_type == 3:
            weeks = list(range(1, 6))
        else:
            season_type = season_type
            weeks = week

        for w in weeks:

            base_url = f'https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/{year}/types/{season_type}/weeks/{w}/qbr/10000'

            def extract_id(url):
                match = re.search(r'/(\d+)(?:\?|$)', url)
                if match:
                    return match.group(1)
                return None

            page_index = 1
            while True:
                url = f"{base_url}?page={page_index}"
                print(url)
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()

                    for x in data['items']:
                        athlete_url = x['athlete']['$ref']
                        event_url = x['event']['$ref']
                        athlete_id = extract_id(athlete_url)
                        id = extract_id(event_url)
                        points_added = x['splits']['categories'][0]['stats'][0]['displayValue']
                        pass_epa = x['splits']['categories'][0]['stats'][1]['displayValue']
                        rush_epa = x['splits']['categories'][0]['stats'][2]['displayValue']
                        sack_epa = x['splits']['categories'][0]['stats'][3]['displayValue']
                        penalty_epa = x['splits']['categories'][0]['stats'][4]['displayValue']
                        total_epa = x['splits']['categories'][0]['stats'][5]['displayValue']
                        qb_plays = x['splits']['categories'][0]['stats'][6]['displayValue']
                        cwa = x['splits']['categories'][0]['stats'][7]['displayValue']
                        raw_qbr = x['splits']['categories'][0]['stats'][8]['displayValue']
                        expected_pass_epa = x['splits']['categories'][0]['stats'][9]['displayValue']
                        int_epa = x['splits']['categories'][0]['stats'][10]['displayValue']
                        yac_epa = x['splits']['categories'][0]['stats'][11]['displayValue']
                        run_epa = x['splits']['categories'][0]['stats'][12]['displayValue']
                        scramble_epa = x['splits']['categories'][0]['stats'][13]['displayValue']
                        expected_sack_epa = x['splits']['categories'][0]['stats'][14]['displayValue']
                        fumble_epa = x['splits']['categories'][0]['stats'][15]['displayValue']
                        defense_faced_epa = x['splits']['categories'][0]['stats'][16]['displayValue']
                        total_qbr = x['splits']['categories'][0]['stats'][17]['displayValue']
                        unq_rank = x['splits']['categories'][0]['stats'][18]['displayValue']

                        epa_dict['athlete_id'].append(athlete_id)
                        epa_dict['id'].append(id)
                        epa_dict['points_added'].append(points_added)
                        epa_dict['pass_epa'].append(pass_epa)
                        epa_dict['rush_epa'].append(rush_epa)
                        epa_dict['sack_epa'].append(sack_epa)
                        epa_dict['penalty_epa'].append(penalty_epa)
                        epa_dict['total_epa'].append(total_epa)
                        epa_dict['qb_plays'].append(qb_plays)
                        epa_dict['cwa'].append(cwa)
                        epa_dict['raw_qbr'].append(raw_qbr)
                        epa_dict['expected_pass_epa'].append(expected_pass_epa)
                        epa_dict['int_epa'].append(int_epa)
                        epa_dict['yac_epa'].append(yac_epa)
                        epa_dict['run_epa'].append(run_epa)
                        epa_dict['scramble_epa'].append(scramble_epa)
                        epa_dict['expected_sack_epa'].append(expected_sack_epa)
                        epa_dict['fumble_epa'].append(fumble_epa)
                        epa_dict['defense_faced_epa'].append(defense_faced_epa)
                        epa_dict['total_qbr'].append(total_qbr)
                        epa_dict['unq_rank'].append(unq_rank)

                    if page_index >= data.get('pageCount', 1):
                        break
                    page_index += 1

                else:
                    print(f"Failed to fetch data from {url}")
                    break

        epa_df = pd.DataFrame(epa_dict)
        total_df = epa_df.merge(athlete_df, on='athlete_id')


        columns_to_float = ['points_added', 'pass_epa', 'rush_epa', 'sack_epa', 'penalty_epa', 'total_epa', 'qb_plays',
                            'cwa', 'raw_qbr', 'expected_pass_epa', 'int_epa', 'yac_epa', 'run_epa', 'scramble_epa',
                            'expected_sack_epa', 'fumble_epa', 'defense_faced_epa', 'total_qbr', 'unq_rank']

        total_df[columns_to_float] = total_df[columns_to_float].astype(float)

        epa_pivot = total_df.groupby('name').agg({'points_added': ['mean', 'sum'],
                                                  'pass_epa': ['mean', 'sum'],
                                                  'rush_epa': ['mean', 'sum'],
                                                  'sack_epa': ['mean', 'sum'],
                                                  'penalty_epa': ['mean', 'sum'],
                                                  'total_epa': ['mean', 'sum'],
                                                  'qb_plays': ['mean', 'sum'],
                                                  'cwa': ['mean', 'sum'],
                                                  'raw_qbr': ['mean', 'sum'],
                                                  'expected_pass_epa': ['mean', 'sum'],
                                                  'int_epa': ['mean', 'sum'],
                                                  'yac_epa': ['mean', 'sum'],
                                                  'run_epa': ['mean', 'sum'],
                                                  'scramble_epa': ['mean', 'sum'],
                                                  'expected_sack_epa': ['mean', 'sum'],
                                                  'fumble_epa': ['mean', 'sum'],
                                                  'defense_faced_epa': ['mean', 'sum'],
                                                  'total_qbr': ['mean', 'sum'],
                                                  'unq_rank':  ['mean', 'sum']}).reset_index()

        epa_pivot.columns = ['_'.join(col).strip() for col in epa_pivot.columns.values]


        if pivot == True:
            return epa_pivot
        else:
            return total_df

    def pd_mov_mod(self, year=None, week=None, season_type=None, team=None):

        boxscore = self.team_boxscore(year, week=None, season_type=None, team=None)

        stat_dict = {'team': [], 'total_point_differential':[], 'avg_point_differential': [], 'avg_margin_of_victory': [], 'avg_margin_of_defeat': []}

        for team in boxscore.team.unique():
            team_df = boxscore[boxscore['team'] == team]
            wins_df = team_df[team_df['result'] == 'W']
            loss_df = team_df[team_df['result'] == 'L']
            total_point_differential = team_df['point_differential'].sum()
            avg_point_differential = team_df['point_differential'].mean()
            avg_margin_of_victory = wins_df['point_differential'].mean()
            avg_margin_of_defeat = loss_df['point_differential'].mean()
            stat_dict['team'].append(team)
            stat_dict['total_point_differential'].append(total_point_differential)
            stat_dict['avg_point_differential'].append(avg_point_differential)
            stat_dict['avg_margin_of_victory'].append(avg_margin_of_victory)
            stat_dict['avg_margin_of_defeat'].append(avg_margin_of_defeat)

        stat_df = pd.DataFrame(stat_dict)

        return stat_df

    def drive_analysis(self, year=None, agg_method=None):

        if agg_method:

            offensive_drives = self.offensive_drive_summary(year=year, agg_method=agg_method)
            defensive_drives = self.defensive_drive_summary(year=year, agg_method=agg_method)
            offensive_drives['date'] = pd.to_datetime(offensive_drives['date'])
            defensive_drives['date'] = pd.to_datetime(defensive_drives['date'])
            offensive_drives = offensive_drives.sort_values(by = ['team','date'], ascending=True)
            defensive_drives = defensive_drives.sort_values(by= ['defense', 'date'], ascending=True)

            od = offensive_drives[['team', 'date', 'punt_%', 'touchdown_%', 'point_%', 'turnover_%', 'splash_allowed_%']]
            dd = defensive_drives[['defense', 'date', 'forced_punt_%', 'touchdown_prevention_%', 'point_prevention_%', 'forced_turnover_%', 'splash_forced_%']]

            dd = dd.rename(columns={'defense': 'team'})
            total_drive_analysis = pd.merge(od, dd, on=['team', 'date'])
            total_drive_analysis[total_drive_analysis.select_dtypes(include='number').columns] = total_drive_analysis.select_dtypes(include='number') / 100

            total_drive_analysis['offensive_performance_metric'] = total_drive_analysis['touchdown_%'] + total_drive_analysis['point_%'] - total_drive_analysis['punt_%'] - total_drive_analysis['splash_allowed_%']
            total_drive_analysis['defensive_performance_metric'] = total_drive_analysis['forced_punt_%'] + total_drive_analysis['touchdown_prevention_%'] + total_drive_analysis['point_prevention_%'] + total_drive_analysis['splash_forced_%']
            total_drive_analysis['total_performance_metric'] = total_drive_analysis['offensive_performance_metric'] + total_drive_analysis['defensive_performance_metric']

        else:


            offensive_drives = self.offensive_drive_summary(year = year, agg_method=agg_method)
            defensive_drives = self.defensive_drive_summary(year = year, agg_method=agg_method)

            od = offensive_drives[['team', 'punt_%', 'touchdown_%', 'point_%', 'turnover_%', 'splash_allowed_%']]
            dd = defensive_drives[['defense', 'forced_punt_%', 'touchdown_prevention_%', 'point_prevention_%', 'forced_turnover_%','splash_forced_%']]


            dd = dd.rename(columns={'defense':'team'})

            total_drive_analysis = pd.merge(od, dd, on='team')
            total_drive_analysis[total_drive_analysis.select_dtypes(include='number').columns] = total_drive_analysis.select_dtypes(include='number') / 100

            total_drive_analysis['offensive_performance_metric'] = total_drive_analysis['touchdown_%'] + total_drive_analysis['point_%'] - total_drive_analysis['punt_%'] - total_drive_analysis['splash_allowed_%']
            total_drive_analysis['defensive_performance_metric'] = total_drive_analysis['forced_punt_%'] + total_drive_analysis['touchdown_prevention_%'] + total_drive_analysis['point_prevention_%'] + total_drive_analysis['splash_forced_%']
            total_drive_analysis['total_performance_metric'] = total_drive_analysis['offensive_performance_metric'] + total_drive_analysis['defensive_performance_metric']


        return total_drive_analysis

    def head_to_head(self, teams = None):

        pass
        # team boxscore pivot offense
        # team boxscore pivot defense
        # drive analysis
        # pd_mov_mod














