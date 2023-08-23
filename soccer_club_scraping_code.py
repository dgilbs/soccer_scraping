import pandas as pd
import numpy as np
import yaml
import os
import time
import shutil
import re
import seaborn as sns
import psycopg2
import hashlib
import creds
from datetime import datetime, date
from collections import defaultdict
from itertools import product


def generate_unique_id(values):
    hash_object = hashlib.md5(','.join(map(str, values)).encode())
    unique_id = hash_object.hexdigest()
    return unique_id

def upsert_data_into_db(df, schema, table_name, primary_key_column='id'):
    db_password = os.environ.get("DATABASE_PASSWORD", creds.db_password)
    db_config = {
    'host': 'localhost',
    'database': 'projects',
    'user': 'dgilberg',
    'password': db_password,
    'port': '5432'
    }

    # Establish a connection to the database
    connection = psycopg2.connect(
        host=db_config['host'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']
    )

    cursor = connection.cursor()
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    upsert_query = f"""
    INSERT INTO {schema}.{table_name} ({columns})
    VALUES ({placeholders})
    ON CONFLICT ({primary_key_column}) DO UPDATE
    SET {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != primary_key_column])};
    """
    data_values = [tuple(row) for _, row in df.iterrows()]

    # Execute the upsert query
    cursor.executemany(upsert_query, data_values)

    # Commit the transaction
    connection.commit()

def get_table_columns(schema_name, table_name):

    db_password = os.environ.get("DATABASE_PASSWORD", creds.db_password)
    db_config = {
    'host': 'localhost',
    'database': 'projects',
    'user': 'dgilberg',
    'password': db_password,
    'port': '5432'
    }

    # Establish a connection to the database
    connection = psycopg2.connect(
        host=db_config['host'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']
    )

    try:
        # Create a cursor
        cursor = connection.cursor()


        # Get column names using the information schema
        query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
            """
        cursor.execute(query, (schema_name, table_name))
        # Fetch all the column names
        column_names = [row[0] for row in cursor.fetchall()]

        return column_names
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor.close()
        connection.close()

def db_connect():
    db_password = os.environ.get("DATABASE_PASSWORD", creds.db_password)
    db_config = {
    'host': 'localhost',
    'database': 'projects',
    'user': 'dgilberg',
    'password': db_password,
    'port': '5432'
    }

    # Establish a connection to the database
    connection = psycopg2.connect(
        host=db_config['host'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']
    )
    return connection

def upsert_multiple_files_to_db(file_path, schema, table, primary_key_column, key_term=None):
    files = all_files_in_subdirectories(file_path, key_term=key_term)
    df = pd.concat([pd.read_pickle(i) for i in files], ignore_index=True)
    table_cols = get_table_columns(schema, table)
    df = df[table_cols]
    df = df.replace('', 0)
    upsert_data_into_db(df, schema, table, primary_key_column)

def retrieve_table(schema_name, table_name, limit=None):
    cols = get_table_columns(schema_name, table_name)
    query = 'select * from {}.{}'.format(schema_name, table_name)
    if limit:
        query += ' limit {};'.format(limit)
    else:
        query += ';'
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=cols)
    conn.close()
    return df

def generate_id(row, columns):
    """
        generates ID for a given row

        Args:
            row(pd.Series): row from dataframe

        Returns:
            id(hash_object): unique id generated for that row

    """
    hash_object = hashlib.md5()
    hash_object.update(str(row[columns]).encode('utf-8'))
    return hash_object.hexdigest()


def all_files_in_subdirectories(dir_path, key_term=None):
    """
    a quick an easy way to list the full path of all files in subdirectories

    Args:
        dir_path(str): relative path you are looking at

    returns:
        arr(list): list of all full relative paths in that folder
    """
    arr = list()
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            arr.append(os.path.join(root, file))
    if key_term:
        arr = [i for i in arr if key_term in i]
    return arr


def build_dataframe_from_subdirectory(dir_path, key_term=None):
    files = all_files_in_subdirectories(dir_path, key_term=key_term)
    df = pd.concat([pd.read_pickle(i) for i in files], ignore_index=True)
    return df

def extract_squad_tag(url):
    """
        extracts web tag for a squad to help find webpages (i.e. England-Women for England)

        Args:
            url(str): string of url you are scraping

        Returns:
            tag(str): the web tag
    """
    try:
        last_elem = url.split('/')[-1]
        return last_elem.split('-Stats')[0]
    except:
        return None

def scrape_standings(info_dict, season, current_season=True):
    """
        Scrapes data for standings of the World Cup

        Args:
            url(str): url of standings page

        Returns:
            final(df) df of total standings across all groups
    """
    competition_id = info_dict['league_id']
    league_table = info_dict['league_table_tag']
    if not info_dict['multi_year']:
        season_str = season
    else:
        prev = int(season) - 1
        season_str = '{}-{}'.format(prev, season)
    url = 'https://fbref.com/en/comps/{}/{}/{}-NWSL-Stats'.format(competition_id, season_str, season_str, league_table)
    attrs = {'id': 'results{}{}1_overall'.format(season_str, competition_id)}
    df = pd.read_html(url, attrs=attrs, extract_links='body')[0]
    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    df['squad_link'] = df.apply(lambda row: row['squad'][1], axis=1)
    df['squad'] = df.apply(lambda row: row['squad'][0], axis=1)
    df['squad_tag'] = df.apply(lambda row: extract_squad_tag(row['squad_link']), axis=1)
    if current_season:
        df['squad_id'] = df.apply(lambda row: row['squad_link'].split('/')[-2], axis=1)
    else:
        df['squad_id'] = df.apply(lambda row: row['squad_link'].split('/')[-3], axis=1)
    df['squad'] = df.apply(lambda row: row['squad'].strip(), axis=1)
    remaining_cols = [i for i in df.columns if 'squad' not in i]
    for col in remaining_cols:
        df[col] = df.apply(lambda row: row[col][0], axis=1)
    df['league'] = info_dict['name']

    df['season'] = season_str
    dir_path = 'data/{}/league_standings'.format(info_dict['folder'])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path ='{}_standings.pkl'.format(str(season))
    full_path = os.path.join(dir_path, file_path)

    df.to_pickle(full_path)
    return df

def scrape_schedule_from_competition(info_dict, season, config, current_year=True):
    #https://fbref.com/en/comps/182/2022/schedule/2022-NWSL-Scores-and-Fixtures
    competition_id = info_dict['league_id']
    schedule_tag = info_dict['schedule_tag']
    if not info_dict['multi_year']:
        season_str = season
    else:
        prev = int(season) - 1
        season_str = '{}-{}'.format(prev, season)
    url = 'https://fbref.com/en/comps/{}/{}/schedule/{}-{}'.format(competition_id, season_str, season_str, schedule_tag)
#     if not current_year:
#         attrs = {'id': 'sched_all'}
#     else:
#         attrs = {'id': 'sched_{}_{}_1'.format(season_str, competition_id)}

    try:
        attrs = {'id': 'sched_all'}
        df = pd.read_html(url, attrs=attrs, extract_links='body')[0]
    except ValueError:
        attrs = {'id': 'sched_{}_{}_1'.format(season_str, competition_id)}
        df = pd.read_html(url, attrs=attrs, extract_links='body')[0]
    except Exception as e:
        print(e)
        return False
    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    df = df.rename(columns=config['schedule_rename_columns'])
    link_cols = config['schedule_link_columns']
    non_link_cols = [i for i in df.columns if i not in link_cols]
    for i in link_cols:
        new_col = i + '_link'
        df[new_col] = df.apply(lambda row: row[i][1], axis=1)
        df[i] = df.apply(lambda row: row[i][0], axis=1)
    for j in non_link_cols:
        df[j] = df.apply(lambda row: row[j][0], axis=1)

    df = df[(df.day_of_week != 'Day') & (df.match_report == 'Match Report') & (df.score.str.contains('–'))]
    df['attendance'] = df.attendance.str.replace(',', '')
    df['home_team_id'] = df.apply(lambda row: row['home_team_link'].split('/')[3], axis=1)
    df['away_team_id'] = df.apply(lambda row: row['away_team_link'].split('/')[3], axis=1)
    df['id'] = df.apply(lambda row: row['match_report_link'].split('/')[-2], axis=1)
    df['competition_id'] = competition_id
    df['home_goals'] = df.apply(lambda row: row['score'].split('–')[0], axis=1)
    df['away_goals'] = df.apply(lambda row: row['score'].split('–')[1], axis=1)
    df['season'] = season_str
    dir_path = 'data/{}/schedules'.format(info_dict['folder'])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    file_name = '{}_schedule.pkl'.format(season)
    full_path = os.path.join(dir_path, file_name)
    df = df.reset_index(drop=True)
    df.to_pickle(full_path)
    df = df.replace('', None)
    tc = get_table_columns('soccer', 'schedules')
    missing_cols = [i for i in tc if i not in df.columns]
    for i in missing_cols:
        df[i] = None
    idf = df[tc]
    upsert_data_into_db(idf, 'soccer', 'schedules')
    return df

def scrape_match_report_from_competition_schedule(row, info_dict, category, config, fact_tables=False):
    """
        scrapes a full match report (both teams) from the competition schedule

        Args:
            row(pd.series): row of the schedule dataframe
            category: which category of data you're pulling, consult fbref for the available ones for the competition
            config(dict): values of the config file

        Returns:
            final(DataFrame): full match report

    """
    if 'fbref' in row['match_report_link']:
        url = row['match_report_link']
    else:
        url = 'https://fbref.com' + row['match_report_link']
    home_table_id = 'stats_{}_{}'.format( row['home_team_id'], category.lower())
    home_df = pd.read_html(url, attrs={'id': home_table_id}, extract_links='body')[0]
    home_df.columns = [i[0].lower().replace(' ', '_') + '_'+ i[1].lower().replace(' ', '_') if 'Unnamed' not in i[0] else i[1].lower().replace(' ', '_') for i in home_df.columns ]
    home_df['player_link'] = home_df.apply(lambda row: row['player'][1], axis=1)
    home_df['player'] = home_df.apply(lambda row: row['player'][0], axis=1)
    cols = [i for i in home_df.columns if i not in ['player', 'player_link']]
    home_df = home_df[pd.notnull(home_df['#'])]
    for i in cols:
        home_df[i] = home_df.apply(lambda row: row[i][0], axis=1)
    home_df['match_id'] = row['id']
    home_df['squad'] = row['home_team']
    home_df['squad_id'] = row['home_team_id']
    home_df['opponent'] = row['away_team']
    home_df['opponent_id'] = row['away_team_id']

    away_table_id = 'stats_{}_{}'.format( row['away_team_id'], category.lower())
    away_df = pd.read_html(url, attrs={'id': away_table_id}, extract_links='body')[0]
    away_df.columns = [i[0].lower().replace(' ', '_') + '_'+ i[1].lower().replace(' ', '_') if 'Unnamed' not in i[0] else i[1].lower().replace(' ', '_') for i in away_df.columns ]
    away_df['player_link'] = away_df.apply(lambda row: row['player'][1], axis=1)
    away_df['player'] = away_df.apply(lambda row: row['player'][0], axis=1)
    cols = [i for i in away_df.columns if i not in ['player', 'player_link']]
    away_df = away_df[pd.notnull(away_df['#'])]
    for i in cols:
        away_df[i] = away_df.apply(lambda row: row[i][0], axis=1)
    away_df['match_id'] = row['id']
    away_df['squad'] = row['away_team']
    away_df['squad_id'] = row['away_team_id']
    away_df['opponent'] = row['home_team']
    away_df['opponent_id'] = row['home_team_id']

    file_dir = 'data/{}/match_reports/{}'.format(info_dict['folder'], category)
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    file_name = '{}_{}.pkl'.format(row['id'], category.lower())
    full_path = os.path.join(file_dir, file_name)
    final = pd.concat([home_df, away_df], ignore_index=True)
    final = final.rename(columns=config['match_report_{}_rename_columns'.format(category)])
    final['player_id'] = final.apply(lambda row: row['player_link'].split('/')[-2], axis=1)
    final['id'] = final.apply(lambda row: generate_unique_id([row['player'], row['match_id']]), axis=1)
    final['gender'] = info_dict['gender']
    final.to_pickle(full_path)
    schema = config['match_report_upsert_config']['schema']
    table = config['match_report_upsert_config']['table'].format(category.lower().replace(' ', '_'))
    table_cols = get_table_columns(schema, table)
    missing_cols = [i for i in table_cols if i not in final.columns]
    for col in missing_cols:
        final[col] = None
    final = final.replace('', None)
    insert_df = final[table_cols]
    if fact_tables:
        update_fact_tables(final, config, info_dict)
    upsert_data_into_db(insert_df, schema, table)

def scrape_match_report_all_categories(row, info_dict, config, advanced=True):
    if advanced:
        categories = config['advanced_match_report_categories']
    else:
        categories = config['basic_match_report_categories']
    try:
        scrape_match_report_from_competition_schedule(row, info_dict, categories[0], config, fact_tables=True)
    except Exception as e:
        print(e, 'summary')
    time.sleep(6)
    for cat in categories[1:]:
        try:
            scrape_match_report_from_competition_schedule(row, info_dict, cat, config)
        except Exception as e:
            print(e, cat)
        time.sleep(6)

    try:
        scrape_shot_creation_match_data(row, info_dict)
    except Exception as e:
        print(e, 'shot data')

def update_fact_tables(df, config, info_dict):
    upsert_info = config['fact_table_upsert_config']
    tables = list (upsert_info.keys())
    for i in tables:
        temp = df.copy()
        schema = upsert_info[i]['table_schema']
        table = upsert_info[i]['table_name']
        df_cols = upsert_info[i]['match_report_columns']
        deduped_df = temp.drop_duplicates(subset=df_cols)[df_cols]
        deduped_df.columns = get_table_columns(schema, table)
        upsert_data_into_db(deduped_df, schema, table)


def scrape_shot_creation_match_data(row, info):
    if 'fbref' in row['match_report_link']:
        url = row['match_report_link']
    else:
        url = 'https://fbref.com' + row['match_report_link']
    match_id = row['id']
    attrs = {'id': 'shots_all'}
    df = pd.read_html(url, attrs=attrs, extract_links='body')[0]
    df.columns = [i[1].lower() if 'Unnamed' in i[0] else i[0].lower().replace(' ', '_') + '_'+  i[1].lower() for i in df.columns]
    link_cols = ['player', 'squad', 'sca_1_player', 'sca_2_player']
    non_link_cols = [i for i in df.columns if i not in link_cols]

    for col in link_cols:
        new_column = col + '_link'
        df[new_column] = df.apply(lambda row: row[col][1], axis=1)
        df[col] = df.apply(lambda row: row[col][0], axis=1)

    for col in non_link_cols:
        df[col] = df.apply(lambda row: row[col][0], axis=1)
    df['match_id'] = match_id
    dir_path = 'data/{}/shot_creation'.format(info['folder'])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_name = '{}_shot_creation.pkl'.format(match_id)
    full_path = os.path.join(dir_path, file_name)
    df = df[df.squad != '']
    df = clean_shot_creation_df(df, config)
    df.to_pickle(full_path)
    return df

def clean_shot_creation_df(df, config):
    df = df[df.minute != '']

    df['stoppage_minute'] = df.apply(lambda row: row['minute'].split('+')[1] if len(row['minute'].split('+')) > 1 else None, axis=1)

    df['minute'] = df.apply(lambda row: row['minute'].split('+')[0], axis=1)
    df['psxg'] = df.psxg.replace('', np.nan)

    df['on_target'] = df.psxg.isnull()

    df['is_free_kick'] = df.notes.str.contains('Free kick')
    df['is_deflected'] = df.notes.str.contains('Deflected')
    df['is_volley'] = df.notes.str.contains('Volley')

    df = df.rename(columns=config['shot_creation_rename_columns'])
    id_cols = ['shot_player_link', 'squad_link', 'sca_1_player_link', 'sca_2_player_link']
    for col in id_cols:
        new_col = col.replace('_link', '_id')
        df[new_col] = df.apply(lambda row: row[col].split('/')[-2] if row[col] is not None else None, axis=1)
    df = df.reset_index()
    id_cols = ['shot_player', 'sca_1_player', 'sca_2_player']
    for i in id_cols:
        df = df.rename(columns={i: 'player'})
        new_col = i.replace('player', 'player_match_id')
        df[new_col] = df.apply(lambda row: generate_unique_id([row['player'], row['match_id']]), axis=1)
        df = df.rename(columns={'player': i})
    df['shot_id'] = df.apply(lambda row: generate_id(row, ['index', 'shot_player_id', 'match_id']), axis=1)
#     df['shot_player_match_id'] = df.apply(lambda row: generate_id(row, ['shot_player', 'match_id']), axis=1)
#     df['sca_1_player_match_id'] = df.apply(lambda row: generate_id(row, ['sca_1_player', 'match_id']), axis=1)
#     df['sca_2_player_match_id'] = df.apply(lambda row: generate_id(row, ['sca_2_player', 'match_id']), axis=1)
    df['sca_1_player_match_id'] = df.apply(lambda row: row['sca_1_player_match_id'] if ['sca_1_player_id'] is not None else None, axis=1)
    df['sca_2_player_match_id'] = df.apply(lambda row: row['sca_2_player_match_id'] if ['sca_2_player_id'] is not None else None, axis=1)
    return df

def extract_shot_creation_data_from_df(df):
    rows = list()
    for i in df.iterrows():
        row = i[1]
        if row['sca_1_player_id'] is None and row['sca_2_player_id'] is None:
            continue
        elif row['sca_1_player_id'] is not None and row['sca_2_player_id'] is None:
            sca_id = generate_id(row, ['sca_1_player_match_id', 'sca_1_event', 'index'])
            shot_id = row['shot_id']
            pid = row['sca_1_player_match_id']
            event = row['sca_1_event']
            squad = row['squad_id']
            order = 1
            minute = row['minute']
            stoppage = row['stoppage_minute']
            shooter = row['shot_player_match_id']
            outcome = row['outcome']
            temp_row = [sca_id, shot_id, pid, event, squad, order, minute, stoppage, shooter, outcome]
            rows.append(temp_row)
        elif row['sca_1_player_id'] is not None and row['sca_2_player_id'] is not None:
            sca_id = generate_id(row, ['sca_1_player_match_id', 'sca_1_event', 'index'])
            shot_id = row['shot_id']
            pid = row['sca_1_player_match_id']
            event = row['sca_1_event']
            squad = row['squad_id']
            order = 1
            minute = row['minute']
            stoppage = row['stoppage_minute']
            shooter = row['shot_player_match_id']
            outcome = row['outcome']
            temp_row = [sca_id, shot_id, pid, event, squad, order, minute, stoppage, shooter, outcome]
            rows.append(temp_row)
            pid = row['sca_2_player_match_id']
            event = row['sca_2_event']
            order = 2
            sca_id = generate_id(row, ['sca_2_player_match_id', 'sca_2_event', 'index'])
            temp_row = [sca_id, shot_id, pid, event, squad, order, minute, stoppage, shooter, outcome]
            rows.append(temp_row)

    sca = pd.DataFrame(rows, columns=['sca_id', 'shot_id', 'player_match_id', 'sca_event', 'squad_id', 'event_order', 'minute', 'stoppage_minute', 'shooter', 'outcome'])

    sca['sca_event'] = sca['sca_event'].str.replace(r'\(|\)', '')

    dummies = pd.get_dummies(sca['sca_event']).astype(bool)

    dummies.columns = ['sca_' + i.lower().replace(' ', '_').replace('-', '_') for i in dummies.columns]

    sca = pd.concat([sca, dummies], axis=1)
    return sca
