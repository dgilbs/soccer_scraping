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
    """
    generates an id based on certain values

    Args:
        values(list): list of the values to input into the id

    Returns:
        unique_id(str): returns hashed string id for the values

    """
    hash_object = hashlib.md5(','.join(map(str, values)).encode())
    unique_id = hash_object.hexdigest()
    return unique_id

def upsert_data_into_db(df, schema, table_name, primary_key_column='id'):
    """
    Will insert new data into a database and update where the id is already present

    Args:
        df(DataFrame): data being inserted
        schema(str): database schema
        table_name(str): database table
        primary_key_colum(str): name of table's primary key

    """
    #connect to DB
    db_password = os.environ.get("DATABASE_PASSWORD", creds.db_password)
    db_config = {
    'host': 'localhost',
    'database': 'projects',
    'user': 'dgilberg',
    'password': db_password,
    'port': '5432'
    }

    connection = psycopg2.connect(
        host=db_config['host'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']
    )
    #create cursor
    cursor = connection.cursor()
    #create substring for query
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    #write out upsert query
    upsert_query = f"""
    INSERT INTO {schema}.{table_name} ({columns})
    VALUES ({placeholders})
    ON CONFLICT ({primary_key_column}) DO UPDATE
    SET {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col != primary_key_column])};
    """

    #create values tuple
    data_values = [tuple(row) for _, row in df.iterrows()]

    # Execute the upsert query
    cursor.executemany(upsert_query, data_values)

    # Commit the transaction
    connection.commit()

def get_table_columns(schema_name, table_name):
    """
    returns the columns of a given table

    Args:
        schema(str): database schema
        table_name(str): database table
    returns:
        column_names(list): list of columns in the table

    """
    #connect to DB
    db_password = os.environ.get("DATABASE_PASSWORD", creds.db_password)
    db_config = {
    'host': 'localhost',
    'database': 'projects',
    'user': 'dgilberg',
    'password': db_password,
    'port': '5432'
    }

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


        # extract column names from information schema
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
    """
    creates a connection to the database for ad hoc queries and purposes

    Args:
        None
    returns
        Connection (psycopg2.connect): connection to database

    """
    db_password = os.environ.get("DATABASE_PASSWORD", creds.db_password)
    db_config = {
    'host': 'localhost',
    'database': 'projects',
    'user': 'dgilberg',
    'password': db_password,
    'port': '5432'
    }

    connection = psycopg2.connect(
        host=db_config['host'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        port=db_config['port']
    )
    return connection

def upsert_multiple_files_to_db(file_path, schema, table, primary_key_column='id', key_term=None):
    """
    Compiles multiple files and inserts them all into the database

    Args:
        file_path(str): path of collective files
        schema(str): database schema
        table(str): database table
        primary_key_column(str): table's primary key column
        key_term(str): key term present in file names to be inserted
    """
    #make a list of all files
    files = all_files_in_subdirectories(file_path, key_term=key_term)
    #concat all files
    df = pd.concat([pd.read_pickle(i) for i in files], ignore_index=True)
    #get the table columns and apply that to the dataframe
    table_cols = get_table_columns(schema, table)
    df = df[table_cols]
    #replace blank cells and upsert
    df = df.replace('', 0)
    upsert_data_into_db(df, schema, table, primary_key_column)

def retrieve_table(schema_name, table_name, limit=None):
    """
    Retrieves a table from the database

    Args:
        schema_name(str): database schema
        table_name(str): database table
        limit(int): limit on rows, if needed


    """
    #get column names
    cols = get_table_columns(schema_name, table_name)
    #run query
    query = 'select * from {}.{}'.format(schema_name, table_name)
    #if a limit is requested then apply that
    if limit:
        query += ' limit {};'.format(limit)
    else:
        query += ';'
    #connect to db
    conn = db_connect()
    cursor = conn.cursor()
    cursor.execute(query)
    #create dataframe
    df = pd.DataFrame(cursor.fetchall(), columns=cols)
    conn.close()
    return df

def cast_dtypes(df, datatypes):
    """
    Casts datatypes to columns in a database

    Args:
        df(DataFrame): DataFrame to assign dtypes to
        dtypes(dict): dict of columns and their corresponding datatypes

    Returns:
        new_df(DataFrame): dataframe with reset datatypes
    """
    new_df = df.copy()
    arr = list(datatypes.keys())
    for i in arr:
        if i in df.columns:
            new_df[i] = new_df[i].fillna(np.nan)
            new_df[i] = new_df[i].replace('', np.nan)
#             if df[i].dtype == 'object':
#                 print(i)
#                 new_df[i] = new_df[i].str.replace(',', '')
            new_df[i] = new_df[i].astype(datatypes[i])
    return new_df

def all_files_in_subdirectories(dir_path, key_term=None):
    """
    a quick an easy way to list the full path of all files in subdirectories

    Args:
        dir_path(str): relative path you are looking at

    returns:
        arr(list): list of all full relative paths in that folder
    """
    #initalize a list
    arr = list()
    #walk through entire file path and append full relative path of each
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            arr.append(os.path.join(root, file))
    #filters for a key term
    if key_term:
        arr = [i for i in arr if key_term in i]
    return arr


def build_dataframe_from_subdirectory(dir_path, key_term=None):
    """
    Takes files in a given file path and builds a dataframe

    Args:
        dir_path(str): relative path to folder
        key_term(str): any key terms in file names

    Returns:
        df(DataFrame): data in folder
    """
    #gets all the files
    files = all_files_in_subdirectories(dir_path, key_term=key_term)
    #concats taht list into a dataframe
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
    #returns the tag for a team's page
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
    #grab the competition id and tag from info
    competition_id = info_dict['league_id']
    league_table = info_dict['league_table_tag']
    #checks if a single year needs to be expanded to a multi-year league
    if not info_dict['multi_year']:
        season_str = season
    elif info_dict['multi_year'] and current_season and date.today().month < 8:
        prev = int(season) - 1
        season_str = '{}-{}'.format(prev, season)
    elif info_dict['multi_year'] and current_season and date.today().month > 8:
        next_year = int(season) + 1
        season_str = '{}-{}'.format(season, next_year)
    else:
        prev = int(season) - 1
        season_str = '{}-{}'.format(prev, season)
    #build out url and table id and read into DataFrame and do initial cleaning
    url = 'https://fbref.com/en/comps/{}/{}/{}-{}'.format(competition_id, season_str, season_str, league_table)
    attrs = {'id': 'results{}{}1_overall'.format(season_str, competition_id)}
    df = pd.read_html(url, attrs=attrs, extract_links='body')[0]
    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    df['squad_link'] = df.apply(lambda row: row['squad'][1], axis=1)
    df['squad'] = df.apply(lambda row: row['squad'][0], axis=1)
    df['squad_tag'] = df.apply(lambda row: extract_squad_tag(row['squad_link']), axis=1)
    df['squad_id'] = df.apply(lambda row: row['squad_link'].split('/')[3], axis=1)
    df['squad'] = df.apply(lambda row: row['squad'].strip(), axis=1)
    remaining_cols = [i for i in df.columns if 'squad' not in i]
    for col in remaining_cols:
        df[col] = df.apply(lambda row: row[col][0], axis=1)
    df['league'] = info_dict['name']

    df['season'] = season_str
    #saves it to proper directory
    dir_path = 'data/{}/league_standings'.format(info_dict['folder'])
    #creates folder for this if it does not exist.
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path ='{}_standings.pkl'.format(str(season))
    full_path = os.path.join(dir_path, file_path)

    df.to_pickle(full_path)
    return df

def scrape_schedule_from_competition(info_dict, season, config):
    """
    Scrapes a competition's schedule page
    Example: https://fbref.com/en/comps/182/schedule/NWSL-Scores-and-Fixtures

    Args:
        info_dict(dict): league information
        season(str): season
        config(dict): config file

    """
    #get info/tag/year info
    competition_id = info_dict['league_id']
    schedule_tag = info_dict['schedule_tag']
    if not info_dict['multi_year']:
        season_str = season
    elif info_dict['multi_year'] and current_season and date.today().month < 8:
        prev = int(season) - 1
        season_str = '{}-{}'.format(prev, season)
    elif info_dict['multi_year'] and current_season and date.today().month > 8:
        next_year = int(season) + 1
        season_str = '{}-{}'.format(season, next_year)
    else:
        prev = int(season) - 1
        season_str = '{}-{}'.format(prev, season)
    url = 'https://fbref.com/en/comps/{}/{}/schedule/{}-{}'.format(competition_id, season_str, season_str, schedule_tag)

    #try to extract the table with the schedule in it
    try:
        attrs = {'id': 'sched_all'}
        df = pd.read_html(url, attrs=attrs, extract_links='body')[0]
    except ValueError:
        attrs = {'id': 'sched_{}_{}_1'.format(season_str, competition_id)}
        df = pd.read_html(url, attrs=attrs, extract_links='body')[0]
    except Exception as e:
        print(e)
        return False
    #some basic cleaning--column renames and also extracting links provided by fbref
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

    #create new columns based on other values and info values
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

    #save to file path and upsert
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
        scrapes a match report (both teams) from the competition schedule for a given category

        Args:
            row(pd.series): row of the schedule dataframe
            category: which category of data you're pulling, consult fbref for the available ones for the competition
            config(dict): values of the config file

        Returns:
            final(DataFrame): full match report

    """
    #clean the link and make sure it works
    if 'fbref' in row['match_report_link']:
        url = row['match_report_link']
    else:
        url = 'https://fbref.com' + row['match_report_link']

    #creates table id
    if category == 'keeper':
        home_table_id = 'keeper_stats_{}'.format( row['home_team_id'])
        away_table_id = 'keeper_stats_{}'.format( row['away_team_id'])
    else:
        home_table_id = 'stats_{}_{}'.format( row['home_team_id'], category.lower())
        away_table_id = 'stats_{}_{}'.format( row['away_team_id'], category.lower())

    #reads in data for the home team, cleans up column names and links, adds new values
    home_df = pd.read_html(url, attrs={'id': home_table_id}, extract_links='body')[0]
    home_df.columns = [i[0].lower().replace(' ', '_') + '_'+ i[1].lower().replace(' ', '_') if 'Unnamed' not in i[0] else i[1].lower().replace(' ', '_') for i in home_df.columns ]
    home_df['player_link'] = home_df.apply(lambda row: row['player'][1], axis=1)
    home_df['player'] = home_df.apply(lambda row: row['player'][0], axis=1)
    cols = [i for i in home_df.columns if i not in ['player', 'player_link']]
    if category != 'keeper':
        home_df = home_df[pd.notnull(home_df['#'])]
    for i in cols:
        home_df[i] = home_df.apply(lambda row: row[i][0], axis=1)
    home_df['match_id'] = row['id']
    home_df['squad'] = row['home_team']
    home_df['squad_id'] = row['home_team_id']
    home_df['opponent'] = row['away_team']
    home_df['opponent_id'] = row['away_team_id']

    #reads in data for the away team, cleans up column names and links, adds new values
    away_df = pd.read_html(url, attrs={'id': away_table_id}, extract_links='body')[0]
    away_df.columns = [i[0].lower().replace(' ', '_') + '_'+ i[1].lower().replace(' ', '_') if 'Unnamed' not in i[0] else i[1].lower().replace(' ', '_') for i in away_df.columns ]
    away_df['player_link'] = away_df.apply(lambda row: row['player'][1], axis=1)
    away_df['player'] = away_df.apply(lambda row: row['player'][0], axis=1)
    cols = [i for i in away_df.columns if i not in ['player', 'player_link']]
    if category != 'keeper':
        away_df = away_df[pd.notnull(away_df['#'])]
    for i in cols:
        away_df[i] = away_df.apply(lambda row: row[i][0], axis=1)
    away_df['match_id'] = row['id']
    away_df['squad'] = row['away_team']
    away_df['squad_id'] = row['away_team_id']
    away_df['opponent'] = row['home_team']
    away_df['opponent_id'] = row['home_team_id']

    #creates directory to save the file in and also concats the two DFs and renames the columns
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

    #condenses dataframe to only the table columns, checks for missing columns, and upserts it
    schema = config['match_report_upsert_config']['schema']
    table = config['match_report_upsert_config']['table'].format(category.lower().replace(' ', '_'))
    table_cols = get_table_columns(schema, table)
    missing_cols = [i for i in table_cols if i not in final.columns]
    for col in missing_cols:
        final[col] = None
    final = final.replace('', None)
    insert_df = final[table_cols]
    #updates fact tables if requested
    if fact_tables:
        update_fact_tables(final, config, info_dict)
    upsert_data_into_db(insert_df, schema, table)

def scrape_match_report_all_categories(row, info_dict, config, advanced=True):
    """
    Scrapes a match report in all categories and uploads the data

    Args:
        row(pd.Series): row in a schedule DataFrame
        info_dict(dict): league info
        config: config file
        advanced(bool): signals whether advanced metrics are available for that match

    returns:
        None

    """
    #pulls list of metrics based on whether or not the game is advanced
    if advanced:
        categories = config['advanced_match_report_categories']
    else:
        categories = config['basic_match_report_categories']

    #start with the summary and update the fact tables
    try:
        scrape_match_report_from_competition_schedule(row, info_dict, categories[0], config, fact_tables=True)
    except Exception as e:
        print(e, 'summary')
    #use time.sleep to prevent hitting the rate limit
    time.sleep(6)
    #iterate through categories and scrape the match reports for those
    for cat in categories[1:]:
        try:
            scrape_match_report_from_competition_schedule(row, info_dict, cat, config)
        except Exception as e:
            print(e, cat)
        time.sleep(6)

    #scrape shot data
    try:
        scrape_shot_creation_match_data(row, info_dict)
    except Exception as e:
        print(e, 'shot data')


def update_fact_tables(df, config, info_dict):
    """
    Updates the fact tables in the Database based on a match report

    Args:
        df(DataFrame): Match Report
        config(dict): config file, used here to signal what columns are meant for which tables
        info_dict(dict): league info
    """
    #get info from config fil;e
    upsert_info = config['fact_table_upsert_config']
    tables = list (upsert_info.keys())
    #go through dataframe and make sure the proper fact tables are updated
    for i in tables:
        temp = df.copy()
        schema = upsert_info[i]['table_schema']
        table = upsert_info[i]['table_name']
        df_cols = upsert_info[i]['match_report_columns']
        deduped_df = temp.drop_duplicates(subset=df_cols)[df_cols]
        deduped_df.columns = get_table_columns(schema, table)
        upsert_data_into_db(deduped_df, schema, table)



def scrape_shot_creation_match_data(row, info):
    """
    Scrapes the shot data for a given match

    Args:
        row(pd.Series): DataFrame row from a schedule df
        info(dict): league info

    Returns:
        df(DataFrame): DataFrame with shot data

    """
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
    """
    cleans a shot creation DataFrame

    args:
        df(DataFrame): Shot data DataFrame
        config(dict): config file

    returns:
        df(DataFrame): cleaned DataFrame
    """
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
    df['shot_id'] = df.apply(lambda row: generate_unique_id([row['index'], row['shot_player_id'], row['match_id']]), axis=1)
#     df['shot_player_match_id'] = df.apply(lambda row: generate_id(row, ['shot_player', 'match_id']), axis=1)
#     df['sca_1_player_match_id'] = df.apply(lambda row: generate_id(row, ['sca_1_player', 'match_id']), axis=1)
#     df['sca_2_player_match_id'] = df.apply(lambda row: generate_id(row, ['sca_2_player', 'match_id']), axis=1)
    df['sca_1_player_match_id'] = df.apply(lambda row: row['sca_1_player_match_id'] if ['sca_1_player_id'] is not None else None, axis=1)
    df['sca_2_player_match_id'] = df.apply(lambda row: row['sca_2_player_match_id'] if ['sca_2_player_id'] is not None else None, axis=1)
    return df

def extract_shot_creation_data_from_df(df):
    """
    Makes a seperate DataFrame of shot creating actions from shot data

    Args:
        df(DataFrame): cleaned shot creation DataFrame

    Returns:
        sca(DataFrame): dataframe of shot creating actions
    """
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

def scrape_multiple_match_reports_from_schedule(df, info_dict, config, advanced=True):
    """
    Scrapes mutliple match reports from a schedule DataFrame

    df(DataFrame): DataFrame of schedule info
    info_dict(dict): league info
    config(dict): config file

    Returns:
        None

    """
    print('scraping {} rows'.format(len(df)))
    for i in df.iterrows():
        row = i[1]
        print(i[0], row['match_report_link'])
        scrape_match_report_all_categories(row, info_dict, config)

    print('done!')


def classify_xg_difference(xg_for, xg_against):
    """
    classifies a level of domninance for a team's performance in a match based on xg

    Args:
        xg_for(str/float): the team's xg for that match
        xg_against(str/float): the opponent's xg

    returns:
        diff(str): string representing the category of difference betrween the numbers
    """
    try:
        xg_for = float(xg_for)
        xg_against = float(xg_against)
        if xg_for - xg_against <= -.5:
            diff = 'Against'
        elif xg_for - xg_against >= .5:
            diff = 'For'
        else:
            diff = 'Neutral'
        return diff
    except:
        return None

def scrape_team_season_results(row, config, info):
    """
    scrape a schedule from a team page on fbref.com

    Args:
        row(pd.Series): row of standings dataframe that has squad info
        config(dict): config file
        info(dict): league info

    Returns:
        df(DataFrame): dataframe of a team's schedule (completed matches only) for a given season
    """
    squad_id = row['squad_id']
    tag = row['squad_tag']
    season = row['season']
    squad = row['squad']

    url = 'https://fbref.com/en/squads/{}/{}/matchlogs/all_comps/schedule/{}-Scores-and-Fixtures-All-Competitions'.format(squad_id, season, tag)
    df = pd.read_html(url, extract_links='body')[0]

    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    link_cols = ['comp', 'opponent', 'match_report', 'captain']
    keep_cols = [i for i in df.columns if i not in link_cols]

    for i in link_cols:
        new_col = i + '_link'
        df[new_col] = df.apply(lambda row: row[i][1], axis=1)
        df[i] = df.apply(lambda row: row[i][0], axis=1)

    for j in keep_cols:
        df[j] = df.apply(lambda row: row[j][0], axis=1)

    df = df[df.match_report == 'Match Report']
    df['competition_id'] = df.apply(lambda row: row['comp_link'].split('/')[3], axis=1)
    df['opponent_id'] = df.apply(lambda row: row['opponent_link'].split('/')[3], axis=1)
    df['match_id'] = df.apply(lambda row: row['match_report_link'].split('/')[3], axis=1)
    df['captain_id'] = df.apply(lambda row: row['captain_link'].split('/')[3] if row['captain_link'] is not None else None, axis=1)
    df.insert(0, 'squad_id', squad_id)
    df.insert(0, 'squad', squad)
    df['id'] = df.apply(lambda row: generate_unique_id([row['date'], row['squad_id'], row['match_id']]), axis=1)
    df = df.rename(columns=config['team_schedule_rename_columns'])
    df['goals_for'] = df.apply(lambda row: row['goals_for'].split('(')[0].strip(), axis=1)
    df['goals_against'] = df.apply(lambda row: row['goals_against'].split('(')[0].strip(), axis=1)
    df['attendance'] = df.attendance.str.replace(',', '')
    df = df.replace('', None)
    dir_path = 'data/{}/team_results'.format(info['folder'])
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_name = '{}_{}_results.pkl'.format(squad_id, season)
    full_path = os.path.join(dir_path, file_name)
    df['clean_sheet_for'] = df.goals_against.astype(int) == 0
    df['clean_sheet_against'] = df.goals_for.astype(int) == 0
    df['higher_xg'] = df.xg_for > df.xg_against
    df['run_of_play'] = df.apply(lambda row: classify_xg_difference(row['xg_for'], row['xg_against']), axis=1)
    df.to_pickle(full_path)
    cols = get_table_columns('soccer', 'team_results')
    missing_cols = [i for i in cols if i not in df.columns]
    idf = df[cols]
    upsert_data_into_db(idf, 'soccer', 'team_results')
    return df

def update_current_league_data(info_dict, config, start_date=None, end_date=None):
    """
    Runs a full update of a league for a given time range, defaults to the last 7 days

    Args:
        info_dict(dict): league info
        config(dict): config file
        start_date: beginning of date range queried/updated, will default to None and then reassigned to
                    7 days ago
        end_date: beginning of date range queried/updated, will default to None and then reassigned to
                  today

    Returns:
        None
    """
    if not start_date:
        start_date = date.today() - timedelta(7)

    if not end_date:
        end_date = date.today()
    year = date.today().year

    standings = scrape_standings(info_dict, year)
    league_schedule = scrape_schedule_from_competition(info_dict, year, config)

    league_schedule['match_date'] = pd.to_datetime(league_schedule.match_date).dt.date
    mask = (league_schedule.match_date >= start_date) & (league_schedule.match_date <= end_date)

    scrape_matches = league_schedule[mask].reset_index(drop=True)

    scrape_multiple_match_reports_from_schedule(scrape_matches, info_dict, config)
