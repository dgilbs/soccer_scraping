import pandas as pd
import requests
import re
import os
import yaml
import time
import hashlib
import numpy as np
import shutil
from bs4 import BeautifulSoup
from collections import defaultdict
from datetime import datetime, date, timedelta
from urllib.request import Request, urlopen


def all_files_in_subdirectories(dir_path, key_terms=[]):
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
    if len(key_terms)>0:
        arr = [i for i in arr if all(j in i for j in key_terms)]
    return arr

def scrape_standings(url):
    """
        Scrapes data for standings of the World Cup

        Args:
            url(str): url of standings page

        Returns:
            final(df) df of total standings across all groups
    """
    arr = pd.read_html(url, extract_links='all')
    for index, df in enumerate(arr):
        df.columns = [i[0].lower().replace(' ', '_') for i in df.columns]
        df['squad_link'] = df.apply(lambda row: 'https://fbref.com' + row['squad'][1], axis=1)
        for col in df.columns:
            if col != 'squad_link':
                df[col] = df.apply(lambda row: row[col][0], axis=1)
        df['group'] = chr(ord('A') + index)
        df['squad_abbreviation'] = df.apply(lambda row: row['squad'].split(' ')[0], axis=1)
        df['squad'] = df.apply(lambda row: row['squad'][3:], axis=1)
    final = pd.concat(arr, ignore_index=True)
    final['squad_tag'] = final.apply(lambda row: extract_squad_tag(row['squad_link']), axis=1)
    final['squad_id'] = final.apply(lambda row: row['squad_link'].split('/')[-2], axis=1)
    final['squad'] = final.apply(lambda row: row['squad'].strip(), axis=1)
    final.to_pickle('data/womens_world_cup/standings.pkl')
    return final

def extract_squad_tag(url):
    """
        extracts web tag for a squad to help find webpages (i.e. England-Women for England)

        Args:
            url(str): string of url you are scraping

        Returns:
            tag(str): the web tag
    """
    last_elem = url.split('/')[-1]
    return last_elem.split('-Stats')[0]

def extract_links_from_page(url):
    """
        extracts all links from a page

        Args:
            url(str): url you are extracting from

        Returns:
            final_links(list): all links on that page

    """
    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all anchor tags
    links = soup.find_all('a')

    # Extract the href attribute from each anchor tag
    extracted_links = []
    for link in links:
        href = link.get('href')
        if href:
            extracted_links.append(href)

    final_links = ['https://fbref.com' + i for i in extracted_links]
    return final_links

def row_link_check(row, column):
    """
        checks to see if a link has been extracted in the scraping, used on a specific row in a dataframe

        Args:
            row: the dataframe row
            column: which column you're checking

        Returns:
            link(str): creates link for row if it's there, None if it's not

    """
    if row[column][1] == None:
        return None
    else:
        return 'https://fbref.com' + row[column][1]

def extract_match_id(url):
    """
        extracts match id from URL

        Args:
            url(str): url you are extracting from

        Returns:
            id(str): match id

    """
    try:
        arr = url.split('/')
        return arr[-2]
    except:
        return None

def scrape_team_schedule(team_tag, team_id, year, squad, config):
    """
        scrapes a team's schedule from their page

        Args:
            team_tag(str): their web tag
            team_id(str): team id on fbref.com
            year(str): what year we're querying for
            squad(str): team name
            config(dict): config file dictionary that has link columns
        Returns:
            df(DataFrame): DataFrame with data on team's schedule

    """
    url = 'https://fbref.com/en/squads/{}/{}/matchlogs/all_comps/schedule/{}-Scores-and-Fixtures-All-Competitions'.format(team_id, str(year), team_tag)
    params = {'id': 'matchlogs_for'}
    df = pd.read_html(url, attrs=params, extract_links='body')[0]
    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    df = df.rename(columns=config['schedule_rename_columns'])
    for col in df.columns:
        if col in config['schedule_link_columns']:
            new_col = col + '_link'
            df[new_col] = df.apply(lambda row: row_link_check(row, col), axis=1)
            df[col] = df.apply(lambda row: row[col][0], axis=1)
        else:
            df[col] = df.apply(lambda row: row[col][0], axis=1)

    df['opponent'] = df.apply(lambda row: row['opponent'][3:], axis=1)
    df = df[df.result != '']
    df.insert(0, 'squad', squad)
    schedule_dir = 'data/womens_world_cup/team_schedules/{}'.format(squad.lower().replace(' ', '_'))

    if not os.path.exists(schedule_dir):
        os.makedirs(schedule_dir)

    file_name = '{}_{}_results.pkl'.format(squad, str(year))
    file_path = os.path.join(schedule_dir, file_name)
    df['squad_id'] = team_id
    df['match_id'] = df.apply(lambda row: extract_match_id(row['match_report_link']), axis=1)
    df.to_pickle(file_path)
    return df

def scrape_team_match_report_from_schedule(row, config):
    """
        extracts a match report for a team from a row in their schedule DataFrame

        Args:
            row(pd.series): a row in a schedule DataFrame
            config(dict): dict from configuration file

        Returns:
            df(DataFrame):

    """
    params = {'id': 'stats_{}_summary'.format(row['squad_id'])}
    df = pd.read_html(row['match_report_link'], attrs=params, skiprows=0, extract_links='body')[0]
    df.columns = [i[0].lower().replace(' ', '_') + '_'+ i[1].lower().replace(' ', '_') if 'Unnamed' not in i[0] else i[1].lower().replace(' ', '_') for i in df.columns ]
    df = df[pd.notnull(df['#'])]
    standard_cols = [i for i in df.columns if i != 'player']
    for i in standard_cols:
        df[i] = df.apply(lambda row: row[i][0], axis=1)
    df['player_link'] = df.apply(lambda row: row['player'][1], axis=1)
    df['player'] = df.apply(lambda row: row['player'][0], axis=1)

    team = row['squad'].lower().replace(' ', '')
    opp = row['opponent'].lower().replace(' ', '')
    md_str = str(row['match_date']).replace('-', '')
    match_report_dir = 'data/womens_world_cup/team_match_reports/{}/'.format(team)
    if not os.path.exists(match_report_dir):
        os.makedirs(match_report_dir)

    file_name = '{}_{}_{}_vs_{}_match_summary.pkl'.format(team, row['match_id'], md_str, opp)
    full_path = os.path.join(match_report_dir, file_name)
    df = clean_single_team_match_report(df, config, row)
    df.to_pickle(full_path)
    return df

def cast_dtypes(df, datatypes):
    """
        converts dtypes in a dataframe

        Args:
            df(DataFrame): dataframe you need to change the values of
            datatypes(dict): columns and what datatypes you want from them

        Returns:
            new_df(DataFrame): dataframe that has right datatypes

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

def clean_single_team_match_report(df, config, row):
    """
        cleans a match report--gives better column names and casts right datatypes

        Args:
            df(DataFrame): DataFrame scraped from the site
            config(dict): dict from configuration file, must have rename columns and datatypes
            row: row from schedule being used to scrape and build the dataframe

        Returns:
            df(DataFrame): cleaned dataframe

    """
    df = df.rename(columns=config['match_report_rename_columns'])
    df = cast_dtypes(df, config['match_report_dtypes'])
    df['match_id'] = row['match_id']
    df['squad'] = row['squad']
    df['opponent'] = row['opponent']
    df['competition'] = row['competition']
    df['round'] = row['round']
    df['match_date'] = row['match_date']
    df['player_id'] = df.apply(lambda row: row['player_link'].split('/')[-2], axis=1)
    return df

def generate_id(row):
    """
        generates ID for a given row

        Args:
            row(pd.Series): row from dataframe

        Returns:
            id(hash_object): unique id generated for that row

    """
    hash_object = hashlib.md5()
    hash_object.update(str(row).encode('utf-8'))
    return hash_object.hexdigest()

def scrape_roster(row):
    """
        scrape the World Cup roster which is available on the team page at time of this project

        Args:
            row(pd.series): row from standings that links to a team's page

        Returns:
            df(DataFrame): team's world cup roster

    """
    url = row['squad_link']
    squad = row['squad'].lower().replace(' ', '_').replace('.', '').strip()
    attrs = {'id': 'roster'}
    df = pd.read_html(url, attrs=attrs, extract_links='body')[0]
    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    standard_cols = [i for i in df.columns if i != 'player']
    for i in standard_cols:
        df[i] = df.apply(lambda row: row[i][0], axis=1)

    df['player_link'] = df.apply(lambda row: row['player'][1], axis=1)
    df['player'] = df.apply(lambda row: row['player'][0], axis=1)
    df['squad'] = row['squad']
    df = clean_roster(df, config)
    roster_dir = 'data/womens_world_cup/team_rosters'
    if not os.path.exists(roster_dir):
        os.makedirs(roster_dir)
    file_name = '{}_roster.pkl'.format(squad)
    full_path = os.path.join(roster_dir, file_name)
    df.to_pickle(full_path)
    return df

def extract_club_name_and_country_from_roster(club_name):
    """
        extracts the club name and country from the way it's formatted on the site

        Args:
            club name: name of club as listed on FBRef

        Returns:
            names(list): returns a list with the country and club name cleaned

    """
    first_arr = club_name.split('.')
    if len(first_arr) > 1:
        combo = first_arr[1]
    else:
        combo = first_arr[0]
    arr = combo.split(' ')
    country = arr[0].upper()
    club = ' '.join(arr[1:])
    return [country, club]

def clean_roster(df, config):
    """
        cleans roster--changes column names and datatypes and more

        Args:
            df(DataFrame): roster DataFrame

        Returns:
            df(DataFrame): cleaned roster dataframe

    """
    df['club_name'] = df.apply(lambda row: extract_club_name_and_country_from_roster(row['club'])[1], axis=1)
    df['club_country'] = df.apply(lambda row: extract_club_name_and_country_from_roster(row['club'])[0], axis=1)
    df['age_years'] = df.apply(lambda row: row['age'].split('-')[0], axis=1)
    df['birth_date'] = pd.to_datetime(df.birth_date).dt.date
    df['exact_age'] = df.apply(lambda row: ((date.today() - row['birth_date']).days)/365.25 if type(row['birth_date']) != pd._libs.tslibs.nattype.NaTType else None , axis=1)
    df['player_id'] = df.apply(lambda row: row['player_link'].split('/')[-2], axis=1)
    df = df.drop(config['roster_drop_columns'], axis=1)
    df = df.rename(columns=config['roster_rename_columns'])
    df['age'] = df.age.astype('Int64')
    return df

def scrape_competition_schedule(url, year, competition_id, config):
    """
        scrapes the competition schedule from the master schedule

        Args:
            url(str): url of the master schedule pag
            year(str): year of competition
            config(dict): values in config file

        Returns:
            df(DataFrame): cleaned roster dataframe
    """
    attrs = {'id': 'sched_{}_{}_1'.format(year, competition_id)}
    df = pd.read_html(url, attrs=attrs, extract_links='body')[0]
    df.columns = [i.lower().replace(' ', '_') for i in df.columns]
    link_cols = ['home', 'away', 'match_report', 'date']
    standard_cols = [i for i in df.columns if i not in link_cols]
    for i in standard_cols:
        df[i] = df.apply(lambda row: row[i][0], axis=1)

    for j in link_cols:
        new_col = j + "_link"
        df[new_col] = df.apply(lambda row: row[j][1], axis=1)
        df[j] = df.apply(lambda row: row[j][0], axis=1)
    df = df[df.match_report == 'Match Report']
    df = clean_competition_schedule(df, config)
    return df

def clean_competition_schedule(df, config):
    """
        cleans the competition schedule and renames columns and re-assigns datatypes

        Args:
            df(DataFrame): data to be cleaned
            config(dict): values from config file

        Returns:
            df(DataFrame): cleaned df

    """
    df = df.rename(columns=config['competition_rename_columns'])
    df['home_team'] = df.apply(lambda row: " ".join(row['home_team'].split(' ')[:-1]), axis=1)
    df['away_team'] = df.apply(lambda row: " ".join(row['away_team'].split(' ')[1:]), axis=1)
    df['home_goals'] = df.apply(lambda row: row['score'].split('–')[0], axis=1)
    df['away_goals'] = df.apply(lambda row: row['score'].split('–')[1], axis=1)
    link_columns = [i for i in df.columns if '_link' in i]
    for col in link_columns:
        df[col] = df.apply(lambda row: 'https://fbref.com' + row[col], axis=1)
    df['attendance'] = df.apply(lambda row: row['attendance'].replace(',', ''), axis=1)
    df['match_date'] = pd.to_datetime(df.match_date).dt.date
    df['home_team_id'] = df.apply(lambda row: row['home_team_link'].split('/')[-2], axis=1)
    df['away_team_id'] = df.apply(lambda row: row['away_team_link'].split('/')[-2], axis=1)
    df['match_id'] = df.apply(lambda row: row['match_report_link'].split('/')[-2], axis=1)
    return df

def scrape_match_report_from_competition_schedule(row, category, config):
    """
        scrapes a full match report (both teams) from the competition schedule

        Args:
            row(pd.series): row of the schedule dataframe
            category: which category of data you're pulling, consult fbref for the available ones for the competition
            config(dict): values of the config file

        Returns:
            final(DataFrame): full match report

    """
    url = row['match_report_link']
    home_table_id = 'stats_{}_{}'.format( row['home_team_id'], category.lower())
    home_df = pd.read_html(url, attrs={'id': home_table_id}, extract_links='body')[0]
    home_df.columns = [i[0].lower().replace(' ', '_') + '_'+ i[1].lower().replace(' ', '_') if 'Unnamed' not in i[0] else i[1].lower().replace(' ', '_') for i in home_df.columns ]
    home_df['player_link'] = home_df.apply(lambda row: row['player'][1], axis=1)
    home_df['player'] = home_df.apply(lambda row: row['player'][0], axis=1)
    cols = [i for i in home_df.columns if i not in ['player', 'player_link']]
    home_df = home_df[pd.notnull(home_df['#'])]
    for i in cols:
        home_df[i] = home_df.apply(lambda row: row[i][0], axis=1)
    home_df['match_id'] = row['match_id']
    home_df['squad'] = row['home_team']
    home_df['opponent'] = row['away_team']

    away_table_id = 'stats_{}_{}'.format( row['away_team_id'], category.lower())
    away_df = pd.read_html(url, attrs={'id': away_table_id}, extract_links='body')[0]
    away_df.columns = [i[0].lower().replace(' ', '_') + '_'+ i[1].lower().replace(' ', '_') if 'Unnamed' not in i[0] else i[1].lower().replace(' ', '_') for i in away_df.columns ]
    away_df['player_link'] = away_df.apply(lambda row: row['player'][1], axis=1)
    away_df['player'] = away_df.apply(lambda row: row['player'][0], axis=1)
    cols = [i for i in away_df.columns if i not in ['player', 'player_link']]
    away_df = away_df[pd.notnull(away_df['#'])]
    for i in cols:
        away_df[i] = away_df.apply(lambda row: row[i][0], axis=1)
    away_df['match_id'] = row['match_id']
    away_df['squad'] = row['away_team']
    away_df['opponent'] = row['home_team']

    file_dir = 'data/womens_world_cup/world_cup_matches'
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    file_name = '{}_{}.pkl'.format(row['match_id'], category.lower())
    full_path = os.path.join(file_dir, file_name)
    final = pd.concat([home_df, away_df], ignore_index=True)
    final = final.rename(columns=config['match_report_rename_columns'])
    final.to_pickle(full_path)
    return final

def scrape_match_report_all_categories(row, config, file_path, file_check=True):
    """
        scrapes a match report for all categories and merges them

        Args:
            row(pd.series): row from Schedule DataFrame
            config(dict): values of config file
            file_path(str): path to check for existing reports
            file_check(bool): whether or not you want to check for an existing report

    """
    file_check_arr = os.listdir(file_path)
    file_name = '{}_report.pkl'.format(row['match_id'])
    if file_name in file_check_arr and file_check:
        return False

    categories = ['summary', 'passing', 'passing_types', 'defense', 'misc', 'possession']
    for j in categories:
        try:
            scrape_match_report_from_competition_schedule(row, j, config)
            time.sleep(6)
        except Exception as e:
            print(e)

    files = all_files_in_subdirectories(file_path, key_terms=row['match_id'])
    dfs = list()
    for i in files:
        dfs.append(pd.read_pickle(i))

    merged_df = reduce(lambda left, right: pd.merge(left, right), dfs)
    full_report_path = 'data/womens_world_cup/world_cup_matches/'
    if not os.path.exists(full_report_path):
        os.makedirs(full_report_path)

    file_name = '{}_report.pkl'.format(row['match_id'])
    full_path = os.path.join(full_report_path, file_name)
    merged_df.to_pickle(full_path)

    
