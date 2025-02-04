#!/usr/bin/env python3

import argparse
import yaml
import requests
import logging
import sys
import time
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError, PyMongoError
import os
import re
from datetime import datetime

# Import the API key from API.py
try:
    from api import KEY
except ImportError:
    print("Error: API.py not found or KEY not defined.")
    sys.exit(1)

def setup_logging():
    """
    Sets up the logging configuration.
    """
    logging.basicConfig(
        filename='/root/moneyball/logs/update_team_stats.log',
        level=logging.INFO,  # Change to DEBUG for more detailed logs
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def parse_arguments():
    """
    Parses command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Update advanced team statistics in MongoDB.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '-t', '--team',
        type=int,
        help='Update data for an individual team by Team ID.'
    )
    group.add_argument(
        '-l', '--league',
        type=str,
        help='Update data for all teams within a specific league by League Name.'
    )
    return parser.parse_args()

def load_config(config_path):
    """
    Loads the YAML configuration file.
    """
    if not os.path.exists(config_path):
        logging.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    with open(config_path, 'r') as file:
        try:
            config = yaml.safe_load(file)
            logging.info(f"Loaded configuration from {config_path}")
            return config
        except yaml.YAMLError as exc:
            logging.error(f"Error parsing YAML file: {exc}")
            sys.exit(1)

def connect_mongo():
    """
    Connects to MongoDB and returns the collection.
    """
    try:
        client = MongoClient('localhost', 27017)
        db = client['footballDB']  # Replace with your database name if different
        collection = db['teams']
        # Create unique index on 'id', 'season', 'competition_id'
        collection.create_index([('id', ASCENDING), ('season', ASCENDING), ('competition_id', ASCENDING)], unique=True)
        logging.info("Connected to MongoDB and ensured unique index on (id, season, competition_id).")
        return collection
    except PyMongoError as e:
        logging.error(f"MongoDB connection failed: {e}")
        sys.exit(1)

def get_team_ids_by_league(collection, league_name, config_base_path='/root/moneyball/Configs'):
    """
    Retrieves team IDs for a given league name for the most recent season (2024/2025).
    """
    config_file = os.path.join(config_base_path, f"{league_name}.yaml")
    config = load_config(config_file)
    
    competition_ids = config.get('competition_ids', {})
    if not competition_ids:
        logging.error(f"No competition_ids found in configuration for league '{league_name}'.")
        sys.exit(1)
    
    # Select the competition_id for the most recent season (2024/2025)
    recent_season = "2024/2025"
    competition_id = competition_ids.get(recent_season)
    if not competition_id:
        logging.error(f"No competition_id found for the recent season '{recent_season}' in league '{league_name}'.")
        sys.exit(1)
    
    logging.info(f"Using competition_id {competition_id} for season '{recent_season}'.")
    
    # Fetch team IDs for the selected competition_id
    try:
        query = {"competition_id": competition_id}
        projection = {"id": 1, "_id": 0}
        teams_cursor = collection.find(query, projection)
        
        team_ids = set()
        for team in teams_cursor:
            team_ids.add(team['id'])
        
        team_count = collection.count_documents(query)
        logging.info(f"Season '{recent_season}' (competition_id {competition_id}): Found {team_count} teams.")
        
        logging.info(f"Total unique teams found for league '{league_name}': {len(team_ids)}.")
        return list(team_ids)
    except PyMongoError as e:
        logging.error(f"Error querying MongoDB for league '{league_name}': {e}")
        sys.exit(1)

def get_team_ids_by_team(collection, team_id):
    """
    Retrieves team ID for a given team ID.
    Validates its existence in the database.
    """
    try:
        # Assuming 12325 is the competition_id for 2024/2025
        query = {"id": team_id, "competition_id": 12325}
        projection = {"id": 1, "_id": 0}
        team = collection.find_one(query, projection)
        if team:
            logging.info(f"Found team ID {team_id}.")
            return [team_id]
        else:
            logging.warning(f"Team ID {team_id} not found in the database for competition_id 12325.")
            return []
    except PyMongoError as e:
        logging.error(f"Error querying MongoDB for team ID {team_id}: {e}")
        sys.exit(1)

def fetch_team_stats(KEY, team_id):
    """
    Fetches advanced team statistics from the API for a given team ID.
    Returns the JSON response and the 'request_remaining' count.
    """
    url = f"https://api.football-data-api.com/team"
    params = {
        'key': KEY,
        'team_id': team_id
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            logging.error(f"API call unsuccessful for team ID {team_id}: {data}")
            return None, None
        metadata = data.get('metadata', {})
        request_remaining = int(metadata.get('request_remaining', 1800))  # Default to 1800 if not provided
        return data, request_remaining
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for team ID {team_id}: {e}")
        return None, None
    except ValueError as ve:
        logging.error(f"Error parsing JSON response for team ID {team_id}: {ve}")
        return None, None

def upsert_team_stats(collection, team_stats):
    """
    Upserts team statistics into MongoDB.
    Each entry is unique based on 'id', 'season', and 'competition_id'.
    Adds a 'last_updated' field with the current readable datetime.
    """
    try:
        for stat in team_stats:
            team_id = stat.get('id')
            season = stat.get('season')
            competition_id = stat.get('competition_id')
            if not team_id or not season or not competition_id:
                logging.warning(f"Missing 'id', 'season', or 'competition_id' in stat: {stat}")
                continue
            
            # Add 'last_updated' field with current UTC datetime in ISO format
            stat['last_updated'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            # Alternatively, for ISO 8601 format:
            # stat['last_updated'] = datetime.utcnow().isoformat() + 'Z'  # 'Z' indicates UTC time
            
            # Define the filter and update
            filter_query = {"id": team_id, "season": season, "competition_id": competition_id}
            update_data = {"$set": stat}
            # Perform upsert
            collection.update_one(filter_query, update_data, upsert=True)
            logging.debug(f"Upserted data for team ID {team_id}, season '{season}', competition ID {competition_id}.")
    except PyMongoError as e:
        logging.error(f"Error upserting team statistics: {e}")

def parse_reset_time(reset_message):
    """
    Parses the reset message to determine wait time in seconds.
    Example reset_message: "Request limit is refreshed every hour."
    """
    match = re.search(r'refreshed every (\w+)', reset_message)
    if match:
        unit = match.group(1).lower()
        if unit in ['minute', 'minutes']:
            return 60 * 60  # Assuming we wait for 60 minutes
        elif unit in ['hour', 'hours']:
            return 60 * 60  # 60 minutes
        # Add more units as needed
    # Default wait time
    return 60 * 60  # 60 minutes

def main():
    setup_logging()
    args = parse_arguments()
    
    # Connect to MongoDB
    collection = connect_mongo()
    
    # Determine team IDs based on input arguments
    if args.team:
        team_ids = get_team_ids_by_team(collection, args.team)
        logging.info(f"Updating data for individual team ID: {args.team}")
    elif args.league:
        team_ids = get_team_ids_by_league(collection, args.league)
        logging.info(f"Updating data for league: {args.league}")
    else:
        logging.error("No valid argument provided. Use -t or -l.")
        sys.exit(1)
    
    if not team_ids:
        logging.warning("No team IDs found to update. Exiting.")
        sys.exit(0)
    
    total_teams = len(team_ids)
    logging.info(f"Starting update for {total_teams} teams.")
    
    for index, team_id in enumerate(team_ids, start=1):
        logging.info(f"Processing team {index}/{total_teams}: ID {team_id}")
        
        # Fetch team stats from API
        data, request_remaining = fetch_team_stats(KEY, team_id)
        
        if data and 'data' in data:
            team_stats = data['data']
            upsert_team_stats(collection, team_stats)
            logging.info(f"Updated stats for team ID {team_id}.")
        else:
            logging.warning(f"No data returned for team ID {team_id}. Skipping.")
        
        # Check API rate limit
        if request_remaining is not None:
            logging.info(f"API requests remaining: {request_remaining}")
            if request_remaining <= 1:
                reset_message = data.get('metadata', {}).get('request_reset_message', '')
                wait_time = parse_reset_time(reset_message)
                logging.warning(f"API request remaining is {request_remaining}. Waiting for {wait_time / 60} minutes to reset rate limit.")
                time.sleep(wait_time)
                logging.info("Resuming API calls after waiting.")
    
    logging.info("Completed updating team statistics.")

if __name__ == "__main__":
    main()
