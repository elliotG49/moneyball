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
        filename='/root/moneyball/logs/update_player_stats.log',
        level=logging.INFO,  # Change to DEBUG for more detailed logs
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def parse_arguments():
    """
    Parses command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Update advanced player statistics in MongoDB.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '-p', '--player',
        type=int,
        help='Update data for an individual player by Player ID.'
    )
    group.add_argument(
        '-t', '--team',
        type=int,
        help='Update data for all players within a specific team by Team ID.'
    )
    group.add_argument(
        '-l', '--league',
        type=str,
        help='Update data for all players within a specific league by League Name.'
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
        collection = db['players']
        # Create unique index on 'id' and 'competition_id'
        collection.create_index([('id', ASCENDING), ('competition_id', ASCENDING)], unique=True)
        logging.info("Connected to MongoDB and ensured unique index on (id, competition_id).")
        return collection
    except PyMongoError as e:
        logging.error(f"MongoDB connection failed: {e}")
        sys.exit(1)

def get_player_ids_by_team(collection, team_id):
    """
    Retrieves player IDs for a given team ID.
    Considers both 'club_team_id' and 'club_team_2_id'.
    """
    try:
        # Query for club_team_id
        players_primary = collection.find({"club_team_id": team_id}, {"id": 1, "_id": 0})
        # Query for club_team_2_id
        players_secondary = collection.find({"club_team_2_id": team_id}, {"id": 1, "_id": 0})
        
        # Combine player IDs from both queries
        player_ids = set()
        for player in players_primary:
            player_ids.add(player['id'])
        for player in players_secondary:
            player_ids.add(player['id'])
        
        logging.info(f"Found {len(player_ids)} players for team ID {team_id}.")
        return list(player_ids)
    except PyMongoError as e:
        logging.error(f"Error querying MongoDB for team ID {team_id}: {e}")
        sys.exit(1)

def get_player_ids_by_league(collection, league_name, config_base_path='/root/moneyball/Configs'):
    """
    Retrieves player IDs for a given league name.
    """
    config_file = os.path.join(config_base_path, f"{league_name}.yaml")
    config = load_config(config_file)
    
    competition_ids = config.get('competition_ids', {})
    if not competition_ids:
        logging.error(f"No competition_ids found in configuration for league '{league_name}'.")
        sys.exit(1)
    
    # Fetch all competition_ids for the league
    try:
        player_ids = set()
        for season, competition_id in competition_ids.items():
            # Query for competition_id
            players_cursor = collection.find({"competition_id": competition_id}, {"id": 1, "_id": 0})
            
            # Iterate through the cursor to collect player IDs
            for player in players_cursor:
                player_ids.add(player['id'])
            
            # Use count_documents to get the number of players
            player_count = collection.count_documents({"competition_id": competition_id})
            logging.info(f"Season '{season}' (competition_id {competition_id}): Found {player_count} players.")
        
        logging.info(f"Total unique players found for league '{league_name}': {len(player_ids)}.")
        return list(player_ids)
    except PyMongoError as e:
        logging.error(f"Error querying MongoDB for league '{league_name}': {e}")
        sys.exit(1)


def fetch_player_stats(KEY, player_id):
    """
    Fetches advanced player statistics from the API for a given player ID.
    Returns the JSON response and the 'request_remaining' count.
    """
    url = f"https://api.football-data-api.com/player-stats"
    params = {
        'key': KEY,
        'player_id': player_id
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if not data.get('success'):
            logging.error(f"API call unsuccessful for player ID {player_id}: {data}")
            return None, None
        metadata = data.get('metadata', {})
        request_remaining = int(metadata.get('request_remaining', 1800))  # Default to 1800 if not provided
        return data, request_remaining
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for player ID {player_id}: {e}")
        return None, None
    except ValueError as ve:
        logging.error(f"Error parsing JSON response for player ID {player_id}: {ve}")
        return None, None

def upsert_player_stats(collection, player_stats):
    """
    Upserts player statistics into MongoDB.
    Each entry is unique based on 'id' and 'competition_id'.
    """
    try:
        for stat in player_stats:
            player_id = stat.get('id')
            competition_id = stat.get('competition_id')
            if not player_id or not competition_id:
                logging.warning(f"Missing 'id' or 'competition_id' in stat: {stat}")
                continue
            # Define the filter and update
            filter_query = {"id": player_id, "competition_id": competition_id}
            update_data = {"$set": stat}
            # Perform upsert
            collection.update_one(filter_query, update_data, upsert=True)
            logging.debug(f"Upserted data for player ID {player_id}, competition ID {competition_id}.")
    except PyMongoError as e:
        logging.error(f"Error upserting player statistics: {e}")

def main():
    setup_logging()
    args = parse_arguments()
    
    # Connect to MongoDB
    collection = connect_mongo()
    
    # Determine player IDs based on input arguments
    if args.player:
        player_ids = [args.player]
        logging.info(f"Updating data for individual player ID: {args.player}")
    elif args.team:
        player_ids = get_player_ids_by_team(collection, args.team)
        logging.info(f"Updating data for team ID: {args.team}")
    elif args.league:
        player_ids = get_player_ids_by_league(collection, args.league)
        logging.info(f"Updating data for league: {args.league}")
    else:
        logging.error("No valid argument provided. Use -p, -t, or -l.")
        sys.exit(1)
    
    if not player_ids:
        logging.warning("No player IDs found to update. Exiting.")
        sys.exit(0)
    
    total_players = len(player_ids)
    logging.info(f"Starting update for {total_players} players.")
    
    # Initialize API rate limit tracking
    api_calls_made = 0
    api_limit = 1800  # As per the metadata example
    api_wait_time = 60 * 60  # 60 minutes in seconds
    
    for index, player_id in enumerate(player_ids, start=1):
        logging.info(f"Processing player {index}/{total_players}: ID {player_id}")
        
        # Fetch player stats from API
        data, request_remaining = fetch_player_stats(KEY, player_id)
        
        if data and 'data' in data:
            player_stats = data['data']
            upsert_player_stats(collection, player_stats)
            logging.info(f"Updated stats for player ID {player_id}.")
        else:
            logging.warning(f"No data returned for player ID {player_id}. Skipping.")
        
        # Check API rate limit
        if request_remaining is not None:
            logging.info(f"API requests remaining: {request_remaining}")
            if request_remaining <= 1:
                logging.warning("API request remaining is 1. Waiting for 60 minutes to reset rate limit.")
                time.sleep(api_wait_time)
                logging.info("Resuming API calls after waiting.")
    
    logging.info("Completed updating player statistics.")

if __name__ == "__main__":
    main()
