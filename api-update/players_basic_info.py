import argparse
import yaml
import requests
import logging
import sys
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
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
        filename='/root/moneyball/logs/update_player_stats.log',  # Change to your existing log file if different
        level=logging.INFO,  # Change to DEBUG for more verbose output
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def parse_arguments():
    """
    Parses command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Insert player IDs into MongoDB.")
    parser.add_argument(
        'league',
        type=str,
        help='League name (e.g., england_premier_league)'
    )
    parser.add_argument(
        '--season',
        type=str,
        default=None,
        help='Season (e.g., 2024/2025). If specified, all seasons up to and including this season will be processed. If omitted, all seasons will be processed.'
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

def get_season_ids(config, specified_season):
    """
    Determines the list of season_ids to use for API calls.
    If specified_season is None, returns all season_ids.
    If specified_season is provided, returns season_ids from the earliest up to and including the specified_season.
    """
    competition_ids = config.get('competition_ids', {})
    if not competition_ids:
        logging.error("No competition_ids found in configuration.")
        sys.exit(1)
    
    # Sort seasons based on starting year
    try:
        sorted_seasons = sorted(
            competition_ids.keys(),
            key=lambda s: int(s.split('/')[0])
        )
    except Exception as e:
        logging.error(f"Error sorting seasons: {e}")
        sys.exit(1)
    
    if specified_season:
        if specified_season not in competition_ids:
            logging.error(f"Season '{specified_season}' not found in competition_ids.")
            sys.exit(1)
        # Find the index of the specified_season
        try:
            index = sorted_seasons.index(specified_season)
            selected_seasons = sorted_seasons[:index + 1]
            logging.info(f"Using seasons from '{sorted_seasons[0]}' up to '{specified_season}'")
            return [competition_ids[season] for season in selected_seasons]
        except ValueError:
            logging.error(f"Specified season '{specified_season}' is not in the competition_ids.")
            sys.exit(1)
    else:
        # No season specified, return all competition_ids
        logging.info(f"No season specified. Using all available seasons from '{sorted_seasons[0]}' to '{sorted_seasons[-1]}'")
        return [competition_ids[season] for season in sorted_seasons]

def fetch_players(KEY, season_id):
    """
    Fetches players from the API for the given season_id.
    Handles pagination if necessary.
    Returns a list of player dictionaries.
    """
    players = []
    base_url = "https://api.football-data-api.com/league-players"
    params = {
        'key': KEY,
        'season_id': season_id,
        'page': 1  # Start with the first page
    }
    
    while True:
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('success'):
                logging.error(f"API call unsuccessful for season_id {season_id}: {data}")
                break
            
            fetched_players = data.get('data', [])
            players.extend(fetched_players)
            logging.info(f"Fetched {len(fetched_players)} players from season_id {season_id}, page {params['page']}")
            
            pager = data.get('pager', {})
            if params['page'] >= pager.get('max_page', 1):
                break
            params['page'] += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed for season_id {season_id}, page {params['page']}: {e}")
            break
        except ValueError as ve:
            logging.error(f"Error parsing JSON response for season_id {season_id}, page {params['page']}: {ve}")
            break
    
    logging.info(f"Total players fetched for season_id {season_id}: {len(fetched_players)}")
    return players

def connect_mongo():
    """
    Connects to MongoDB and returns the collection.
    """
    try:
        client = MongoClient('localhost', 27017)
        db = client['footballDB']  # Replace with your database name
        collection = db['players']
        # Create unique index on 'id' and 'competition_id'
        collection.create_index([('id', ASCENDING), ('competition_id', ASCENDING)], unique=True)
        logging.info("Connected to MongoDB and ensured unique index on (id, competition_id).")
        return collection
    except Exception as e:
        logging.error(f"MongoDB connection failed: {e}")
        sys.exit(1)

def insert_players(collection, players):
    """
    Inserts player documents into MongoDB.
    """
    inserted_count = 0
    for player in players:
        # Directly use the player dictionary as the document
        document = player.copy()
        try:
            collection.insert_one(document)
            inserted_count += 1
            logging.debug(f"Inserted player ID {document.get('id')} into MongoDB.")
        except DuplicateKeyError:
            logging.warning(f"Duplicate entry for player ID {document.get('id')} and competition ID {document.get('competition_id')}. Skipping.")
        except Exception as e:
            logging.error(f"Failed to insert player ID {document.get('id')}: {e}")
    
    logging.info(f"Inserted {inserted_count} new players into MongoDB.")

def main():
    setup_logging()
    args = parse_arguments()
    
    # Hardcoded base path for YAML configs
    config_base_path = '/root/moneyball/Configs'
    config_file = os.path.join(config_base_path, f"{args.league}.yaml")
    config = load_config(config_file)
    
    # Get list of season_ids to process
    season_ids = get_season_ids(config, args.season)
    
    # Connect to MongoDB
    collection = connect_mongo()
    
    total_inserted = 0
    for season_id in season_ids:
        logging.info(f"Processing season_id {season_id}")
        players = fetch_players(KEY, season_id)
        
        if not players:
            logging.warning(f"No players fetched for season_id {season_id}. Continuing to next season.")
            continue
        
        insert_players(collection, players)
        # Optionally, you can keep a running total
        # total_inserted += inserted_count
    
    logging.info("Script completed successfully.")

if __name__ == "__main__":
    main()
