import os
import time
import json
import requests
import logging
import argparse
import yaml
from pymongo import MongoClient, errors, ASCENDING
from api import KEY  # Ensure that 'api.py' contains the 'KEY' variable

# Set up logging
logging.basicConfig(
    filename='/root/moneybal;/logs/manual_import.log',  # Log file path
    level=logging.INFO,  # Logging level
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log message format
)

# MongoDB setup
client = MongoClient('localhost', 27017)
db = client.footballDB  # Replace with your database name
collection = db.matches  # Replace with your collection name

def get_next_game_week(competition_id):
    """
    Connects to MongoDB, queries the matches collection for the earliest incomplete match
    in the given competition, and returns the game_week of that match as an integer.
    """
    try:
        matches_collection = db.matches
        # Build the query
        query = {
            "competition_id": int(competition_id),
            "status": "incomplete"
        }
        projection = {
            "game_week": 1,
            "date_unix": 1,
            "_id": 0  # Exclude the _id field from the result
        }
        sort = [("date_unix", ASCENDING)]

        # Perform the query
        cursor = matches_collection.find(query, projection).sort(sort).limit(1)

        # Get the game_week from the result
        match = next(cursor, None)  # Get the first document from the cursor
        if match:
            game_week = match.get("game_week")
            if game_week is not None:
                game_week_int = int(game_week)
                logging.info(f"Next game_week for competition_id {competition_id} is: {game_week_int}")
                return game_week_int
            else:
                logging.info(f"No game_week found in the document for competition_id {competition_id}.")
                return None
        else:
            logging.info(f"No incomplete matches found for the given competition_id {competition_id}.")
            return None

    except Exception as e:
        logging.error(f"An error occurred while fetching the next game week: {e}")
        return None

def get_match_ids_for_game_week(competition_id, game_week):
    """
    Returns a list of match_ids for a given competition_id and game_week from the database.
    """
    try:
        matches_collection = db.matches
        query = {
            "competition_id": int(competition_id),
            "game_week": int(game_week)
        }
        projection = {
            "id": 1,
            "_id": 0  # Exclude the _id field
        }
        cursor = matches_collection.find(query, projection)
        match_ids = [match['id'] for match in cursor if 'id' in match]
        logging.info(f"Found {len(match_ids)} match_ids for competition_id {competition_id} game_week {game_week}")
        return match_ids
    except Exception as e:
        logging.error(f"An error occurred while fetching match_ids for competition_id {competition_id} game_week {game_week}: {e}")
        return []

def fetch_and_insert_data(match_ids):
    """Fetch detailed data for each match_id and insert/update into MongoDB."""
    for match_id in match_ids:
        match_stats_url = f'https://api.football-data-api.com/match?key={KEY}&match_id={match_id}'
        logging.info(f"Fetching detailed data from URL: {match_stats_url}")
        try:
            response = requests.get(match_stats_url, timeout=10)  # Added timeout for robustness
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            detailed_data = response.json()
            insert_document(detailed_data, collection)
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error fetching data for match {match_id}: {req_err}")
        except json.JSONDecodeError as json_err:
            logging.error(f"JSON decode error for match {match_id}: {json_err}")
        except Exception as e:
            logging.error(f"Unexpected error fetching data for match {match_id}: {e}")

def insert_document(data, collection):
    """Insert or update a document in MongoDB without overwriting existing fields."""
    # Extract the nested 'data' dictionary
    match_data = data.get('data')

    if match_data and isinstance(match_data, dict) and 'id' in match_data and 'season' in match_data:
        composite_id = f"{match_data['id']}_{match_data['season']}"
        match_data["_id"] = composite_id

        try:
            # Use the $set operator to update only the fields provided by the API
            update_result = collection.update_one(
                {"_id": composite_id},
                {"$set": match_data},
                upsert=True
            )
            if update_result.upserted_id:
                logging.info(f"Inserted new match document with id {match_data['id']} for season {match_data['season']}")
            elif update_result.modified_count > 0:
                logging.info(f"Updated match document with id {match_data['id']} for season {match_data['season']}")
            else:
                logging.info(f"No changes made to match document with id {match_data['id']} for season {match_data['season']}")
        except Exception as e:
            logging.error(f"Error updating/inserting document for id {match_data['id']}: {e}")
    else:
        match_id = match_data.get('id', 'unknown') if match_data else 'unknown'
        logging.error(f"Document for match_id {match_id} is missing 'id' or 'season' or does not contain a valid 'data' object, skipping...")

def process_yaml_config(league_name):
    """Process the YAML config file to fetch and insert data for the specified competition ID."""
    try:
        # Base directory for YAML config files
        base_config_dir = '/root/moneyball/Configs'
        config_file = os.path.join(base_config_dir, f"{league_name}.yaml")

        if not os.path.exists(config_file):
            logging.error(f"Config file {config_file} does not exist.")
            return

        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
            league = config.get('league')
            competition_ids = config.get('competition_ids', {})
            competition_id_2425 = competition_ids.get('2024/2025')
            if competition_id_2425 is None:
                logging.error(f"No competition ID found for 2024/2025 in config file {config_file}")
                return
            competition_id = competition_id_2425
            logging.info(f"Processing league {league} with competition ID {competition_id} for 2024/2025 season")

            # Step 2: Get the next game week
            next_game_week = get_next_game_week(competition_id)

            # Determine game weeks to process
            game_weeks_to_process = set()  # Use a set to avoid duplicates

            if next_game_week is not None:
                game_weeks_to_process.add(next_game_week)  # Next game week
                if next_game_week > 1:
                    game_weeks_to_process.add(next_game_week - 1)  # Previous game week
                else:
                    game_weeks_to_process.add(0)  # If next_game_week is 1, add 0 instead of negative
            else:
                logging.info(f"No next game week found for competition_id {competition_id}.")

            # Always include game_week 0
            game_weeks_to_process.add(0)

            logging.info(f"Game weeks to process for competition_id {competition_id}: {sorted(game_weeks_to_process)}")

            for gw in sorted(game_weeks_to_process):
                logging.info(f"Processing game_week {gw} for competition_id {competition_id}")
                match_ids = get_match_ids_for_game_week(competition_id, gw)
                if match_ids:
                    logging.info(f"Fetching and updating data for competition_id {competition_id}, game_week {gw}")
                    fetch_and_insert_data(match_ids)
                else:
                    logging.info(f"No match_ids found for competition_id {competition_id} game_week {gw}")

    except Exception as e:
        logging.error(f"An error occurred while processing the config file {config_file}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process YAML config file for football data update.')
    parser.add_argument('league_name', type=str, help='Name of the league (used to locate the YAML config file)')
    args = parser.parse_args()
    league_name = args.league_name

    process_yaml_config(league_name)
