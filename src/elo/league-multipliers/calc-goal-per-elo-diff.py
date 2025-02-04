import argparse
import yaml
import os
import logging
from pymongo import MongoClient
import pandas as pd
import numpy as np
import sys
from datetime import datetime

def parse_arguments():
    """
    Parse command-line arguments for configuration.
    """
    parser = argparse.ArgumentParser(description='Massey\'s Method Implementation for Football Leagues')
    parser.add_argument('--config', type=str, required=True, help='Path to the YAML configuration file')
    args = parser.parse_args()
    return args.config

def load_config(config_path):
    """
    Load the YAML configuration file.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def setup_logging(log_file_path):
    """
    Configure the logging module.
    """
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,  # Change to DEBUG for more detailed logs
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler(sys.stdout)
        ]
    )

def retrieve_matches(matches_collection, competition_ids):
    """
    Retrieve all matches for the specified competition IDs from the MongoDB collection.
    """
    try:
        matches_cursor = matches_collection.find({"competition_id": {"$in": competition_ids}})
        matches = list(matches_cursor)
        logging.info(f"Retrieved {len(matches)} matches for the specified competition IDs from the database.")
        return matches
    except Exception as e:
        logging.error(f"Failed to retrieve matches: {e}")
        return []

def compute_elo_gap(home_elo, away_elo):
    """
    Compute the ELO gap between home and away teams.
    """
    return home_elo - away_elo

def bin_elo_gaps(elo_gaps, bin_size=50, max_gap=500):
    """
    Bin ELO gaps into intervals.
    """
    bins = np.arange(-max_gap - bin_size, max_gap + bin_size + 1, bin_size)
    labels = [f"{int(bins[i])}-{int(bins[i+1]-1)}" for i in range(len(bins)-1)]
    binned = pd.cut(elo_gaps, bins=bins, labels=labels, include_lowest=True)
    return binned

def calculate_average_goal_diff(df, goal_diff_column='goal_diff'):
    """
    Calculate the average goal difference for each ELO gap bin.
    """
    avg_goal_diff = df.groupby('elo_gap_bin')[goal_diff_column].mean().reset_index()
    avg_goal_diff.rename(columns={goal_diff_column: 'avg_goal_diff'}, inplace=True)
    return avg_goal_diff

def extract_lower_bound(bin_str):
    """
    Safely extract the lower bound from an elo_gap_bin string.
    """
    try:
        # Remove any whitespace
        bin_str = bin_str.strip()
        # Use regex to match the lower bound
        import re
        match = re.match(r'^(-?\d+)-', bin_str)
        if match:
            lower_bound_str = match.group(1)
            return int(lower_bound_str)
        else:
            logging.error(f"Could not parse elo_gap_bin '{bin_str}'")
            return None
    except Exception as e:
        logging.error(f"Failed to extract lower bound from elo_gap_bin '{bin_str}': {e}")
        return None

def split_into_batches(lst, batch_size):
    """
    Split a list into batches of given size.
    """
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]

def process_competitions(matches_collection, competition_ids, output_file, bin_size, max_gap, batch_size):
    """
    Process matches for given competition IDs in batches and save the overall average goal differences.
    """
    logging.info(f"Processing competitions with IDs: {competition_ids}")
    
    # Initialize bins
    bins = np.arange(-max_gap - bin_size, max_gap + bin_size + 1, bin_size)
    bin_labels = [f"{int(bins[i])}-{int(bins[i+1]-1)}" for i in range(len(bins)-1)]
    all_bins = pd.Categorical(bin_labels, categories=bin_labels, ordered=True)
    
    # Initialize sums and counts dictionaries
    sums = {bin_label: 0.0 for bin_label in bin_labels}
    counts = {bin_label: 0 for bin_label in bin_labels}
    
    # Split competition IDs into batches
    batches = list(split_into_batches(competition_ids, batch_size))
    logging.info(f"Competition IDs divided into {len(batches)} batches of size up to {batch_size}.")
    
    for batch_num, batch_competition_ids in enumerate(batches, start=1):
        logging.info(f"Processing batch {batch_num}/{len(batches)} with competition IDs: {batch_competition_ids}")
        # Retrieve Matches
        matches = retrieve_matches(matches_collection, batch_competition_ids)
        if not matches:
            logging.warning(f"No matches retrieved for batch {batch_num}. Skipping.")
            continue
        
        # Prepare DataFrame
        data = []
        for match in matches:
            match_id = match.get('id')
            try:
                home_elo = float(match['home_elo_pre_match_HA'])
                away_elo = float(match['away_elo_pre_match_HA'])
                home_goals = int(match['homeGoalCount'])
                away_goals = int(match['awayGoalCount'])
                goal_diff = home_goals - away_goals
                elo_gap = compute_elo_gap(home_elo, away_elo)
                
                data.append({
                    'match_id': match_id,
                    'home_id': match.get('homeID'),
                    'away_id': match.get('awayID'),
                    'home_goals': home_goals,
                    'away_goals': away_goals,
                    'goal_diff': goal_diff,
                    'elo_gap': elo_gap
                })
            except KeyError as e:
                logging.warning(f"Match ID {match_id} is missing field {e}. Skipping.")
                continue
            except (ValueError, TypeError):
                logging.warning(f"Invalid data types for Match ID {match_id}. Skipping.")
                continue
        
        df = pd.DataFrame(data)
        logging.info(f"Prepared DataFrame with {len(df)} valid matches for batch {batch_num}.")
        
        if df.empty:
            logging.warning(f"DataFrame is empty for batch {batch_num}. Skipping.")
            continue
        
        # Bin ELO Gaps
        df['elo_gap_bin'] = bin_elo_gaps(df['elo_gap'], bin_size=bin_size, max_gap=max_gap)
        logging.info(f"Binned ELO gaps into intervals for batch {batch_num}.")
        
        # Calculate sums and counts for each bin
        bin_sums = df.groupby('elo_gap_bin')['goal_diff'].sum()
        bin_counts = df.groupby('elo_gap_bin')['goal_diff'].count()
        
        # Update overall sums and counts
        for bin_label in bin_labels:
            sums[bin_label] += bin_sums.get(bin_label, 0.0)
            counts[bin_label] += bin_counts.get(bin_label, 0)
        
        logging.info(f"Updated sums and counts for batch {batch_num}.")
    
    # After processing all batches, compute the average goal differences
    avg_goal_diff_data = []
    for bin_label in bin_labels:
        total_sum = sums[bin_label]
        total_count = counts[bin_label]
        if total_count > 0:
            avg_goal_diff = total_sum / total_count
        else:
            avg_goal_diff = 0.0
        avg_goal_diff_data.append({
            'elo_gap_bin': bin_label,
            'avg_goal_diff': avg_goal_diff
        })
    
    avg_goal_diff_df = pd.DataFrame(avg_goal_diff_data)
    logging.info("Calculated average goal differences for each ELO gap bin.")
    
    # Add Computed At Timestamp
    avg_goal_diff_df['computed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Reorder Columns
    avg_goal_diff_df = avg_goal_diff_df[['computed_at', 'elo_gap_bin', 'avg_goal_diff']]
    
    # Sort the bins numerically
    avg_goal_diff_df['elo_gap_lower'] = avg_goal_diff_df['elo_gap_bin'].apply(extract_lower_bound)
    avg_goal_diff_df = avg_goal_diff_df.dropna(subset=['elo_gap_lower']).sort_values('elo_gap_lower').drop('elo_gap_lower', axis=1)
    
    # Save Results
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        avg_goal_diff_df.to_csv(output_file, index=False)
        logging.info(f"Overall average goal differences saved to {output_file}.")
    except Exception as e:
        logging.error(f"Failed to save results to {output_file}: {e}")

def main():
    # === Parse Command-Line Arguments ===
    config_path = parse_arguments()

    # === Load Configuration ===
    try:
        config = load_config(config_path)
    except Exception as e:
        print(f"Error loading configuration: {e}")
        exit(1)

    # === Extract Configurations ===
    try:
        MONGO_URI = config['mongodb']['uri']
        DATABASE_NAME = config['mongodb']['database']
        MATCHES_COLLECTION_NAME = config['mongodb']['matches_collection']
        LOG_FILE_PATH = config['paths']['log_file']
        TOP_DIVISION_OUTPUT_FILE_PATH = config['paths']['top_division_overall_output_file']
        LOWER_DIVISION_OUTPUT_FILE_PATH = config['paths']['lower_division_overall_output_file']
        BIN_SIZE = config['masseys_method']['bin_size']
        MAX_GAP = config['masseys_method']['max_gap']
        BATCH_SIZE = config['masseys_method'].get('batch_size', 10)  # Default batch size is 10
        TOP_DIVISION_COMPETITION_IDS = config['masseys_method']['top_division_competition_ids']
        LOWER_DIVISION_COMPETITION_IDS = config['masseys_method']['lower_division_competition_ids']
    except KeyError as e:
        print(f"Missing configuration key: {e}")
        exit(1)

    # === Initialize Logging ===
    setup_logging(LOG_FILE_PATH)
    logging.info("Starting Massey's Method script.")

    # === Connect to MongoDB ===
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        matches_collection = db[MATCHES_COLLECTION_NAME]
        logging.info(f"Connected to MongoDB at {MONGO_URI}, database: {DATABASE_NAME}, collection: {MATCHES_COLLECTION_NAME}.")
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        exit(1)

    # === Process Top Division Competitions ===
    process_competitions(
        matches_collection,
        TOP_DIVISION_COMPETITION_IDS,
        TOP_DIVISION_OUTPUT_FILE_PATH,
        BIN_SIZE,
        MAX_GAP,
        batch_size=BATCH_SIZE
    )

    # === Process Lower Division Competitions ===
    process_competitions(
        matches_collection,
        LOWER_DIVISION_COMPETITION_IDS,
        LOWER_DIVISION_OUTPUT_FILE_PATH,
        BIN_SIZE,
        MAX_GAP,
        batch_size=BATCH_SIZE
    )

    logging.info("Massey's Method script completed successfully.")

if __name__ == "__main__":
    main()
