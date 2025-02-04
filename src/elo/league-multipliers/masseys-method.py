import argparse
import yaml
import os
import logging
from pymongo import MongoClient
import pandas as pd
import numpy as np
import sys
from datetime import datetime
import re
from collections import defaultdict  # Added for tracking matches per league

def parse_arguments():
    """
    Parse command-line arguments for configuration.
    """
    parser = argparse.ArgumentParser(description='League Ratings via Massey\'s Method')
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

def retrieve_inter_league_matches(matches_collection, competition_ids):
    """
    Retrieve inter-league matches from MongoDB.
    """
    try:
        matches_cursor = matches_collection.find({"competition_id": {"$in": competition_ids}})
        matches = list(matches_cursor)
        logging.info(f"Retrieved {len(matches)} inter-league matches from the database.")
        return matches
    except Exception as e:
        logging.error(f"Failed to retrieve inter-league matches: {e}")
        return []

def get_team_domestic_league(teams_collection, team_id, team_name, season, combined_ids_df):
    """
    Determine the domestic league for a team in a given season.
    Uses team_name instead of team_id in logging.
    """
    try:
        team_docs = teams_collection.find({"id": int(team_id), "season": season}, {"competition_id": 1})
        competition_ids = [doc['competition_id'] for doc in team_docs]
        if not competition_ids:
            logging.warning(f"No competition IDs found for team '{team_name}' in season {season}.")
            return None
        # Ensure competition_ids are integers
        competition_ids = [int(cid) for cid in competition_ids]
        # Filter competition IDs to find domestic league
        for comp_id in competition_ids:
            league_info = combined_ids_df[combined_ids_df['competition_id'] == comp_id]
            if league_info.empty:
                logging.warning(f"Competition ID {comp_id} not found in combined_ids_df for team '{team_name}'.")
                continue
            if 'league_type' not in league_info.columns:
                logging.error(f"'league_type' column missing in combined_ids_df for competition ID {comp_id} (Team '{team_name}').")
                return None
            if league_info['league_type'].iloc[0] == 'domestic':
                league_name = league_info['league_name'].iloc[0]
                return league_name
        logging.warning(f"Domestic league not found for team '{team_name}' in season {season}.")
        return None
    except Exception as e:
        logging.error(f"Error finding domestic league for team '{team_name}' (ID {team_id}) in season {season}: {e}")
        return None

def compute_elo_gap(home_elo, away_elo):
    """
    Compute the ELO gap between home and away teams.
    """
    return home_elo - away_elo

def compute_expected_goal_diff(elo_gap, overall_avg_goal_diffs_df):
    """
    Compute the expected goal difference based on ELO gap.
    """
    # Find the appropriate ELO gap bin
    try:
        bins = overall_avg_goal_diffs_df['elo_gap_bin']
        elo_gap_bins = bins.apply(extract_lower_upper_bounds)
        for idx, bounds in elo_gap_bins.iteritems():  # Changed from iteritems() to items()
            logging.debug(f"Processing bin {idx}: bounds {bounds}")
            if bounds is None or bounds == (None, None):
                logging.warning(f"Bin {idx} has invalid bounds. Skipping.")
                continue
            lower, upper = bounds
            logging.debug(f"Lower bound: {lower}, Upper bound: {upper}")
            if lower <= elo_gap <= upper:
                expected_goal_diff = overall_avg_goal_diffs_df.loc[idx, 'avg_goal_diff']
                logging.debug(f"Matched bin {idx}: expected_goal_diff={expected_goal_diff}")
                return expected_goal_diff
        
        # If ELO gap is outside the range, use the closest bin
        if elo_gap < elo_gap_bins.iloc[0][0]:
            expected_goal_diff = overall_avg_goal_diffs_df['avg_goal_diff'].iloc[0]
            logging.debug(f"ELO gap below all bins: expected_goal_diff={expected_goal_diff}")
            return expected_goal_diff
        else:
            expected_goal_diff = overall_avg_goal_diffs_df['avg_goal_diff'].iloc[-1]
            logging.debug(f"ELO gap above all bins: expected_goal_diff={expected_goal_diff}")
            return expected_goal_diff
    except Exception as e:
        logging.error(f"Failed to compute expected goal difference for ELO gap {elo_gap}: {e}")
        return 0.0

def extract_lower_upper_bounds(bin_str):
    """
    Extract the lower and upper bounds from an elo_gap_bin string using regex.
    Handles both single and double hyphens for negative ranges.
    """
    try:
        # Use regex to extract two integers (possibly negative)
        matches = re.findall(r'-?\d+', bin_str)
        if len(matches) != 2:
            raise ValueError(f"Expected 2 numbers, found {len(matches)} in '{bin_str}'")
        lower_str, upper_str = matches
        return int(lower_str), int(upper_str)
    except Exception as e:
        logging.error(f"Failed to extract bounds from elo_gap_bin '{bin_str}': {e}")
        return None, None

def build_massey_matrix(data):
    """
    Build Massey's matrix and vector for solving league ratings.
    """
    leagues = sorted(set(data['home_league']).union(set(data['away_league'])))
    league_indices = {league: idx for idx, league in enumerate(leagues)}
    n_leagues = len(leagues)

    # Initialize Massey's matrix and vector
    M = np.zeros((n_leagues, n_leagues))
    y = np.zeros(n_leagues)

    # Build the system of equations
    for idx, row in data.iterrows():
        home_idx = league_indices[row['home_league']]
        away_idx = league_indices[row['away_league']]
        residual = row['residual']

        M[home_idx, home_idx] += 1
        M[away_idx, away_idx] += 1
        M[home_idx, away_idx] -= 1
        M[away_idx, home_idx] -= 1

        y[home_idx] += residual
        y[away_idx] -= residual

    # Impose constraint that ratings sum to zero
    M[-1, :] = 1
    y[-1] = 0

    return M, y, leagues

def solve_massey_equation(M, y):
    """
    Solve Massey's equation to find league ratings.
    """
    try:
        ratings = np.linalg.lstsq(M, y, rcond=None)[0]
        return ratings
    except Exception as e:
        logging.error(f"Failed to solve Massey's equation: {e}")
        return None

def get_team_elo(matches_collection, team_id, league_competition_ids, match_date_unix):
    """
    Get the team's ELO either from the next domestic match after the given date,
    or from the most recent domestic match before the given date.
    """
    # First, try to get the ELO from the next match after the given date
    try:
        query_next = {
            "competition_id": {"$in": league_competition_ids},
            "date_unix": {"$gt": match_date_unix},
            "$or": [
                {"homeID": team_id},
                {"awayID": team_id},
            ]
        }
        # Sort by date_unix ascending to get the next match after the inter-league match
        next_match_cursor = matches_collection.find(query_next).sort("date_unix", 1).limit(1)
        next_matches = list(next_match_cursor)
        if next_matches:
            match = next_matches[0]
            if match['homeID'] == team_id:
                # If the team is the home team, get their pre-match ELO
                elo = float(match['home_elo_pre_match_HA'])
            else:
                # If the team is the away team, get their pre-match ELO
                elo = float(match['away_elo_pre_match_HA'])
            return elo
    except Exception as e:
        logging.error(f"Error retrieving next ELO for team ID {team_id}: {e}")

    # If not found, try to get the ELO from the most recent match before the given date
    try:
        query_prev = {
            "competition_id": {"$in": league_competition_ids},
            "date_unix": {"$lt": match_date_unix},
            "$or": [
                {"homeID": team_id},
                {"awayID": team_id}
            ]
        }
        # Sort by date_unix descending to get the most recent match before the inter-league match
        prev_match_cursor = matches_collection.find(query_prev).sort("date_unix", -1).limit(1)
        prev_matches = list(prev_match_cursor)
        if prev_matches:
            match = prev_matches[0]
            if match['homeID'] == team_id:
                # If the team is the home team, get their post-match ELO
                elo = float(match['home_elo_pre_match_HA'])
            else:
                # If the team is the away team, get their post-match ELO
                elo = float(match['away_elo_pre_match_HA'])
            return elo
    except Exception as e:
        logging.error(f"Error retrieving previous ELO for team ID {team_id}: {e}")

    # If neither is found, return None
    return None

def process_league_ratings(matches_collection, teams_collection, inter_league_competition_ids,
                           combined_ids_csv, overall_avg_goal_diffs_file, output_file):
    """
    Process inter-league matches and compute league ratings.
    Also, track and log the number of matches used overall and per league.
    """
    logging.info("Starting processing of league ratings.")

    # Load Combined IDs CSV
    try:
        combined_ids_df = pd.read_csv(combined_ids_csv)
        logging.info(f"Loaded combined IDs CSV from {combined_ids_csv}.")
    except Exception as e:
        logging.error(f"Failed to read combined IDs CSV: {e}")
        return

    # Load Overall Average Goal Differences
    try:
        overall_avg_goal_diffs_df = pd.read_csv(overall_avg_goal_diffs_file)
        logging.info(f"Loaded overall average goal differences from {overall_avg_goal_diffs_file}.")
    except Exception as e:
        logging.error(f"Failed to read overall average goal differences CSV: {e}")
        return

    # Retrieve Inter-League Matches
    matches = retrieve_inter_league_matches(matches_collection, inter_league_competition_ids)
    if not matches:
        logging.error("No inter-league matches retrieved. Exiting.")
        return

    # Initialize counters for missing data
    missing_team_data_counter = 0
    missing_domestic_league_counter = 0
    missing_elo_counter = 0

    # Initialize match counters
    total_matches_used = 0
    matches_per_league = defaultdict(int)

    # Prepare Data
    data = []
    for match in matches:
        match_id = match.get('id')
        season = match.get('season')
        match_date_unix = match.get('date_unix')
        if match_date_unix is None:
            logging.warning(f"Match ID {match_id} is missing 'date_unix'. Skipping.")
            missing_team_data_counter += 1
            continue
        try:
            home_team_id = match['homeID']
            away_team_id = match['awayID']
            home_team_name = match.get('home_name', f"HomeTeam{home_team_id}")  # Fallback if name missing
            away_team_name = match.get('away_name', f"AwayTeam{away_team_id}")  # Fallback if name missing

            # Get domestic leagues
            home_league = get_team_domestic_league(teams_collection, home_team_id, home_team_name, season, combined_ids_df)
            away_league = get_team_domestic_league(teams_collection, away_team_id, away_team_name, season, combined_ids_df)

            if home_league is None or away_league is None:
                if home_league is None:
                    logging.warning(f"Domestic league not found for home team '{home_team_name}' in match ID {match_id}. Skipping match.")
                if away_league is None:
                    logging.warning(f"Domestic league not found for away team '{away_team_name}' in match ID {match_id}. Skipping match.")
                missing_domestic_league_counter += 1
                continue

            # Get competition IDs for domestic leagues
            home_league_comp_ids = combined_ids_df[
                (combined_ids_df['league_name'] == home_league) &
                (combined_ids_df['season'] == season)
            ]['competition_id'].tolist()

            away_league_comp_ids = combined_ids_df[
                (combined_ids_df['league_name'] == away_league) &
                (combined_ids_df['season'] == season)
            ]['competition_id'].tolist()

            if not home_league_comp_ids or not away_league_comp_ids:
                logging.warning(f"Competition IDs not found for leagues '{home_league}' or '{away_league}'. Skipping match ID {match_id}.")
                missing_domestic_league_counter += 1
                continue

            # Get ELOs for the teams
            home_elo = get_team_elo(matches_collection, home_team_id, home_league_comp_ids, match_date_unix)
            away_elo = get_team_elo(matches_collection, away_team_id, away_league_comp_ids, match_date_unix)

            if home_elo is None or away_elo is None:
                logging.warning(f"ELOs not found for teams '{home_team_name}' or '{away_team_name}' in match ID {match_id}. Skipping match.")
                missing_elo_counter += 1
                continue

            home_goals = int(match['homeGoalCount'])
            away_goals = int(match['awayGoalCount'])
            goal_diff = home_goals - away_goals
            elo_gap = compute_elo_gap(home_elo, away_elo)
            expected_goal_diff = compute_expected_goal_diff(elo_gap, overall_avg_goal_diffs_df)
            residual = goal_diff - expected_goal_diff

            data.append({
                'match_id': match_id,
                'home_league': home_league,
                'away_league': away_league,
                'residual': residual
            })

            # Update match counters
            total_matches_used += 1
            matches_per_league[home_league] += 1
            matches_per_league[away_league] += 1

        except KeyError as e:
            logging.warning(f"Match ID {match_id} is missing field {e}. Skipping.")
            missing_team_data_counter += 1
            continue
        except (ValueError, TypeError) as e:
            logging.warning(f"Invalid data types for Match ID {match_id}: {e}. Skipping.")
            missing_team_data_counter += 1
            continue

    if not data:
        logging.error("No valid data to process. Exiting.")
        logging.info(f"Total matches skipped due to missing team data: {missing_team_data_counter}")
        logging.info(f"Total matches skipped due to missing domestic league: {missing_domestic_league_counter}")
        logging.info(f"Total matches skipped due to missing ELOs: {missing_elo_counter}")
        return

    df = pd.DataFrame(data)
    logging.info(f"Prepared DataFrame with {len(df)} valid matches.")

    # Build and Solve Massey's Equation
    M, y, leagues = build_massey_matrix(df)
    ratings = solve_massey_equation(M, y)
    if ratings is None:
        logging.error("Failed to compute league ratings. Exiting.")
        return

    # Prepare Results
    league_ratings = pd.DataFrame({
        'league': leagues,
        'rating': ratings
    })

    # Add Computed At Timestamp
    league_ratings['computed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Sort the DataFrame from best to worst (descending order of 'rating')
    league_ratings = league_ratings.sort_values(by='rating', ascending=False).reset_index(drop=True)

    # Reorder Columns
    league_ratings = league_ratings[['computed_at', 'league', 'rating']]

    # Save Results
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        league_ratings.to_csv(output_file, index=False)
        logging.info(f"League ratings saved to {output_file}.")
    except Exception as e:
        logging.error(f"Failed to save league ratings to {output_file}: {e}")

    # Log the counters
    logging.info(f"Total matches used in calculations: {total_matches_used}")
    logging.info("Number of matches per league:")
    for league, count in matches_per_league.items():
        logging.info(f"  {league}: {count}")

    logging.info(f"Total matches skipped due to missing team data: {missing_team_data_counter}")
    logging.info(f"Total matches skipped due to missing domestic league: {missing_domestic_league_counter}")
    logging.info(f"Total matches skipped due to missing ELOs: {missing_elo_counter}")

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
        TEAMS_COLLECTION_NAME = config['mongodb']['teams_collection']
        LOG_FILE_PATH = config['paths']['log_file']
        OVERALL_AVG_GOAL_DIFFS_FILE = config['paths']['overall_average_goal_diffs_file']
        COMBINED_IDS_CSV = config['paths']['combined_ids_csv']
        LEAGUE_RATINGS_OUTPUT_FILE = config['paths']['league_ratings_output_file']
        INTER_LEAGUE_COMPETITION_IDS = config['masseys_method']['inter_league_competition_ids']
    except KeyError as e:
        print(f"Missing configuration key: {e}")
        exit(1)

    # === Initialize Logging ===
    setup_logging(LOG_FILE_PATH)
    logging.info("Starting League Ratings script.")

    # === Connect to MongoDB ===
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        matches_collection = db[MATCHES_COLLECTION_NAME]
        teams_collection = db[TEAMS_COLLECTION_NAME]
        logging.info(f"Connected to MongoDB at {MONGO_URI}, database: {DATABASE_NAME}, collections: {MATCHES_COLLECTION_NAME}, {TEAMS_COLLECTION_NAME}.")
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        exit(1)

    # === Process League Ratings ===
    process_league_ratings(
        matches_collection,
        teams_collection,
        INTER_LEAGUE_COMPETITION_IDS,
        COMBINED_IDS_CSV,
        OVERALL_AVG_GOAL_DIFFS_FILE,
        LEAGUE_RATINGS_OUTPUT_FILE
    )

    logging.info("League Ratings script completed successfully.")

if __name__ == "__main__":
    main()
