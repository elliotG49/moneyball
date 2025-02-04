import argparse
import yaml
import os
import logging
from pymongo import MongoClient
import joblib
import pandas as pd
import json

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
            logging.StreamHandler()
        ]
    )

def parse_arguments():
    """
    Parse command-line arguments for country_code.
    """
    parser = argparse.ArgumentParser(description='ELO Rating Calculator and Updater')
    parser.add_argument('country_code', type=str, help='Country code (e.g., ENG, GER)')
    args = parser.parse_args()
    return args.country_code

def load_league_configs(country_code):
    """
    Load configurations for all domestic leagues in the specified country.
    """
    configs = []
    configs_dir = '/root/barnard/ML/Configs'
    for filename in os.listdir(configs_dir):
        if filename.endswith('.yaml'):
            config_path = os.path.join(configs_dir, filename)
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                # Adjusted condition to handle possible missing keys
                if config.get('country', '') == country_code and config.get('league_type', '') == 'domestic':
                    configs.append(config)
    return configs

def get_elo(RA, RB, home_advantage=0):
    """
    Calculate expected scores based on ELO ratings and home advantage.
    """
    RA_adj = RA + home_advantage
    EA = 1 / (1 + 10 ** ((RB - RA_adj) / 500))
    EB = 1 - EA
    return EA, EB

def new_elo(RA, RB, EA, EB, K, SA, SB):
    """
    Update ELO ratings based on match outcome.
    """
    RA_new = RA + K * (SA - EA)
    RB_new = RB + K * (SB - EB)
    return RA_new, RB_new

def determine_scores(home_goals, away_goals):
    """
    Determine actual scores based on match goals.
    """
    if home_goals > away_goals:
        return 1, 0  # Home win
    elif home_goals < away_goals:
        return 0, 1  # Away win
    else:
        return 0.5, 0.5  # Draw

def get_prior_season(season_str):
    """
    Calculate the prior season string given the current season string.
    """
    try:
        years = season_str.split('/')
        start_year = int(years[0])
        end_year = int(years[1])
        prior_start_year = start_year - 1
        prior_end_year = end_year - 1
        prior_season = f"{prior_start_year}/{prior_end_year}"
        return prior_season
    except Exception as e:
        logging.error(f"Error parsing season string '{season_str}': {e}")
        return None

def main():
    # === Initialize Logging Early ===
    LOG_FILE_PATH = "/root/barnard/logs/elo_update.log"
    setup_logging(LOG_FILE_PATH)
    logging.info("Logging initialized.")

    try:
        # === Parse Command-Line Arguments ===
        country_code = parse_arguments()
        logging.info(f"Country code provided: {country_code}")

        # === Load League Configurations ===
        configs = load_league_configs(country_code)
        if not configs:
            logging.error(f"No league configurations found for country code '{country_code}'.")
            exit(1)
        logging.info(f"Loaded configurations for {len(configs)} leagues.")

        # === Collect Competition IDs and League Info ===
        competition_info = {}
        competitions_by_season = {}
        for config in configs:
            competition_ids_dict = config.get('competition_ids', {})
            league_level = config.get('domestic_value', 1)  # Assuming domestic_value indicates league level
            league_name = config.get('league', 'Unknown League')
            country_code = config.get('country', 'UNK')
            league_type = config.get('league_type', 'domestic')
            for season, comp_id in competition_ids_dict.items():
                # Update competition_info to include season and league details
                competition_info[comp_id] = {
                    'league_level': league_level,
                    'config': config,
                    'league_name': league_name,
                    'country_code': country_code,
                    'league_type': league_type,
                    'season': season
                }
                # Build competitions_by_season mapping
                if season not in competitions_by_season:
                    competitions_by_season[season] = []
                competitions_by_season[season].append(comp_id)
        logging.info("Competition IDs and league info collected.")

        # === Load Leagues Seasons CSV ===
        LEAGUES_SEASONS_CSV = "/root/barnard/data/betting/usefuls/all-leagues.csv"
        if not os.path.exists(LEAGUES_SEASONS_CSV):
            logging.error(f"Leagues seasons CSV file not found at {LEAGUES_SEASONS_CSV}.")
            exit(1)
        try:
            leagues_seasons_df = pd.read_csv(LEAGUES_SEASONS_CSV)
            logging.info(f"Loaded leagues seasons CSV from {LEAGUES_SEASONS_CSV}.")
        except Exception as e:
            logging.error(f"Failed to read leagues seasons CSV: {e}")
            exit(1)

        # === Map Competition IDs to Seasons ===
        # Since we have competitions_by_season from the configs, we can skip this step
        # However, if you need to verify or use additional data from the CSV, you can merge the information
        df_competitions = leagues_seasons_df[leagues_seasons_df['competition_id'].isin(competition_info.keys())]
        if df_competitions.empty:
            logging.error("No competitions found in leagues_seasons_df matching the competition IDs.")
            exit(1)
        logging.info("Mapped competition IDs to seasons from CSV.")

        # Create a mapping from competition_id to league_name using CSV data if needed
        comp_id_to_league_name = df_competitions.drop_duplicates('competition_id').set_index('competition_id')['league_name'].to_dict()

        # === Connect to MongoDB ===
        MONGO_URI = "mongodb://localhost:27017/"
        DATABASE_NAME = "footballDB"
        MATCHES_COLLECTION_NAME = "matches"

        try:
            client = MongoClient(MONGO_URI)
            db = client[DATABASE_NAME]
            matches_collection = db[MATCHES_COLLECTION_NAME]
            logging.info(f"Connected to MongoDB at {MONGO_URI}, database: {DATABASE_NAME}, collection: {MATCHES_COLLECTION_NAME}.")
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            exit(1)

        # === Load or Initialize ELO Ratings ===
        ELO_RATINGS_FILE = "/root/barnard/machine-learning/tmp/elo_ratings.joblib"
        if os.path.exists(ELO_RATINGS_FILE):
            try:
                elo_ratings = joblib.load(ELO_RATINGS_FILE)
                logging.info(f"ELO ratings loaded from {ELO_RATINGS_FILE}.")
            except Exception as e:
                logging.error(f"Failed to load ELO ratings from {ELO_RATINGS_FILE}: {e}")
                elo_ratings = {}
        else:
            logging.info("No existing ELO ratings found. Initializing empty ELO ratings.")
            elo_ratings = {}

        # === ELO Parameters ===
        INITIAL_ELO = 1500
        K_FACTOR = 20
        HOME_ADVANTAGE = 100  # Fixed home advantage

        # === Initialize Variables to Track Prior Season Teams and Standings ===
        prior_season_standings = {}
        prior_season_teams = set()

        # === Prepare to Store End-of-Season Top and Bottom 3 ELOs ===
        END_OF_SEASON_ELOS_DIR = "/root/barnard/elo/data/elo-data/end-of-season-data"
        os.makedirs(END_OF_SEASON_ELOS_DIR, exist_ok=True)

        # === Process Seasons in Order ===
        seasons = sorted(competitions_by_season.keys())
        for season in seasons:
            competition_ids_in_season = competitions_by_season[season]
            logging.info(f"Processing Season: {season} with Competition IDs: {competition_ids_in_season}")

            # === Retrieve Matches for Current Season ===
            try:
                matches_cursor = matches_collection.find({"competition_id": {"$in": competition_ids_in_season}}).sort("date_unix", 1)
                matches = list(matches_cursor)
                matches_count = len(matches)
                logging.info(f"Retrieved {matches_count} matches for Season {season}.")
            except Exception as e:
                logging.error(f"Failed to retrieve matches for Season {season}: {e}")
                continue  # Skip to the next season

            # === Collect Current Season Team IDs ===
            current_season_team_ids = set()
            for match in matches:
                home_id = match.get("homeID")
                away_id = match.get("awayID")
                if home_id:
                    current_season_team_ids.add(home_id)
                if away_id:
                    current_season_team_ids.add(away_id)

            # === Identify New Teams ===
            new_teams = current_season_team_ids - prior_season_teams
            logging.info(f"Identified {len(new_teams)} new teams in Season {season}.")

            # === Assign ELOs to New Teams ===
            for team_id in new_teams:
                assigned = False
                # Attempt to assign ELO based on prior season standings
                prior_season = get_prior_season(season)
                if prior_season and prior_season in prior_season_standings:
                    prior_teams = prior_season_standings[prior_season]['teams']
                    if team_id in prior_teams:
                        # Team was in the league last season
                        elo_ratings[team_id] = elo_ratings.get(team_id, INITIAL_ELO)
                        assigned = True
                        logging.info(f"Team {team_id} continues from prior season with ELO {elo_ratings[team_id]:.2f}.")
                    else:
                        # Assign average ELO of bottom 3 teams from prior season
                        bottom_3_elos = prior_season_standings[prior_season]['bottom_3_elos']
                        if bottom_3_elos:
                            mean_bottom_3 = sum(bottom_3_elos.values()) / len(bottom_3_elos)
                            elo_ratings[team_id] = mean_bottom_3
                            assigned = True
                            logging.info(f"Assigned average bottom 3 ELO ({mean_bottom_3:.2f}) to new team {team_id}.")
                        else:
                            # If no bottom_3_elos, assign INITIAL_ELO
                            elo_ratings[team_id] = INITIAL_ELO
                            logging.info(f"No bottom 3 ELOs available. Assigned INITIAL_ELO ({INITIAL_ELO}) to new team {team_id}.")
                if not assigned:
                    # No prior season data, assign INITIAL_ELO
                    elo_ratings[team_id] = INITIAL_ELO
                    logging.info(f"No prior season data. Assigned INITIAL_ELO ({INITIAL_ELO}) to new team {team_id}.")

            # === Process Matches ===
            for match in matches:
                match_id = match.get("id")
                if match_id is None:
                    logging.warning("Match is missing 'id' field. Skipping.")
                    continue

                try:
                    match_id_int = int(match_id)
                except (ValueError, TypeError):
                    logging.warning(f"Match ID {match_id} is not a valid integer. Skipping.")
                    continue

                # Extract necessary fields
                home_id = match.get("homeID")
                away_id = match.get("awayID")
                home_name = match.get("home_name", "Unknown Team")
                away_name = match.get("away_name", "Unknown Team")
                home_goals = match.get("homeGoalCount")
                away_goals = match.get("awayGoalCount")

                # Validate essential fields
                essential_field_names = ["homeID", "awayID", "homeGoalCount", "awayGoalCount"]
                essential_fields = [match.get(field) for field in essential_field_names]

                if any(field is None for field in essential_fields):
                    missing_fields = [fname for fname, fval in zip(essential_field_names, essential_fields) if fval is None]
                    logging.warning(f"Match ID {match_id} is missing essential fields: {', '.join(missing_fields)}. Skipping ELO update.")
                    continue

                # Determine BTTS
                try:
                    home_goals_int = int(home_goals)
                    away_goals_int = int(away_goals)
                    btts = 1 if (home_goals_int > 0 and away_goals_int > 0) else 0
                except (ValueError, TypeError):
                    btts = 0
                    logging.warning(f"Invalid goal counts for Match ID {match_id}. Setting BTTS to 0.")
                    continue  # Skip updating ELOs if goal counts are invalid

                # Retrieve pre-match ELOs
                RA_before = elo_ratings.get(home_id, INITIAL_ELO)
                RB_before = elo_ratings.get(away_id, INITIAL_ELO)

                # Calculate ELO with and without home advantage
                RA_before_HA = RA_before + HOME_ADVANTAGE
                RB_before_HA = RB_before  # Away team's ELO is not adjusted

                # Calculate expected scores
                EA, EB = get_elo(RA_before, RB_before, home_advantage=HOME_ADVANTAGE)

                # Determine actual scores
                SA, SB = determine_scores(home_goals_int, away_goals_int)

                # Update ELO ratings
                RA_after, RB_after = new_elo(RA_before, RB_before, EA, EB, K_FACTOR, SA, SB)

                # Update ELO ratings in the dictionary
                elo_ratings[home_id] = RA_after
                elo_ratings[away_id] = RB_after

                # Insert pre-match ELOs into MongoDB
                try:
                    matches_collection.update_one(
                        {"id": match_id},
                        {"$set": {
                            "home_elo_pre_match": RA_before,
                            "home_elo_pre_match_HA": RA_before_HA,
                            "away_elo_pre_match": RB_before,
                            "away_elo_pre_match_HA": RB_before_HA,
                            "BTTS": btts
                        }}
                    )
                    logging.info(f"Match ID {match_id}: Pre-match ELOs inserted into MongoDB.")
                except Exception as e:
                    logging.error(f"Failed to update Match ID {match_id} with pre-match ELOs: {e}")
                    continue

                # Log ELO updates
                logging.info(f"Match ID {match_id}: {home_name} ELO updated from {RA_before:.2f} to {RA_after:.2f} (+{RA_after - RA_before:.2f})")
                logging.info(f"Match ID {match_id}: {away_name} ELO updated from {RB_before:.2f} to {RB_after:.2f} (+{RB_after - RB_before:.2f})")

            # === End of Season Standings ===
            season_standings = {'season': season, 'teams': [], 'top_3_elos': {}, 'bottom_3_elos': {}}

            # Compute team points for standings
            team_points = {}
            for match in matches:
                home_id = match.get("homeID")
                away_id = match.get("awayID")
                home_goals = int(match.get("homeGoalCount", 0))
                away_goals = int(match.get("awayGoalCount", 0))
                if home_id not in team_points:
                    team_points[home_id] = 0
                if away_id not in team_points:
                    team_points[away_id] = 0
                if home_goals > away_goals:
                    team_points[home_id] += 3
                elif home_goals < away_goals:
                    team_points[away_id] += 3
                else:
                    team_points[home_id] += 1
                    team_points[away_id] += 1

            # Sort teams by points
            sorted_teams = sorted(team_points.items(), key=lambda x: x[1], reverse=True)
            team_ids = [tid for tid, _ in sorted_teams]

            # Get top 3 and bottom 3 teams
            top_3_teams = team_ids[:3]
            bottom_3_teams = team_ids[-3:]

            # Store standings
            season_standings['teams'] = team_ids
            season_standings['top_3_elos'] = {tid: elo_ratings.get(tid, INITIAL_ELO) for tid in top_3_teams}
            season_standings['bottom_3_elos'] = {tid: elo_ratings.get(tid, INITIAL_ELO) for tid in bottom_3_teams}

            logging.info(f"Processed standings for season {season}.")

            # Save end-of-season ELOs to a single file per season
            sanitized_season = season.replace('/', '-')
            standings_file = os.path.join(END_OF_SEASON_ELOS_DIR, f"season_{sanitized_season}_{country_code}.json")
            with open(standings_file, 'w') as f:
                json.dump(season_standings, f, indent=4)
            logging.info(f"End-of-season ELOs saved to {standings_file}")

            # Update prior season standings and teams
            prior_season_standings[season] = season_standings
            prior_season_teams = current_season_team_ids
            logging.info(f"Season {season} processing complete. Updated prior season standings and teams.")

        # === Save Updated ELO Ratings ===
        try:
            os.makedirs(os.path.dirname(ELO_RATINGS_FILE), exist_ok=True)
            joblib.dump(elo_ratings, ELO_RATINGS_FILE)
            logging.info(f"ELO ratings successfully saved to {ELO_RATINGS_FILE}.")
        except Exception as e:
            logging.error(f"Failed to save ELO ratings to {ELO_RATINGS_FILE}: {e}")

        logging.info("ELO update script completed successfully.")

    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
        exit(1)

if __name__ == "__main__":
    main()
