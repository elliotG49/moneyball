import logging
import time  # To get current Unix timestamp
from pymongo import MongoClient

def setup_logging():
    """Configure logging settings."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

def update_latest_team_elos(db):
    """Update latest ELO rating and timestamp for each team in the teams collection."""
    logging.info(f"Starting latest team ELO update process...")

    matches_collection = db["matches"]
    teams_collection = db["teams"]

    # Find all teams in the teams collection
    all_teams = teams_collection.distinct("id")
    
    if not all_teams:
        logging.warning("No teams found in the database.")
        return

    logging.info(f"Found {len(all_teams)} teams in the database.")

    # Get current Unix timestamp
    current_time = int(time.time())

    # Process each team
    for team_id in all_teams:
        recent_match = matches_collection.find_one(
            {
                "$or": [{"homeID": team_id}, {"awayID": team_id}],
                "status": "complete"
            },
            sort=[("date_unix", -1)]
        )

        if not recent_match:
            logging.warning(f"No completed matches found for team {team_id}. Skipping.")
            continue

        # Extract latest ELO
        latest_elo = recent_match.get("home_elo_pre_match") if recent_match["homeID"] == team_id else recent_match.get("away_elo_pre_match")
        if latest_elo is None:
            logging.warning(f"No ELO data found for team {team_id} in most recent match. Skipping.")
            continue

        # Update the teams collection with latest ELO and timestamp
        update_result = teams_collection.update_one(
            {"id": team_id},  # Match based on team ID
            {"$set": {
                "latest_elo": latest_elo,
                "elo_last_updated": current_time
            }}
        )

        if update_result.modified_count > 0:
            logging.info(f"Updated latest ELO for team {team_id}: {latest_elo} (Updated at {current_time})")

    logging.info(f"Finished updating latest team ELOs.")

def main():
    """Main function to update team ELOs."""
    setup_logging()

    try:
        # Connect to MongoDB
        MONGO_URI = "mongodb://localhost:27017/"
        DATABASE_NAME = "footballDB"
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]

        # Update latest ELOs for teams
        update_latest_team_elos(db)

    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")
        exit(1)

if __name__ == "__main__":
    main()
