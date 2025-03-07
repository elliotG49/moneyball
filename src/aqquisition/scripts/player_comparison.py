#!/usr/bin/env python3
import argparse
import yaml
from pymongo import MongoClient

def load_template(template_path):
    with open(template_path, 'r') as f:
        return yaml.safe_load(f)

def get_nested_value(document, field_path):
    """
    Retrieve a nested value from the document given a dot-separated field_path.
    For example, if field_path is 'detailed.interceptions_per_90_overall', this will return
    document['detailed']['interceptions_per_90_overall'] if it exists.
    """
    keys = field_path.split('.')
    value = document
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None
    return value

def main():
    parser = argparse.ArgumentParser(description="Player Comparison Script")
    parser.add_argument("--ids", required=True,
                        help="Comma separated list of player IDs to compare")
    parser.add_argument("--template", required=True,
                        help="Path to the YAML template file")
    parser.add_argument("--comp_id",
                        help="Comma separated list of competition IDs corresponding to each player")
    parser.add_argument("--season",
                        help="Comma separated list of season strings (e.g. '2023/2024') corresponding to each player")
    args = parser.parse_args()
    
    # Ensure that exactly one of --comp_id or --season is provided.
    if (args.comp_id and args.season) or (not args.comp_id and not args.season):
        print("Error: Please provide either --comp_id or --season (but not both).")
        return

    # Convert the string IDs to integers for correct type matching in MongoDB
    ids = [int(pid.strip()) for pid in args.ids.split(',')]
    
    # Load the YAML template file
    template = load_template(args.template)
    
    # Connect to MongoDB (update the URI and database name as needed)
    client = MongoClient("mongodb://localhost:27017")
    db = client["footballDB"]  # Use your actual database name
    
    # Mode: Competition mode using --comp_id
    if args.comp_id:
        comp_ids = [int(cid.strip()) for cid in args.comp_id.split(',')]
        if len(ids) != len(comp_ids):
            print("Error: The number of player IDs must match the number of competition IDs.")
            return
        
        for index, pid in enumerate(ids):
            current_comp_id = comp_ids[index]
            print(f"--- Player ID: {pid} ---")
            # Build a query using both the player id and competition_id.
            query = {"id": pid, "competition_id": current_comp_id}
            player_doc = db["players"].find_one(query)
            
            if not player_doc:
                print("No player found for this id/competition combination.\n")
                continue
            
            # Print season and known_as from the document.
            season = player_doc.get("season", "N/A")
            print(f"Season: {season}")
            known_as = player_doc.get("known_as", "N/A")
            print(f"Known as: {known_as}")
            
            # Loop through each datapoint specified in the YAML template
            for friendly_name, field_spec in template.items():
                # Use only the field path part (ignoring any extra flags separated by a comma)
                if ',' in field_spec:
                    field_path, _ = field_spec.split(',', 1)
                else:
                    field_path = field_spec
                
                value = get_nested_value(player_doc, field_path)
                print(f"{friendly_name}: {value}")
            print()
    
    # Mode: Season mode using --season
    else:
        seasons = [s.strip() for s in args.season.split(',')]
        if len(ids) != len(seasons):
            print("Error: The number of player IDs must match the number of seasons provided.")
            return
        
        for index, pid in enumerate(ids):
            current_season = seasons[index]
            print(f"--- Player ID: {pid} ---")
            # Build a query that gathers all documents for the given player and season
            query = {"id": pid, "season": current_season}
            docs = list(db["players"].find(query))
            
            if not docs:
                print("No player found for this id/season combination.\n")
                continue
            
            # Filter out competitions where minutes_played_overall is less than 200
            valid_docs = [doc for doc in docs if doc.get("minutes_played_overall", 0) >= 200]
            if not valid_docs:
                print("No competitions with at least 200 minutes played for this player/season combination.\n")
                continue
            
            # Use the first valid document to retrieve known fields
            season_from_doc = valid_docs[0].get("season", current_season)
            known_as = valid_docs[0].get("known_as", "N/A")
            print(f"Season: {season_from_doc}")
            print(f"Known as: {known_as}")
            
            # For each datapoint, compute a weighted average based on minutes_played_overall
            for friendly_name, field_spec in template.items():
                if ',' in field_spec:
                    field_path, _ = field_spec.split(',', 1)
                else:
                    field_path = field_spec
                
                weighted_sum = 0.0
                total_minutes = 0.0
                for doc in valid_docs:
                    val = get_nested_value(doc, field_path)
                    minutes = doc.get("minutes_played_overall", 0)
                    if isinstance(val, (int, float)) and minutes:
                        weighted_sum += val * minutes
                        total_minutes += minutes
                if total_minutes > 0:
                    weighted_avg = weighted_sum / total_minutes
                else:
                    weighted_avg = None
                print(f"{friendly_name}: {weighted_avg:.3}")
            print()

if __name__ == "__main__":
    main()
