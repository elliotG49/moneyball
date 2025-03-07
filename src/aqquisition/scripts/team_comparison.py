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
    For example, if field_path is 'detailed.clearances_per_90_overall', this will return
    document['detailed']['clearances_per_90_overall'] if it exists.
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
    parser = argparse.ArgumentParser(description="Team Comparison Script")
    parser.add_argument("--ids", required=True,
                        help="Comma separated list of team IDs to compare")
    parser.add_argument("--template", required=True,
                        help="Path to the YAML template file")
    parser.add_argument("--comp_id",
                        help="Comma separated list of competition IDs corresponding to each team")
    parser.add_argument("--season",
                        help="Comma separated list of season strings (e.g. '2023/2024') corresponding to each team")
    args = parser.parse_args()
    
    # Ensure that exactly one of --comp_id or --season is provided.
    if (args.comp_id and args.season) or (not args.comp_id and not args.season):
        print("Error: Please provide either --comp_id or --season (but not both).")
        return

    # Convert team IDs to integers for correct type matching in MongoDB
    ids = [int(tid.strip()) for tid in args.ids.split(',')]
    
    # Load the YAML template file
    template = load_template(args.template)
    
    # Connect to MongoDB (update the URI and database name as needed)
    client = MongoClient("mongodb://localhost:27017")
    db = client["footballDB"]  # Use your actual database name
    
    # Competition mode using --comp_id
    if args.comp_id:
        comp_ids = [int(cid.strip()) for cid in args.comp_id.split(',')]
        if len(ids) != len(comp_ids):
            print("Error: The number of team IDs must match the number of competition IDs.")
            return
        
        for index, tid in enumerate(ids):
            current_comp_id = comp_ids[index]
            print(f"--- Team ID: {tid} ---")
            # Query team document for the given id and competition
            query = {"id": tid, "competition_id": current_comp_id}
            team_doc = db["teams"].find_one(query)
            
            if not team_doc:
                print("No team found for this id/competition combination.\n")
                continue
            
            team_name = team_doc.get("cleanName", "N/A")
            season = team_doc.get("season", "N/A")
            print(f"Season: {season}")
            print(f"Known as: {team_name}")
            
            # Output each datapoint specified in the YAML template
            for friendly_name, field_spec in template.items():
                # Use only the field path part (ignoring extra flags after a comma)
                if ',' in field_spec:
                    field_path, _ = field_spec.split(',', 1)
                else:
                    field_path = field_spec
                
                value = get_nested_value(team_doc, field_path)
                print(f"{friendly_name}: {value}")
            print()
    
    # Season mode using --season
    else:
        seasons = [s.strip() for s in args.season.split(',')]
        if len(ids) != len(seasons):
            print("Error: The number of team IDs must match the number of seasons provided.")
            return
        
        for index, tid in enumerate(ids):
            current_season = seasons[index]
            print(f"--- Team ID: {tid} ---")
            # Query all team documents for the given id and season
            query = {"id": tid, "season": current_season}
            docs = list(db["teams"].find(query))
            
            if not docs:
                print("No team found for this id/season combination.\n")
                continue
            
            # Use the first document for common fields
            team_name = docs[0].get("cleanName", "N/A")
            season_from_doc = docs[0].get("season", current_season)
            print(f"Season: {season_from_doc}")
            print(f"Known as: {team_name}")
            
            # For each datapoint, average its numeric value across all documents
            for friendly_name, field_spec in template.items():
                if ',' in field_spec:
                    field_path, _ = field_spec.split(',', 1)
                else:
                    field_path = field_spec
                
                values = []
                for doc in docs:
                    val = get_nested_value(doc, field_path)
                    if isinstance(val, (int, float)):
                        values.append(val)
                if values:
                    avg_val = sum(values) / len(values)
                else:
                    avg_val = None
                print(f"{friendly_name}: {avg_val}")
            print()

if __name__ == "__main__":
    main()
