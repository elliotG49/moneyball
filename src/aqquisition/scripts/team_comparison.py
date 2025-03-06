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
    parser.add_argument("--comp_id", required=True,
                        help="Comma separated list of competition IDs corresponding to each team")
    args = parser.parse_args()
    
    # Convert the string team IDs and competition IDs to integers
    ids = [int(tid.strip()) for tid in args.ids.split(',')]
    comp_ids = [int(cid.strip()) for cid in args.comp_id.split(',')]
    
    if len(ids) != len(comp_ids):
        print("Error: The number of team IDs must match the number of competition IDs.")
        return
    
    # Load the YAML template file
    template = load_template(args.template)
    
    # Connect to MongoDB (update the URI and database name as needed)
    client = MongoClient("mongodb://localhost:27017")
    db = client["footballDB"]  # Use your actual database name
    
    # Loop through each team ID with its corresponding competition ID and output the datapoints
    for index, tid in enumerate(ids):
        current_comp_id = comp_ids[index]
        print(f"--- Team ID: {tid} ---")
        # Build a query that includes both the team id and its corresponding competition id
        query = {"id": tid, "comp_id": current_comp_id}
        team_doc = db["teams"].find_one(query)
        
        if not team_doc:
            print("No team found for this id/competition combination.\n")
            continue
        
        # Print the team's "team_name" name
        team_name = team_doc.get("CleanName", "N/A")
        season = team_doc.get("season", "N/A")
        print(f"Season: {season}")
        print(f"Known as: {team_name}")
        
        
        # Loop through each datapoint specified in the YAML template
        for friendly_name, field_spec in template.items():
            # If extra flags exist (separated by a comma), only use the field path.
            if ',' in field_spec:
                field_path, _ = field_spec.split(',', 1)
            else:
                field_path = field_spec
            
            value = get_nested_value(team_doc, field_path)
            print(f"{friendly_name}: {value}")
        print()

if __name__ == "__main__":
    main()
