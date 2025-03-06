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
    parser.add_argument("--comp_id", required=True,
                        help="Comma separated list of competition IDs corresponding to each player")
    args = parser.parse_args()
    
    # Convert the string IDs to integers for correct type matching in MongoDB
    ids = [int(pid.strip()) for pid in args.ids.split(',')]
    comp_ids = [int(cid.strip()) for cid in args.comp_id.split(',')]
    
    if len(ids) != len(comp_ids):
        print("Error: The number of player IDs must match the number of competition IDs.")
        return
    
    # Load the YAML template file
    template = load_template(args.template)
    
    # Connect to MongoDB (update the URI and database name as needed)
    client = MongoClient("mongodb://localhost:27017")
    db = client["footballDB"]  # Use your actual database name
    
    # Loop through each player ID with its corresponding competition ID and output the datapoints
    for index, pid in enumerate(ids):
        current_comp_id = comp_ids[index]
        print(f"--- Player ID: {pid} ---")
        # Build a query that includes both the player id and its corresponding competition id
        query = {"id": pid, "competition_id": current_comp_id}
        player_doc = db["players"].find_one(query)
        
        if not player_doc:
            print("No player found for this id/competition combination.\n")
            continue
        
        # Print the player's "known_as" field
        known_as = player_doc.get("known_as", "N/A")
        print(f"Known as: {known_as}")
        
        # Loop through each datapoint specified in the YAML template
        for friendly_name, field_spec in template.items():
            # Allow extra flags in the YAML (separated by comma) but only use the field path
            if ',' in field_spec:
                field_path, _ = field_spec.split(',', 1)
            else:
                field_path = field_spec
            
            value = get_nested_value(player_doc, field_path)
            print(f"{friendly_name}: {value}")
        print()

if __name__ == "__main__":
    main()
