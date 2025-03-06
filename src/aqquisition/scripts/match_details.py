#!/usr/bin/env python3
import argparse
import yaml
from pymongo import MongoClient

def load_template(template_path):
    with open(template_path, 'r') as f:
        return yaml.safe_load(f)

def fetch_value(db, collection_name, query, field_name):
    doc = db[collection_name].find_one(query, {field_name: 1})
    return doc.get(field_name) if doc and field_name in doc else None

def main():
    parser = argparse.ArgumentParser(description="Match Details Script")
    parser.add_argument("--ids", required=True,
                        help="Comma separated list of match IDs to compare")
    parser.add_argument("--template", required=True,
                        help="Path to the YAML template file")
    args = parser.parse_args()
    
    ids = [mid.strip() for mid in args.ids.split(',')]
    template = load_template(args.template)
    
    client = MongoClient("mongodb://localhost:27017")
    db = client["your_database_name"]  # Replace with your actual database name
    
    for mid in ids:
        print(f"--- Match ID: {mid} ---")
        query = {"id": mid}  # No competition ID for match details
        for friendly_name, field_spec in template.items():
            parts = field_spec.split('.')
            if len(parts) < 2:
                continue
            collection_name = parts[0]
            field_and_flags = ".".join(parts[1:])
            if ',' in field_and_flags:
                field_name, _ = field_and_flags.split(',', 1)
            else:
                field_name = field_and_flags
            value = fetch_value(db, collection_name, query, field_name)
            print(f"{friendly_name}: {value}")
        print()

if __name__ == "__main__":
    main()
