import argparse
import pymongo

def main():
    parser = argparse.ArgumentParser(description="Fetch players from the database based on search criteria.")
    parser.add_argument("--ln", type=str, help="Player's last name")
    parser.add_argument("--ka", type=str, help="Player's known_as name")
    parser.add_argument("--tid", type=int, help="Player's club team ID")
    parser.add_argument("--cid", type=int, help="Competition ID (name of the competition)")
    parser.add_argument("--age", type=int, help="Player's age")
    args = parser.parse_args()

    # Ensure at least one argument is provided
    if not (args.ln or args.ka or args.tid is not None or args.cid or args.age is not None):
        parser.error("At least one search parameter (--ln, --ka, --tid, --cid, or --age) must be provided.")

    # Build the query based on the provided arguments
    query = {}
    if args.ln:
        query["last_name"] = args.ln
    if args.ka:
        query["known_as"] = args.ka
    if args.tid is not None:
        query["club_team_id"] = args.tid
    if args.cid:
        query["competition_id"] = args.cid
    if args.age is not None:
        query["age"] = args.age

    # Connect to MongoDB
    client = pymongo.MongoClient("localhost", 27017)
    db = client.footballDB
    collection = db.players

    # Query the players collection for matching players,
    # projecting only 'id' and 'known_as' (excluding _id)
    projection = {"id": 1, "known_as": 1, "_id": 0}
    players = collection.find(query, projection)

    # Print out the matching players
    print("Matching Players:")
    for player in players:
        print(player)

if __name__ == "__main__":
    main()
