from pymongo import MongoClient, DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure

def query_matches():
    try:
        # 1. Establish a Connection to MongoDB
        # Replace the URI string with your MongoDB deployment's connection string.
        # For a local MongoDB instance:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        
        # Attempt to connect to the server to trigger potential connection errors
        client.admin.command('ping')
        print("Connected to MongoDB successfully.")
        
        # 2. Access the Database and Collection
        db = client['footballDB']      # Replace with your database name
        collection = db['matches']             # Access the 'matches' collection
        TEAM_ID = [
            152,
            59,
            143,
            144,
            271,
            251,
            153,
            93,
            145,
            218,
            223,
            209,
            151,
            149,
            162,
            211,
            92, 
            148,
            158,
            157
]
        for ids in TEAM_ID:
            # 3. Define the Query Filter
            query_filter_1 = {
                "homeID": ids,
                "competition_id": 9660
            }
            
            query_filter_2 = {
                "awayID": ids,
                "competition_id": 9660
            }
            
            projection_2 = {
                "away_elo_pre_match": 1,
                "game_week": 1,
                "away_name": 1,
                "_id": 0
            }

            projection_1 = {
                "home_elo_pre_match": 1,
                "game_week": 1,
                "home_name": 1,
                "_id": 0
            }
            
            
            # 5. Define the Sort Order
            sort_order = [("game_week", DESCENDING)]  # Sort by 'game_week' descending
            
            # 6. Execute the Query
            results_home = collection.find(query_filter_1, projection_1).sort(sort_order)
            results_home = list(results_home)
            results_away = collection.find(query_filter_2, projection_2).sort(sort_order)
            results_away = list(results_away)
            
            combined_matches = results_home + results_away
            
            combined_matches_sorted = sorted(combined_matches, key=lambda x: x['game_week'], reverse=False)
            # 9. Store Results in a Single Dictionary Key

            # 10. Display the Results
            team_name = [match.get('home_name') for match in combined_matches_sorted]
            elos = [match.get('home_elo_pre_match') or match.get('away_elo_pre_match') for match in combined_matches_sorted]
            print(team_name)
            print(elos)

            
        
    except ConnectionFailure:
        print("Failed to connect to MongoDB. Please check your connection settings.")
    except OperationFailure as e:
        print(f"MongoDB operation failed: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # 8. Close the Connection
        client.close()
        print("MongoDB connection closed.")

if __name__ == "__main__":
    query_matches()
