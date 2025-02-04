import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
import argparse
import re
import time  # optional: to avoid hammering Wikipedia too quickly

from pymongo import MongoClient
import pandas as pd
from sklearn.preprocessing import MinMaxScaler  # or StandardScaler

# -----------------------------
# Command-line Arguments
# -----------------------------
parser = argparse.ArgumentParser(description="Football player analysis with optional position web scraping.")
parser.add_argument('-p', '--position', action='store_true',
                    help='If provided, fetch player positions from Wikipedia using web scraping.')
args = parser.parse_args()

# -----------------------------
# Web Scraper Functions
# -----------------------------
def get_wikipedia_player_page(full_name):
    # Replace spaces with underscores and URL-encode the full name.
    slug = quote(full_name.replace(" ", "_"))
    url = f"https://en.wikipedia.org/wiki/{slug}"
    return url

def extract_playing_position(html_content):
    soup = BeautifulSoup(html_content, "lxml")
    # Find the infobox table (it typically has class 'infobox vcard')
    infobox = soup.find("table", {"class": "infobox"})
    if not infobox:
        return None
    # Loop through the rows looking for the "Playing position" or similar key.
    for row in infobox.find_all("tr"):
        header = row.find("th")
        if header and "position" in header.text.lower():
            cell = row.find("td")
            if cell:
                # Remove all <sup> tags (typically used for references)
                for sup in cell.find_all("sup"):
                    sup.decompose()
                position_text = cell.get_text(separator=", ").strip()
                # Clean up the position text by removing extra commas or spaces.
                position_text = re.sub(r"\s*,\s*", ", ", position_text)
                position_text = re.sub(r",\s*,", ",", position_text)
                # Split the text by commas and filter out any empty strings.
                positions = [pos.strip() for pos in position_text.split(",") if pos.strip()]
                if len(positions) > 1:
                    return ", ".join(positions)
                return positions[0]
    return None

def get_player_position(full_name):
    url = get_wikipedia_player_page(full_name)
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code} for URL: {url}")
        return None
    position = extract_playing_position(response.content)
    return position

# -----------------------------
# MongoDB Configuration
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "footballDB"
PLAYERS_COLLECTION_NAME = "players"
TEAMS_COLLECTION_NAME = "teams"
COMPETITION_ID = 12325

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]

players_collection = db[PLAYERS_COLLECTION_NAME]
teams_collection = db[TEAMS_COLLECTION_NAME]

# -----------------------------
# Teams Query
# -----------------------------
teams_query = {"competition_id": COMPETITION_ID}
teams_projection = {"stats.seasonMatchesPlayed_overall": 1, "stats.possessionAVG_overall": 1, "id": 1, "_id": 0, "name": 1}

# Fetch teams data
teams_cursor = teams_collection.find(teams_query, teams_projection)
team_amount = teams_collection.count_documents(teams_query)

# Build dictionaries for team stats
team_matches_dict = {}
team_possession = {}
team_names_dict = {}
total_possession = 0
for team in teams_cursor:
    team_id = team.get('id')
    matches_played = team.get('stats', {}).get('seasonMatchesPlayed_overall', 0)
    possession = team.get('stats', {}).get('possessionAVG_overall', 0)
    total_possession += possession
    if team_id is not None:
        team_matches_dict[team_id] = matches_played
        team_possession[team_id] = possession
        team_names_dict[team_id] = team.get("name", "Unknown")
        
average_possesion = total_possession / team_amount

print("Team Matches Dictionary:", team_matches_dict)

# -----------------------------
# Players Query
# -----------------------------
# Set parameters for file paths and position
position = 'Forward'
file_name = 'winger'

# Load metrics with their indicators, importance, and team_style_induced flag from CSV
metrics_df = pd.read_csv(f'/root/moneyball/data/profiles/{position}/metrics/{file_name}_metrics.csv')
metrics_df = metrics_df.dropna(subset=['metric_name'])  # Ensure no NaN metric names
metrics = list(metrics_df['metric_name'])
print("Loaded Metrics:", metrics)

# Create dictionaries from CSV:
# Indicator dictionary for inversion
metric_indicator_dict = metrics_df.set_index('metric_name')['indicator'].to_dict()
print("Metric Indicator Dictionary:", metric_indicator_dict)
# Importance dictionary
metric_importance_dict = metrics_df.set_index('metric_name')['importance'].to_dict()
# Team style induced flag dictionary: convert to boolean and remove "detailed." prefix if present.
team_style_induced_dict = {}
for idx, row in metrics_df.iterrows():
    m_name = row['metric_name']
    flag = row['team_style_induced']
    if isinstance(flag, str):
        flag = flag.strip().lower() == "true"
    else:
        flag = bool(flag)
    key = m_name.split('.', 1)[1] if m_name.startswith('detailed.') else m_name
    team_style_induced_dict[key] = flag

# Construct Projection Dictionary for MongoDB query
projection_dict = {metric: 1 for metric in metrics}  # Include nested metrics
projection_dict.update({
    "_id": 0,
    "appearances_overall": 1,
    "known_as": 1,
    "id": 1,
    "club_team_id": 1
})
print("Projection Dictionary:", projection_dict)

# Define Players Query
players_query = {"competition_id": COMPETITION_ID, "position": position}

# Execute Players Query
player_metrics_cursor = players_collection.find(players_query, projection_dict)
player_metrics = list(player_metrics_cursor)
print(f"Total Players Retrieved: {len(player_metrics)}")

# -----------------------------
# Filter Players Based on Appearances
# -----------------------------
confirmed_players = []
for player in player_metrics:
    club_id = player.get('club_team_id')
    appearances = player.get('appearances_overall', 0)
    known_as = player.get('known_as', 'Unknown Player')
    if club_id in team_matches_dict:
        total_matches = team_matches_dict[club_id]
        if total_matches == 0:
            print(f"Team ID {club_id} has zero matches. Skipping player {known_as}.")
            continue  # Avoid division by zero
        half_matches = total_matches / 2
        print(f"Player: {known_as}, Appearances: {appearances}, Team ID: {club_id}, Team Matches: {total_matches}, Half Matches: {half_matches}")
        if appearances >= half_matches:
            confirmed_players.append(player)
    else:
        print(f"Club ID {club_id} not found in teams data. Skipping player {known_as}.")

print(f"Total Confirmed Players: {len(confirmed_players)}")

# -----------------------------
# Build Enhanced DataFrame with Metrics
# -----------------------------
confirmed_players_data = []
for player in confirmed_players:
    known_as = player.get('known_as', 'Unknown Player')
    minutes = player.get('minutes_played_overall', 0)
    club_id = player.get('club_team_id')
    player_id = player.get('id')
    
    player_data = {
        'Player': known_as,
        'Minutes Played': minutes,
        'Club ID': club_id,
        'Player ID': player_id,
        # Add the team name from the teams query
        'Team Name': team_names_dict.get(club_id, "Unknown")
    }
    
    detailed = player.get('detailed', {})
    for metric in metrics:
        if metric.startswith('detailed.'):
            metric_key = metric.split('.', 1)[1]  # Remove 'detailed.' prefix
            metric_value = detailed.get(metric_key, None)
            player_data[metric_key] = metric_value
        else:
            metric_key = metric
            metric_value = player.get(metric_key, None)
            player_data[metric_key] = metric_value
    confirmed_players_data.append(player_data)

# Create DataFrame
confirmed_players_df = pd.DataFrame(confirmed_players_data)
print("Confirmed Players DataFrame with Metrics:")
print(confirmed_players_df)

# -----------------------------
# Retrieve or Set Player Positions
# -----------------------------
positions = []
if args.position:
    # Fetch positions using the web scraper
    for name in confirmed_players_df['Player']:
        print(f"Fetching position for {name}...")
        pos = get_player_position(name)
        if pos is None:
            pos = "Unknown"
        positions.append(pos)
        # time.sleep(0.3)  # adjust delay if needed
else:
    # If the -p flag is not set, use a default value
    positions = ["Not Fetched"] * len(confirmed_players_df)

# Insert the new "Position" column right after "Player"
confirmed_players_df.insert(1, "Position", positions)
print("DataFrame after adding Position column:")
print(confirmed_players_df[['Player', 'Position', 'Appearances']].head())

# -----------------------------
# Add Team Possession to the DataFrame
# -----------------------------
# Map each player's Club ID to its team possession and add it as a new column.
confirmed_players_df['Team Possession'] = confirmed_players_df['Club ID'].map(team_possession)

# -----------------------------
# Normalize Metrics (Min-Max Scaling)
# -----------------------------
# Identify metric columns (excluding 'Player', 'Position', 'Appearances', 'Club ID', 'Player ID', 'Team Name', and 'Team Possession')
metric_columns = [col for col in confirmed_players_df.columns 
                 if col not in ['Player', 'Position', 'Appearances', 'Club ID', 'Player ID', 'Team Name', 'Team Possession']]
confirmed_players_df[metric_columns] = confirmed_players_df[metric_columns].fillna(0)  # Fill missing values with 0

scaler = MinMaxScaler()
confirmed_players_df[metric_columns] = scaler.fit_transform(confirmed_players_df[metric_columns])
print("Normalized Metrics (Min-Max Scaling):")
print(confirmed_players_df[metric_columns].head())

# -----------------------------
# Adjust Metrics Based on Negative Indicators
# -----------------------------
# For each metric in our list, if its indicator is 'negative' we invert its value.
for metric in metrics:
    indicator = metric_indicator_dict.get(metric, 'positive')  # Default to 'positive'
    if metric.startswith('detailed.'):
        metric_key = metric.split('.', 1)[1]
        if indicator.lower() == 'negative':
            confirmed_players_df[metric_key] = confirmed_players_df[metric_key] * -1
            print(f"Inverted metric: {metric_key}")
    else:
        if indicator.lower() == 'negative':
            confirmed_players_df[metric] = confirmed_players_df[metric] * -1
            print(f"Inverted metric: {metric}")

# -----------------------------
# Calculate the Weighted Composite Score
# -----------------------------
# Map the 'importance' values to numeric weights.
importance_map = {"low": 1, "medium": 2, "high": 3}
# Build a mapping from the DataFrame column name (used for metrics) to its weight.
metric_weight_mapping = {}
for metric_name, imp in metric_importance_dict.items():
    weight = importance_map.get(imp.strip().lower(), 1)
    key = metric_name.split('.', 1)[1] if metric_name.startswith('detailed.') else metric_name
    metric_weight_mapping[key] = weight

print("Metric Weight Mapping:", metric_weight_mapping)

# Calculate the composite score using these weights.
# For metrics flagged as team-style induced, adjust by the team possession multiplier.
def compute_weighted_score(row):
    score = 0
    club_id = row['Club ID']
    # Compute the team possession multiplier: (team possession / league average)
    team_mult = team_possession.get(club_id, average_possesion) / average_possesion
    for metric, weight in metric_weight_mapping.items():
        value = row.get(metric, 0)
        # If the metric is team-style induced, modify it by the possession multiplier.
        if team_style_induced_dict.get(metric, False):
            value *= team_mult
        score += value * weight
    return score

confirmed_players_df['Composite_Score'] = confirmed_players_df.apply(compute_weighted_score, axis=1)
print("Composite Scores:")
print(confirmed_players_df[['Player', 'Composite_Score']].head())

# -----------------------------
# Sort Players by Composite Score in Descending Order
# -----------------------------
sorted_players_df = confirmed_players_df.sort_values(by='Composite_Score', ascending=False).reset_index(drop=True)
print("Players Sorted by Composite Score:")
print(sorted_players_df[['Player', 'Team Name', 'Position', 'Composite_Score', 'Team Possession']].head())

# -----------------------------
# (Optional) Save the Enhanced and Sorted DataFrame
# -----------------------------
sorted_players_df.to_csv(f'/root/moneyball/data/profiles/{position}/scores/{file_name}_scores.csv', index=False)
sorted_players_df.to_excel(f'/root/moneyball/data/profiles/{position}/scores/{file_name}_scores.xlsx', index=False)
