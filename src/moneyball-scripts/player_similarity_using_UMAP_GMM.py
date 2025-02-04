import pymongo
import pandas as pd
import numpy as np
import umap.umap_ as umap
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.mixture import GaussianMixture

# --- 1. Connect to MongoDB and Query Players ---
client = pymongo.MongoClient('mongodb://localhost:27017')
db = client.footballDB
players_collection = db.players

# Competition IDs for Premier League, Ligue 1, Bundesliga 1
competition_ids = [12325, 12337, 12529]

# Query only forwards from these competitions
query = {"competition_id": {"$in": competition_ids}, "position": "Forward"}
cursor = players_collection.find(query)

# --- 2. Define Required Metrics and Their Aspects ---
# List of all required metrics (from your CSV)
metrics = [
    "detailed.accurate_crosses_per_90_overall",
    "detailed.key_passes_per_90_overall",
    "assists_per_90_overall",
    "detailed.pass_completion_rate_overall",
    "npg_per_90",
    "detailed.shot_accuraccy_percentage_overall",
    "detailed.shot_conversion_rate_overall",
    "over_under_perform",
    "detailed.dribbles_successful_per_90_overall",
    "detailed.dispossesed_per_90_overall",
    "detailed.fouls_drawn_per_90_overall",
    "detailed.offsides_per_90_overall",
    "detailed.tackles_per_90_overall",
    "detailed.fouls_committed_per_90_overall",
    "detailed.dribbled_past_per_game_overall",
    "minutes_played_overall"
]

# Map metrics to aspects (and note whether they are positive or negative)
aspect_metrics = {
    "passing_ability": [
        "detailed.accurate_crosses_per_90_overall",
        "detailed.key_passes_per_90_overall",
        "assists_per_90_overall",
        "detailed.pass_completion_rate_overall",

    ],
    "finishing_ability": [
        "npg_per_90",
        "detailed.shot_accuraccy_percentage_overall",
        "detailed.shot_conversion_rate_overall",
        "over_under_perform",

    ],
    "on_the_ball_ability": [
        "detailed.dribbles_successful_per_90_overall",
        "detailed.dispossesed_per_90_overall",  # negative indicator
        "detailed.fouls_drawn_per_90_overall",

    ],
    "off_the_ball_ability": [
        "detailed.offsides_per_90_overall",      # negative indicator
        "detailed.tackles_per_90_overall",
        "detailed.fouls_committed_per_90_overall",  # negative indicator
        "detailed.dribbled_past_per_game_overall",

    ]
}

# For adjusting the sign (positive: +1, negative: -1)
metric_sign = {
    "detailed.accurate_crosses_per_90_overall": 1,
    "detailed.key_passes_per_90_overall": 1,
    "assists_per_90_overall": 1,
    "detailed.pass_completion_rate_overall": 1,
    "npg_per_90": 1,
    "detailed.shot_accuraccy_percentage_overall": 1,
    "detailed.shot_conversion_rate_overall": 1,
    "over_under_perform": 1,
    "detailed.dribbles_successful_per_90_overall": 1,
    "detailed.dispossesed_per_90_overall": -1,
    "detailed.fouls_drawn_per_90_overall": 1,
    "detailed.offsides_per_90_overall": -1,
    "detailed.tackles_per_90_overall": 1,
    "detailed.fouls_committed_per_90_overall": -1,
    "detailed.dribbled_past_per_game_overall": -1,
    "minutes_played_overall": 1
}

# --- 3. Helper Function to Retrieve Nested Values ---
def get_nested_value(doc, key):
    """Traverse a document using dot notation and return the value (or None if missing)."""
    keys = key.split('.')
    value = doc
    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return None
    return value

# --- 4. Load Players into a DataFrame and Filter ---
players_data = []
for player in cursor:
    # Ensure all required metrics and age are present (assume player's age is stored under "age")
    if any(get_nested_value(player, m) is None for m in metrics) or player.get('age') is None:
        continue
    record = {
        "id": player.get("id"),
        "competition_id": player.get("competition_id"),
        "position": player.get("position"),
        "age": player.get("age"),
        "Player_Name": player.get("known_as", "Unknown Player"),
        "club_id": player.get("club_team_id", "Unknown Player"),
        "minutes": player.get("minutes_played_overall", 0)
        
    }
    # Add each metric value
    for m in metrics:
        record[m] = get_nested_value(player, m)
    players_data.append(record)

df = pd.DataFrame(players_data)
df.dropna(inplace=True)  # Drop any rows with missing values, just to be safe
print("DataFrame shape after filtering:", df.shape)

# --- 5. Encode Age ---
def encode_age(age):
    if age < 24:
        return 3
    elif 24 <= age < 29:
        return 2
    else:
        return 1

df['age_encoded'] = df['age'].apply(encode_age)

# --- 6. Compute Aspect Scores and Overall Score ---
for aspect, feats in aspect_metrics.items():
    col_name = aspect + "_score"
    df[col_name] = 0
    for feat in feats:
        df[col_name] += df[feat] * metric_sign[feat]

# Overall score as average of the four aspect scores
df['overall_score'] = (
    df['passing_ability_score'] +
    df['finishing_ability_score'] +
    df['on_the_ball_ability_score'] +
    df['off_the_ball_ability_score']
) / 4

# --- 7. UMAP Embedding for Each Aspect ---
# We'll mimic the research approach by scaling positive and negative features separately
def aspect_umap(df, positive_features, negative_features):
    # Subset the data for this aspect
    data_subset = df[positive_features + negative_features].copy()
    
    # Scale positive features with StandardScaler
    pos_scaler = StandardScaler()
    pos_scaled = pos_scaler.fit_transform(data_subset[positive_features])
    
    # Scale negative features with MinMaxScaler, then invert (1 - value)
    neg_scaler = MinMaxScaler()
    neg_scaled = neg_scaler.fit_transform(data_subset[negative_features])
    neg_scaled_inverted = 1 - neg_scaled
    
    # Compute weighted scores: use the mean of positive and negative parts, then multiply them
    positive_scores = pos_scaled.mean(axis=1)
    negative_scores = neg_scaled_inverted.mean(axis=1)
    weighted_scores = positive_scores * negative_scores
    weighted_scores = weighted_scores.reshape(-1, 1)
    
    # Concatenate the scaled positive features, scaled negative features, and the weighted score
    features_for_umap = np.concatenate((pos_scaled, neg_scaled_inverted, weighted_scores), axis=1)
    
    # Apply UMAP to reduce to 2 dimensions
    reducer = umap.UMAP(random_state=42)
    umap_embedding = reducer.fit_transform(features_for_umap)
    return umap_embedding

# Define positive and negative features for each aspect based on our CSV:
# Passing and Finishing: all features are positive.
passing_pos = [
    "detailed.accurate_crosses_per_90_overall",
    "detailed.key_passes_per_90_overall",
    "assists_per_90_overall",
    "detailed.pass_completion_rate_overall",
]
finishing_pos = [
    "npg_per_90",
    "detailed.shot_accuraccy_percentage_overall",
    "detailed.shot_conversion_rate_overall",
    "over_under_perform",
]

# On-the-ball: positive features and one negative feature
on_ball_pos = [
    "detailed.dribbles_successful_per_90_overall",
    "detailed.fouls_drawn_per_90_overall",
]
on_ball_neg = [
    "detailed.dispossesed_per_90_overall"
]

# Off-the-ball: one positive feature and two negative features
off_ball_pos = [
    "detailed.tackles_per_90_overall",
    "minutes_played_overall"
]
off_ball_neg = [
    "detailed.offsides_per_90_overall",
    "detailed.fouls_committed_per_90_overall"
]

# For aspects with only positive features, simply scale them and apply UMAP
def simple_umap(df, features):
    scaler = StandardScaler()
    scaled = scaler.fit_transform(df[features])
    reducer = umap.UMAP(random_state=42)
    return reducer.fit_transform(scaled)

# Create UMAP embeddings for each aspect:
umap_passing = simple_umap(df, passing_pos)
df['UMAP_Passing1'] = umap_passing[:, 0]
df['UMAP_Passing2'] = umap_passing[:, 1]

umap_finishing = simple_umap(df, finishing_pos)
df['UMAP_Finishing1'] = umap_finishing[:, 0]
df['UMAP_Finishing2'] = umap_finishing[:, 1]

umap_on_ball = aspect_umap(df, on_ball_pos, on_ball_neg)
df['UMAP_OnBall1'] = umap_on_ball[:, 0]
df['UMAP_OnBall2'] = umap_on_ball[:, 1]

umap_off_ball = aspect_umap(df, off_ball_pos, off_ball_neg)
df['UMAP_OffBall1'] = umap_off_ball[:, 0]
df['UMAP_OffBall2'] = umap_off_ball[:, 1]

# Optionally, create an overall UMAP embedding using the four aspect scores and overall_score.
overall_features = ['passing_ability_score', 'finishing_ability_score', 'on_the_ball_ability_score', 'off_the_ball_ability_score', 'overall_score']
scaler_overall = StandardScaler()
overall_data = scaler_overall.fit_transform(df[overall_features])
umap_overall = umap.UMAP(random_state=42).fit_transform(overall_data)
df['UMAP_Overall1'] = umap_overall[:, 0]
df['UMAP_Overall2'] = umap_overall[:, 1]

# --- 8. Cluster Each UMAP Embedding Using GMM ---
def apply_gmm(umap_embedding, n_components=3):
    gmm = GaussianMixture(n_components=n_components, covariance_type='full', random_state=42)
    return gmm.fit_predict(umap_embedding)

df['Cluster_Passing'] = apply_gmm(umap_passing)
df['Cluster_Finishing'] = apply_gmm(umap_finishing)
df['Cluster_OnBall'] = apply_gmm(umap_on_ball)
df['Cluster_OffBall'] = apply_gmm(umap_off_ball)
df['Cluster_Overall'] = apply_gmm(umap_overall)

# --- 9. Final Output ---
print("Final DataFrame with UMAP embeddings and clusters:")
print(df.head())

# Optionally, save the DataFrame for further analysis
df.to_csv("/root/moneyball/data/profiles/Forward/metrics/forward_players_clustered.csv", index=False)
df.to_excel("/root/moneyball/data/profiles/Forward/metrics/forward_players_clustered.xlsx", index=False)

