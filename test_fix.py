import pandas as pd
import numpy as np

# --- Load Files ---
combined_path = 'indexing/playoffs/2025/2025_combined.csv'
dates_path = 'game_dates.csv'

df = pd.read_csv(combined_path)
df_dates = pd.read_csv(dates_path)

# --- Create Game Map ---
# Identify Home Team ID
home_map = df_dates[df_dates['team'] == df_dates['HTM']][['GAME_ID', 'TEAM_ID']]
home_map = home_map.rename(columns={'TEAM_ID': 'home_team_id'})

# Identify Away Team ID
away_map = df_dates[df_dates['team'] == df_dates['VTM']][['GAME_ID', 'TEAM_ID']]
away_map = away_map.rename(columns={'TEAM_ID': 'away_team_id'})

# Merge to get one row per game with both IDs
game_map = pd.merge(home_map, away_map, on='GAME_ID', how='outer')
game_map['GAME_ID'] = game_map['GAME_ID'].astype(int)

# --- Apply Fix ---
# Remove the old (incomplete) score_margin column if it exists
if 'score_margin' in df.columns:
    df = df.drop(columns=['score_margin'])

# Create a numeric game_id for merging
df['game_id_numeric'] = pd.to_numeric(df['game_id'], errors='coerce').fillna(-1).astype(int)

# Merge the new map
df = df.merge(game_map, left_on='game_id_numeric', right_on='GAME_ID', how='left')

# Calculate Score Margin
# Logic: 
#   - If teamId == home_team_id -> Margin = scoreHome - scoreAway
#   - If teamId == away_team_id -> Margin = scoreAway - scoreHome
conditions = [
    df['teamId'] == df['home_team_id'],
    df['teamId'] == df['away_team_id']
]

choices = [
    df['scoreHome'] - df['scoreAway'],
    df['scoreAway'] - df['scoreHome']
]

df['score_margin'] = np.select(conditions, choices, default=np.nan)

# --- Cleanup and Save ---
df.drop(columns=['game_id_numeric', 'GAME_ID', 'home_team_id', 'away_team_id'], inplace=True)

output_filename = '2025_combined_fixed.csv'
df.to_csv(output_filename, index=False)

print(f"Fixed file saved to {output_filename}")
print(f"Rows with missing score_margin: {df['score_margin'].isnull().sum()}")