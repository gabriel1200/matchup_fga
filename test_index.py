import pandas as pd
import glob
import os
import numpy as np  # Added for conditional logic

# --- Paths ---
regular_path = '../../web_app/data/pbp/regular_season/*.parquet'
playoffs_path = '../../web_app/data/pbp/playoffs/*.parquet'
game_dates_path = 'https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv'  # Assumed to be in the same directory

columns = [
    'teamId', 'actionNumber', 'scoreHome', 'scoreAway',
    'game_id', 'playoffs', 'team'
]

def extract_year(filename):
    """
    Example filename: ATL_2021_rs.parquet
    Splits by '_' and returns second token => '2021'
    """
    base = os.path.basename(filename)
    parts = base.replace(".parquet", "").split("_")
    return parts[1]  # second token

def load_game_metadata(csv_path):
    """
    Loads game_dates.csv and creates a mapping of GAME_ID -> (home_team_id, away_team_id).
    """

    df = pd.read_csv(csv_path)
    
    # Identify Home Team ID: Row where team abbreviation matches HTM (Home Team)
    home_map = df[df['team'] == df['HTM']][['GAME_ID', 'TEAM_ID']]
    home_map = home_map.rename(columns={'TEAM_ID': 'home_team_id'})
    
    # Identify Away Team ID: Row where team abbreviation matches VTM (Visitor Team)
    away_map = df[df['team'] == df['VTM']][['GAME_ID', 'TEAM_ID']]
    away_map = away_map.rename(columns={'TEAM_ID': 'away_team_id'})
    
    # Merge to get one row per game with both IDs
    game_map = pd.merge(home_map, away_map, on='GAME_ID', how='outer')
    
    # Ensure GAME_ID is int for merging
    game_map['GAME_ID'] = game_map['GAME_ID'].astype(int)
    
    return game_map

def save_by_season(files, base_output_dir, columns, game_map):
    """
    Load all parquet files, group by year, save each year's combined df.
    Now includes score_margin calculation.
    """
    season_dfs = {}

    for f in files:
        try:
            df = pd.read_parquet(f)
            # print(df.columns) # Optional: reduce clutter
            shots = ['3pt', '2pt']
            df = df[df['actionType'].isin(shots)]
            
            year = extract_year(f)
            df['source_file'] = os.path.basename(f)

            if year not in season_dfs:
                season_dfs[year] = []
            season_dfs[year].append(df)

        except Exception as e:
            print(f"Error reading file {f}: {e}")

    # Save each season separately
    for year, df_list in season_dfs.items():
        # Concatenate and select initial columns
        combined = pd.concat(df_list, ignore_index=True)[columns]

        # --- START NEW LOGIC ---
        if not game_map.empty:
            # 1. Ensure types match for merge. PBP game_id is often string, map is int.
            combined['game_id_numeric'] = pd.to_numeric(combined['game_id'], errors='coerce').fillna(-1).astype(int)
            
            # 2. Merge Game Metadata
            combined = combined.merge(game_map, left_on='game_id_numeric', right_on='GAME_ID', how='left')
            
            # 3. Ensure scores are numeric
            combined['scoreHome'] = pd.to_numeric(combined['scoreHome'], errors='coerce').fillna(0)
            combined['scoreAway'] = pd.to_numeric(combined['scoreAway'], errors='coerce').fillna(0)

            # 4. Calculate Score Margin for the Shooter's Team
            # Logic:
            #   - If shooter's teamId == home_team_id -> Margin = scoreHome - scoreAway
            #   - If shooter's teamId == away_team_id -> Margin = scoreAway - scoreHome
            #   - Else (unknown) -> NaN
            
            conditions = [
                combined['teamId'] == combined['home_team_id'],
                combined['teamId'] == combined['away_team_id']
            ]
            
            choices = [
                combined['scoreHome'] - combined['scoreAway'],
                combined['scoreAway'] - combined['scoreHome']
            ]
            
            combined['score_margin'] = np.select(conditions, choices, default=np.nan)

            # 5. Clean up helper columns
            combined.drop(columns=['game_id_numeric', 'GAME_ID', 'home_team_id', 'away_team_id'], inplace=True)
        else:
            combined['score_margin'] = np.nan
        # --- END NEW LOGIC ---

        # Ensure directory exists
        out_dir = os.path.join(base_output_dir, year)
        os.makedirs(out_dir, exist_ok=True)

        out_path = os.path.join(out_dir, f"{year}_combined.csv")
        combined.sort_values(by=['game_id', 'actionNumber'], inplace=True)
        
        # Debug prints
        print(combined.head(1))
        print(combined.columns)
        
        combined.to_csv(out_path, index=False)

        print(f"Saved {year} -> {out_path} ({len(combined)} rows)")

# --- Main Execution ---

# 1. Load the Game Date Metadata first
game_map = load_game_metadata(game_dates_path)
print(f"Loaded metadata for {len(game_map)} games.")

# 2. Process Regular Season
regular_files = glob.glob(regular_path)
save_by_season(
    regular_files,
    base_output_dir='indexing/regular_season',
    columns=columns,
    game_map=game_map
)

# 3. Process Playoffs
playoff_files = glob.glob(playoffs_path)
save_by_season(
    playoff_files,
    base_output_dir='indexing/playoffs',
    columns=columns,
    game_map=game_map
)