import pandas as pd
import os
import numpy as np

# --- Paths ---
regular_path = '../../playindex/gameplaybyplay'

# Ensure directories exist
os.makedirs('indexing/playoffs', exist_ok=True)
os.makedirs('indexing/regular_season', exist_ok=True)

# --- 1. Load Game Dates and Build Team Map ---
# We need to know who is Home and who is Away for every Game ID to calculate margins correctly.
game_dates = pd.read_csv('game_dates.csv') # Use local copy as requested

# Create a dictionary: GAME_ID -> {'HOME_TEAM_ID': x, 'VISITOR_TEAM_ID': y}
# We assume game_dates has columns: GAME_ID, TEAM_ID, HTM (Home), VTM (Visitor)
# Since game_dates might have two rows per game (one for each team), we just need to process unique games.
game_info_map = {}

# Drop duplicates to process each game once
unique_games = game_dates.drop_duplicates(subset=['GAME_ID'])

for _, row in unique_games.iterrows():
    g_id = str(row['GAME_ID'])
    # In game_dates, 'HTM' is Home Team Abbreviation, 'VTM' is Visitor.
    # We usually need IDs. Let's try to infer IDs or map abbreviations if needed.
    # However, standard PBP uses IDs. Let's assume game_dates provides enough info 
    # or we can infer it from the PBP file later if needed. 
    # A safer bet with your specific files:
    # If game_dates has TEAM_ID, we need to know if that TEAM_ID is Home or Away.
    # We will lookup abbreviations.
    
    game_info_map[g_id] = {
        'HTM': row['HTM'],
        'VTM': row['VTM']
    }

# --- 2. Processing Loop ---
all_dfs = []

# Filter game_dates as per your original logic (years 2018-2020)
# Note: Adjust the year filtering as needed based on your data coverage
frame = game_dates.copy()
frame['year'] = frame['season'].str.split('-').str[0].astype(int) + 1
frame = frame[(frame['year'] > 2017) & (frame['year'] < 2021)]
frame = frame[['GAME_ID', 'year']].drop_duplicates()
frame['GAME_ID'] = frame['GAME_ID'].astype(str)

print(f"Processing {len(frame)} games...")

for _, row in frame.iterrows():
    game_id = row['GAME_ID']
    year = row['year']
    
    # Construct file path (adjust if your local structure is different)
    # Using the uploaded file name for testing the logic
    # In production, use: file_path = os.path.join(regular_path, f"{game_id}.csv")
    file_path = os.path.join(regular_path, f"{game_id}.csv")
   
    
    # SKIP check for this example so code runs, but keep your logic:
    # if not os.path.exists(file_path): continue
    
    try:
        # Determine Home/Away Teams for this specific game
        if game_id not in game_info_map:
            continue
            
        home_abbr = game_info_map[game_id]['HTM']
        visit_abbr = game_info_map[game_id]['VTM']

        df = pd.read_csv(file_path)
        df['playoffs'] = 1 if game_id.startswith('4') else 0
        # --- FIX: Clean and Fill Score Margin Globally First ---
        
        # 1. Handle "TIE" and convert to numeric
        # 'SCOREMARGIN' is often usually from Home Team perspective in standard NBA files
        df['SCOREMARGIN'] = df['SCOREMARGIN'].replace('TIE', '0')
        df['SCOREMARGIN'] = pd.to_numeric(df['SCOREMARGIN'], errors='coerce')
        
        # 2. Forward Fill: Carries the last known margin down to non-scoring rows (subs, fouls)
        df['SCOREMARGIN'] = df['SCOREMARGIN'].ffill()
        df['SCOREMARGIN'] = df['SCOREMARGIN'].fillna(0) # Start of game is 0
        
        # 3. Fill SCORE (Global) if needed
        df['SCORE'] = df['SCORE'].ffill()
       

        # --- Filter Garbage Rows ---
        df = df[
            (
                df['HOMEDESCRIPTION'].str.contains('LAY|SHOT|MISS|MISSED', case=False, na=False) |
                df['NEUTRALDESCRIPTION'].str.contains('LAY|SHOT|MISS|MISSED', case=False, na=False) |
                df['VISITORDESCRIPTION'].str.contains('LAY|SHOT|MISS|MISSED', case=False, na=False)
            )
        ]
        
        # --- Determine Team Identity for Each Row ---
        # We use PLAYER1_TEAM_ID or PLAYER1_TEAM_ABBREVIATION to know which team triggered the event
        
        # Create columns for mapping
        df['teamId'] = df['PLAYER1_TEAM_ID']
        df['team'] = df['PLAYER1_TEAM_ABBREVIATION']
        
        # Drop rows where we can't identify a team (timeouts, etc, often have no Player1_Team)
        # If you want to keep them, you'd need complex logic to infer possession.
        df.dropna(subset=['teamId'], inplace=True)
        
        # --- FIX: Correct Margin Direction ---
        # Standard PBP Margin is Home-Visitor. 
        # If the row belongs to Visitor, we must flip the margin sign.
        
        def adjust_margin(row):
            # If the active team (teamId) is the Visitor, flip the sign
            # We check against Home Abbreviation because IDs might vary slightly in formats
            if row['team'] == visit_abbr: 
                return -1 * row['SCOREMARGIN']
            return row['SCOREMARGIN']

        df['score_margin'] = df.apply(adjust_margin, axis=1)
        
        # Add Metadata
        df['game_id'] = game_id
        df['year'] = year
        
        # Map other columns as requested
        mapping = {
            'EVENTNUM': 'actionNumber',
        }
        df.rename(columns=mapping, inplace=True)
        
        # Select final columns
        values_list = [
            'teamId',
            'actionNumber',
            'game_id',
            'team',
            'SCORE', # Case sensitive match to source
            'score_margin',
            'year'
        ]
        
        # Ensure cols exist (SCORE might be 'SCORE' or 'score' depending on read)
        final_cols = [c for c in values_list if c in df.columns]
        final_cols.append('playoffs')
        df_final = df[final_cols]
        
        all_dfs.append(df_final)

    except Exception as e:
        print(f"Error processing {game_id}: {e}")
        continue

# Combine all processed games
if all_dfs:
    pbp_df = pd.concat(all_dfs, ignore_index=True)
    print("Combined Shape:", pbp_df.shape)
    print(pbp_df.head(50))
    
        # Save Logic (Matches your snippet)
    for year in pbp_df['year'].unique():
        year_data = pbp_df[pbp_df['year'] == year]
        
        # Separate playoffs and regular season
        playoffs_data = year_data[year_data['playoffs'] == 1]
        regular_season_data = year_data[year_data['playoffs'] == 0]
        
        # Save playoffs data
        if not playoffs_data.empty:
            playoffs_dir = f'indexing/playoffs/{year}'
            os.makedirs(playoffs_dir, exist_ok=True)
            playoffs_file = os.path.join(playoffs_dir, f'{year}_combined.csv')
            playoffs_data.to_csv(playoffs_file, index=False)
            print(f"Saved playoffs data for {year}: {playoffs_data.shape[0]} rows")
        
        # Save regular season data
        if not regular_season_data.empty:
            regular_season_dir = f'indexing/regular_season/{year}'
            os.makedirs(regular_season_dir, exist_ok=True)
            regular_season_file = os.path.join(regular_season_dir, f'{year}_combined.csv')
            regular_season_data.to_csv(regular_season_file, index=False)
            print(f"Saved regular season data for {year}: {regular_season_data.shape[0]} rows")

    print("\nAll data saved successfully!")
else:
    print("No data processed.")