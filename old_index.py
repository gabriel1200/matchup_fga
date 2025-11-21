import pandas as pd
import os
import sys

# --- Paths ---
regular_path = '../../playindex/gameplaybyplay'

# Load game dates
frame = pd.read_csv('https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv')

frame['year'] = frame['season'].str.split('-').str[0]
frame['year'] = frame['year'].astype(int) + 1
frame = frame[frame['year'] > 2017]
frame = frame[frame['year'] < 2021]


frame.drop_duplicates(subset=['GAME_ID'],inplace=True)

# Ensure game_id is string for matching filenames
frame['GAME_ID'] = frame['GAME_ID'].astype(str)

# Final list to collect dfs
all_dfs = []

for _, row in frame.iterrows():
   
    game_id = row['GAME_ID']
    year = row['year']
    team_code = row['team'] # The team for this row
    home_team_code = row['HTM'] # The home team for this game
    file_path = os.path.join(regular_path, f"{game_id}.csv")
   

    if not os.path.exists(file_path):
        print(f"Missing file for game_id {game_id}")
        continue

    try:
        df = pd.read_csv(file_path)
        
        # --- Fix Score Margin ---
        # 1. Ensure SCOREMARGIN is treated as string first to replace 'TIE'
        df['SCOREMARGIN'] = df['SCOREMARGIN'].astype(str).str.replace('TIE', '0')
        
        # 2. Convert to numeric, forcing errors to NaN (for any empty strings)
        df['SCOREMARGIN'] = pd.to_numeric(df['SCOREMARGIN'], errors='coerce')
        
        # 3. Forward fill NaNs (score margin doesn't change on non-scoring plays) and fill initial NaNs with 0
        df['SCOREMARGIN'] = df['SCOREMARGIN'].ffill().fillna(0)
        
        # 4. If the current team (team_code) is NOT the home team, flip the margin
        # SCOREMARGIN is typically (Home - Visitor). 
        # If we are the Visitor, our margin is (Visitor - Home), which is -(SCOREMARGIN).
        if team_code != home_team_code:
            df['SCOREMARGIN'] = -1 * df['SCOREMARGIN']
            
        # ------------------------

        # Original Filtering Logic (Preserved from snippet)
        # Note: This logic seemingly removes rows with 'LAY' or 'SHOT' in descriptions. 
        # Ensure this is intended (usually one wants to KEEP shots). 
        # Assuming original intent was to filter out specific non-relevant rows or keep others.
        df = df[
            ~(
                df['HOMEDESCRIPTION'].str.contains('LAY|SHOT', case=False, na=False) |
                df['NEUTRALDESCRIPTION'].str.contains('LAY|SHOT', case=False, na=False) |
                df['VISITORDESCRIPTION'].str.contains('LAY|SHOT', case=False, na=False)
            )
        ]
        df['SCORE'] = df['SCORE'].ffill()

        # Add year column
        df['year'] = year
        
        # Determine if playoffs or regular season based on game_id prefix
        df['playoffs'] = 1 if game_id.startswith('4') else 0
        
        # Identify home and away team IDs
        home_team_id = None
        away_team_id = None
        
        # Find team IDs from rows with SCORE data
        for idx, game_row in df.iterrows():
            if pd.notna(game_row['SCORE']) and game_row['SCORE'] != '':
                if pd.notna(game_row['PLAYER1_TEAM_ID']):
                    team_id = int(game_row['PLAYER1_TEAM_ID'])
                    
                    # Check if this is mentioned in home or visitor description
                    if pd.notna(game_row['HOMEDESCRIPTION']) and game_row['HOMEDESCRIPTION'] != '':
                        if home_team_id is None:
                            home_team_id = team_id
                    elif pd.notna(game_row['VISITORDESCRIPTION']) and game_row['VISITORDESCRIPTION'] != '':
                        if away_team_id is None:
                            away_team_id = team_id
                
                if home_team_id and away_team_id:
                    break
        
        # If we still don't have both teams, find them from all PLAYER1_TEAM_IDs
        if home_team_id is None or away_team_id is None:
            unique_teams = df[df['PLAYER1_TEAM_ID'].notna()]['PLAYER1_TEAM_ID'].unique()
            if len(unique_teams) >= 2:
                if home_team_id is None:
                    home_team_id = int(unique_teams[0])
                if away_team_id is None:
                    away_team_id = int(unique_teams[1])
        
        # Calculate score margin from SCORE column
        def calculate_margin_from_score(score_str, team_id, home_id, away_id):
            if pd.isna(score_str) or score_str == '':
                return None
            
            scores = str(score_str).split(' - ')
            if len(scores) != 2:
                return None
            
            try:
                home_score = int(scores[0])
                away_score = int(scores[1])
                
                # Return margin from team's perspective
                if team_id == home_id:
                    return home_score - away_score
                elif team_id == away_id:
                    return away_score - home_score
                else:
                    return None
            except:
                return None
        
        # Process each team separately
        team_dfs = []
        
        for team_id in df['PLAYER1_TEAM_ID'].dropna().unique():
            team_id = int(team_id)
            team_df = df[df['PLAYER1_TEAM_ID'] == team_id].copy()
            
            # Calculate margin for rows with SCORE
            team_df['calculated_margin'] = team_df.apply(
                lambda row: calculate_margin_from_score(
                    row['SCORE'], 
                    team_id, 
                    home_team_id, 
                    away_team_id
                ) if pd.notna(row['SCORE']) else None,
                axis=1
            )
            
            # Use existing SCOREMARGIN if available, otherwise use calculated
            team_df['SCOREMARGIN'] = team_df.apply(
                lambda row: row['SCOREMARGIN'] if pd.notna(row['SCOREMARGIN']) else row['calculated_margin'],
                axis=1
            )
            
            # Forward fill within this team's rows
            team_df['SCOREMARGIN'] = team_df['SCOREMARGIN'].ffill()

            
            # Drop the temporary column
            team_df.drop('calculated_margin', axis=1, inplace=True)
            
            team_dfs.append(team_df)
        
        # Add rows with no PLAYER1_TEAM_ID
        no_team_df = df[df['PLAYER1_TEAM_ID'].isna()].copy()
        if not no_team_df.empty:
            team_dfs.append(no_team_df)
        
        # Combine all teams back together and sort by original order
        df = pd.concat(team_dfs, ignore_index=False).sort_index()

        all_dfs.append(df)

    except Exception as e:
        print(f"Error reading {file_path}: {e}")

# Combine all games into one dataframe
pbp_df = pd.concat(all_dfs, ignore_index=True)

mapping = {
    'PLAYER1_TEAM_ID': 'teamId',
    'EVENTNUM': 'actionNumber',
    'GAME_ID': 'game_id',
    'PLAYER1_TEAM_ABBREVIATION': 'team',
    'SCORE': 'score',
    'SCOREMARGIN': 'score_margin'
}

values_list = [
    'teamId',
    'actionNumber',
    'game_id',
    'team',
    'score',
    'score_margin',
    'year',
    'playoffs'
]

pbp_df.rename(columns=mapping, inplace=True)
pbp_df = pbp_df[values_list]
pbp_df.dropna(subset='teamId',inplace=True)
print(pbp_df.head(30))
print("Loaded PBP shape:", pbp_df.shape)

# Save to appropriate directories based on playoffs flag
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