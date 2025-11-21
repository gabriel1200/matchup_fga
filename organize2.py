# FILE: reorganize_dfga.by_year.py

import pandas as pd
import os
import numpy as np
import sys

# ==============================================================================
# DATA LOADING FUNCTIONS (MODIFIED FOR YEAR-BY-YEAR PROCESSING)
# ==============================================================================
# NOTE: Assuming GAME_DATES is available globally or defined here as in the original snippet
GAME_DATES = pd.read_csv('https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv')

def load_lebron_data(url='https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/main/lebron.csv', index_file='modern_index.csv', current_season_year=2026):
    """
    Loads LEBRON player stats and supplements with position data from a local
    index file if the current season's LEBRON data is unavailable.

    Args:
        url (str): The URL to the LEBRON stats CSV file.
        index_file (str): The local CSV file ('modern_index.csv') for current season data.
        current_season_year (int): The integer year (e.g., 2026) to check and supplement.

    Returns:
        pd.DataFrame: A DataFrame containing LEBRON stats, supplemented with 
                      current season position data.
    """
    print("--- Loading LEBRON Stats (once for all years) ---")
    
    # 1. Load LEBRON data from URL
    lebron_df = pd.DataFrame()
    try:
        lebron_df = pd.read_csv(url)
        
        # Convert "YYYY-YY" Season format to a single integer year (e.g., '2024-25' -> 2026).
        lebron_df['year'] = lebron_df['Season'].str.split('-').str[0].astype(int) + 1
        
        # Select and rename core columns.
        lebron_df = lebron_df[['year', 'Pos', 'Defensive Role', 'Offensive Archetype', 'NBA ID', 'D-LEBRON', 'O-LEBRON']].rename(columns={
            'Pos': 'POSITION', 
            'Defensive Role': 'DEF_ROLE', 
            'Offensive Archetype': 'OFF_ROLE',
            'NBA ID': 'player_id', 
            'D-LEBRON': 'D_LEBRON',
            'O-LEBRON': 'O_LEBRON'
        })
        
        lebron_df['player_id'] = pd.to_numeric(lebron_df['player_id'], errors='coerce').dropna().astype(int)
        print(f"Successfully loaded LEBRON data from {url}.")

    except Exception as e:
        print(f"Error loading LEBRON data from URL: {e}")
        lebron_df = pd.DataFrame()

    # 2. Check for current season data (2026) and supplement if missing
    if lebron_df.empty or current_season_year not in lebron_df['year'].unique():
        print(f"--- Current season ({current_season_year}) LEBRON data is missing. Supplementing with {index_file}. ---")
        try:
            # 3. Load the index file
            index_df = pd.read_csv(index_file)
            
            # Filter for the current season year. Assuming the index file has a 'year' column.
            current_data = index_df[index_df['year'] == current_season_year].copy()
            
            if not current_data.empty:
                print(f"Found {len(current_data)} players for year {current_season_year} in {index_file}.")

                # 4. Standardize and add placeholder columns
                current_data = current_data.rename(columns={'Pos': 'POSITION', 'nba_id': 'player_id'})
                current_data['player_id'] = pd.to_numeric(current_data['player_id'], errors='coerce').dropna().astype(int)
                
                # Add placeholder columns for missing LEBRON metrics/roles
                current_data['DEF_ROLE'] = 'NA_INDEX'
                current_data['OFF_ROLE'] = 'NA_INDEX'
                current_data['D_LEBRON'] = np.nan
                current_data['O_LEBRON'] = np.nan
                
                # Select only the required columns for concatenation
                current_data = current_data[['year', 'POSITION', 'DEF_ROLE', 'OFF_ROLE', 'player_id', 'D_LEBRON', 'O_LEBRON']]
                
                # 5. Concatenate with existing data
                if lebron_df.empty:
                    lebron_df = current_data
                else:
                    # Remove any partial or stale current year data from the LEBRON frame before concatenating the new index data
                    lebron_df = lebron_df[lebron_df['year'] != current_season_year]
                    lebron_df = pd.concat([lebron_df, current_data], ignore_index=True)
                    
                print(f"LEBRON data updated with {current_season_year} positions from {index_file}.")
            else:
                print(f"Warning: No data found for year {current_season_year} in {index_file}. LEBRON data remains incomplete.")

        except FileNotFoundError:
            print(f"Error: Index file '{index_file}' not found locally. Could not supplement current season data.")
        except Exception as e:
            print(f"Error processing index file: {e}")

    # Final check and cleanup
    if lebron_df.empty:
        print("Fatal error: LEBRON data could not be loaded or created.")
        return pd.DataFrame()
    else:
        # Ensure 'player_id' is an integer before returning
        lebron_df['player_id'] = lebron_df['player_id'].fillna(-999).astype(int).replace(-999, np.nan)
        return lebron_df

def _load_margin_data(year):
    """
    Helper function to load and prepare the score margin index data for a specific year.
    Returns a DataFrame with columns: ['gi', 'ei', 'team_id', 'score_margin']
    """
    margin_dfs = []
    
    # Define paths to the indexed margin files
    index_paths = [
        f'indexing/regular_season/{year}/{year}_combined.csv',
        f'indexing/playoffs/{year}/{year}_combined.csv'
    ]
    
    print("Loading indexed score margin files...")
    for p in index_paths:
        if os.path.exists(p):
            try:
                df_idx = pd.read_csv(p)
                # Keep only necessary columns
                cols_to_keep = ['score_margin']
                
                # Map standard index columns if they exist
                rename_map = {}
                if 'game_id' in df_idx.columns:
                    rename_map['game_id'] = 'gi'
                    cols_to_keep.append('game_id')
                elif 'gi' in df_idx.columns:
                    cols_to_keep.append('gi')
                    
                if 'actionNumber' in df_idx.columns:
                    rename_map['actionNumber'] = 'ei'
                    cols_to_keep.append('actionNumber')
                elif 'ei' in df_idx.columns:
                    cols_to_keep.append('ei')

                if 'teamId' in df_idx.columns:
                    rename_map['teamId'] = 'team_id'
                    cols_to_keep.append('teamId')
                elif 'team_id' in df_idx.columns:
                    cols_to_keep.append('team_id')

                # Filter and Rename
                # Only select columns that actually exist in df_idx to avoid KeyError
                available_cols = [c for c in cols_to_keep if c in df_idx.columns]
                df_idx = df_idx[available_cols].rename(columns=rename_map)
                margin_dfs.append(df_idx)

            except Exception as e:
                print(f"Warning: Could not read index file {p}: {e}")
        else:
            print(f"Note: Index file not found at {p}")

    if not margin_dfs:
        print("Warning: No margin data loaded. 'score_margin' column will be missing.")
        return pd.DataFrame()

    margin_data = pd.concat(margin_dfs, ignore_index=True)
    
    # Ensure we have the required join keys
    required_keys = ['gi', 'ei', 'team_id']
    if not all(col in margin_data.columns for col in required_keys):
        print(f"Warning: Margin data missing keys. Columns found: {margin_data.columns}. Skipping merge.")
        return pd.DataFrame()
    
    # CRITICAL: Convert join keys to int to prevent mismatch
    margin_data = margin_data.dropna(subset=required_keys)
    
    for col in required_keys:
        try:
            margin_data[col] = margin_data[col].astype(int)
        except Exception as e:
             print(f"Error converting margin data column {col} to int: {e}")
    
    # Drop duplicates if any exist in the index to prevent row explosion during merge
    margin_data = margin_data.drop_duplicates(subset=required_keys)
    
    print(f"Loaded margin data: {len(margin_data)} rows.")
    return margin_data

def load_defender_data_for_year(year, path_template='scraped_data/{year}_dfgtotal.csv'):
    """
    Loads the raw defender tracking data for a single specified year,
    merges it with score-margin indexing, and handles dtype normalization.
    """
    print(f"--- Loading Raw Defender Data for {year} ---")
    file_path = path_template.format(year=year)

    try:
        frame = pd.read_csv(file_path)
        margin = _load_margin_data(year)

        # --- Normalize types for proper merge ---
        for df in (frame, margin):
            df['gi'] = pd.to_numeric(df['gi'], errors='coerce').astype('Int64')
            df['ei'] = pd.to_numeric(df['ei'], errors='coerce').astype('Int64')

        # --- Margin sometimes contains duplicates (multiple contesters)
        #     Keep 1 row per (gi, ei) event
        margin = margin[['gi', 'ei', 'score_margin']].drop_duplicates()

        print(len(frame), " defender rows before merge")
        print(len(margin), " unique margin rows")

        # --- Merge ONLY on gi, ei ---
        frame = frame.merge(
            margin,
            how='left',
            on=['gi', 'ei']        )

        print(len(frame), " rows after merge")
        print(frame.head())
        return frame

    except FileNotFoundError:
        print(f"Warning: File not found at {file_path}, skipping year.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred loading {file_path}. Error: {e}")
        return pd.DataFrame()


def load_shot_data_for_year(defender_df, year, base_path='../../shot_data/team'):
    """
    Loads regular season and post-season shot data for a single year, based on
    the teams present in that year's defender data.

    Args:
        defender_df (pd.DataFrame): The defender DataFrame for a single year.
        year (int): The year to load shot data for.
        base_path (str): The base path to the shot data directory.

    Returns:
        pd.DataFrame: A single DataFrame containing all shot data for the year.
    """
    print(f"--- Loading Team Shot Data for {year} ---")
    unique_teams = defender_df['team_id'].drop_duplicates()
    print(defender_df[defender_df['team_id'].isnull()])
    

    print(unique_teams)
    all_shot_data = []
    
    print(f"Found {len(unique_teams)} teams to process for {year}.")
    
    for team_id in unique_teams:
        season_dfs = []

        # Load Regular Season Data
        reg_season_path = os.path.join(base_path, str(year), f'{team_id}.csv')
        try:
            reg_df = pd.read_csv(reg_season_path)
            reg_df['season_type'] = 'REG'
            season_dfs.append(reg_df)
        except FileNotFoundError:
            pass  # Expected if a team has no shot data

        # Load Postseason Data
        post_season_path = os.path.join(base_path, f'{year}ps', f'{team_id}.csv')
        try:
            post_df = pd.read_csv(post_season_path)
            post_df['season_type'] = 'PS'
            season_dfs.append(post_df)
        except FileNotFoundError:
            pass # Expected

        if season_dfs:
            combined_df = pd.concat(season_dfs, ignore_index=True)
            combined_df['year'] = year
            all_shot_data.append(combined_df)
            
    if not all_shot_data:
        print(f"Warning: No shot data was loaded for {year}.")
        return pd.DataFrame()
        
    print(f"Finished loading shot data for {year}.")
    return pd.concat(all_shot_data, ignore_index=True)


def load_shot_vs_data_for_year(defender_df, year, base_path='../../shot_data/team'):
    """
    Loads regular season and post-season shot data for a single year, based on
    the teams present in that year's defender data.

    Args:
        defender_df (pd.DataFrame): The defender DataFrame for a single year.
        year (int): The year to load shot data for.
        base_path (str): The base path to the shot data directory.

    Returns:
        pd.DataFrame: A single DataFrame containing all shot data for the year.
    """
    print(f"--- Loading Team Shot Data for {year} ---")
    unique_teams = defender_df['team_id'].drop_duplicates()
    print(defender_df[defender_df['team_id'].isnull()])
    

    print(unique_teams)
    all_shot_data = []
    
    print(f"Found {len(unique_teams)} teams to process for {year}.")
    
    for team_id in unique_teams:
        season_dfs = []

        # Load Regular Season Data
        reg_season_path = os.path.join(base_path, str(year), f'{team_id}vs.csv')
        try:
            reg_df = pd.read_csv(reg_season_path)
            reg_df['season_type'] = 'REG'
            season_dfs.append(reg_df)
        except FileNotFoundError:
            pass  # Expected if a team has no shot data

        # Load Postseason Data
        post_season_path = os.path.join(base_path, f'{year}ps', f'{team_id}vs.csv')
        try:
            post_df = pd.read_csv(post_season_path)
            post_df['season_type'] = 'PS'
            season_dfs.append(post_df)
        except FileNotFoundError:
            pass # Expected

        if season_dfs:
            combined_df = pd.concat(season_dfs, ignore_index=True)
            combined_df['year'] = year
            all_shot_data.append(combined_df)
            
    if not all_shot_data:
        print(f"Warning: No shot data was loaded for {year}.")
        return pd.DataFrame()
        
    print(f"Finished loading shot data for {year}.")
    return pd.concat(all_shot_data, ignore_index=True)
# ==============================================================================
# DATA PROCESSING FUNCTIONS (Now accepts lebron_df as an argument)
# ==============================================================================

def add_defender_stats(shot_data_df, defender_df, lebron_df, year):
    """
    Merges defender tracking data with shot data, adding defender position,
    role, D-LEBRON rating, and score_margin.
    """
    print("--- Adding Defender Stats ---")
    
    # 1. Handle Empty Input Cases
    if defender_df.empty or shot_data_df.empty:
        print("Input DataFrame is empty. Returning shot data without changes.")
        cols_to_add = ['DEF_ID', 'DEF_POSITION', 'DEF_ROLE', 'D_LEBRON', 'dsc', 'score_margin']
        for col in cols_to_add:
            shot_data_df[col] = np.nan
        return shot_data_df

    # 2. Group defender_df to handle single vs. multi-defender plays
    defender_df['def_id'] = defender_df['def_id'].astype(str)
    
    # Create the backbone for the merge (Game ID + Event ID + Aggregated Defender IDs)
    def_ids_agg = defender_df.groupby(['gi', 'ei'])['def_id'].apply('|'.join).reset_index()
    def_ids_agg.rename(columns={'def_id': 'DEF_ID'}, inplace=True)

    # --- NEW BLOCK: Preserve score_margin ---
    # Since score_margin is the same for all rows of a specific (gi, ei) event,
    # we extract it and merge it into our aggregated backbone.
    if 'score_margin' in defender_df.columns:
        margin_lookup = defender_df[['gi', 'ei', 'score_margin']].drop_duplicates()
        def_ids_agg = def_ids_agg.merge(margin_lookup, on=['gi', 'ei'], how='left')
    else:
        def_ids_agg['score_margin'] = np.nan
    # ----------------------------------------

    # 3. Identify single-defender plays to fetch LEBRON stats
    defender_counts = defender_df.groupby(['gi', 'ei']).size().reset_index(name='counts')
    single_defender_plays = defender_counts[defender_counts['counts'] == 1][['gi', 'ei']]
    
    # single_defender_rows includes 'dsc' from defender_df
    single_defender_rows = defender_df.merge(single_defender_plays, on=['gi', 'ei'], how='inner')
    
    # Filter LEBRON stats
    lebron_for_year = lebron_df[lebron_df['year'] == year].copy()
    print(f"Filtered LEBRON stats for year {year}. Found {len(lebron_for_year)} records.")

    # 4. Merge LEBRON stats ONLY for single-defender rows
    single_defender_rows['def_id'] = pd.to_numeric(single_defender_rows['def_id'])
    
    defender_lebron_stats = lebron_for_year[['player_id', 'POSITION', 'DEF_ROLE', 'D_LEBRON']].rename(columns={'POSITION': 'DEF_POSITION'})
    
    single_defender_stats = pd.merge(single_defender_rows, defender_lebron_stats, left_on='def_id', right_on='player_id', how='left')

    # 5. Combine the aggregated DEF_IDs/Margins with the single-defender stats
    # Note: We do NOT need to include score_margin in the list below, because it is already in def_ids_agg
    df_combined = pd.merge(def_ids_agg, 
                             single_defender_stats[['gi', 'ei', 'DEF_POSITION', 'DEF_ROLE', 'D_LEBRON', 'dsc']], 
                             on=['gi', 'ei'], how='left')
    
    # 6. Finalize the merge with the main shot data
    df_combined.rename(columns={'gi': 'GAME_ID', 'ei': 'GAME_EVENT_ID'}, inplace=True)
    df_combined.drop_duplicates(subset=['GAME_ID','GAME_EVENT_ID'], inplace=True)
    shot_data_df.drop_duplicates(subset=['GAME_ID','GAME_EVENT_ID'], inplace=True)
    
    merged_data = shot_data_df.merge(df_combined, on=['GAME_ID', 'GAME_EVENT_ID'], how='left')
    merged_data.drop_duplicates(subset=['GAME_ID','GAME_EVENT_ID'], inplace=True)
    
    print("Successfully merged defender stats (and score_margin) for the year.")
    return merged_data

def add_shooter_stats(shot_data_df, lebron_df, year):
    """
    Merges shooter offensive stats (Position, Role, O-LEBRON) onto the shot data.

    Args:
        shot_data_df (pd.DataFrame): The DataFrame with shot details for one year.
        lebron_df (pd.DataFrame): The pre-loaded DataFrame with LEBRON stats for ALL years.
        year (int): The specific year being processed, used to filter LEBRON stats.

    Returns:
        pd.DataFrame: The shot_data_df merged with the shooter stats.
    """
    print("--- Adding Shooter Stats ---")
    if shot_data_df.empty:
        print("Input shot_data_df is empty. Cannot add shooter stats.")
        return shot_data_df

    # 1. Filter LEBRON data for the specific year
    lebron_for_year = lebron_df[lebron_df['year'] == year].copy()

    # 2. Select and rename columns for clarity in the final merged DataFrame
    shooter_stats = lebron_for_year[['player_id', 'POSITION', 'OFF_ROLE', 'O_LEBRON']].rename(columns={
        'POSITION': 'OFF_POSITION'
    })

    # Ensure the merge key in shot_data_df is numeric, coercing errors
    shot_data_df['PLAYER_ID'] = pd.to_numeric(shot_data_df['PLAYER_ID'], errors='coerce')

    # 3. Merge the shooter stats onto the main shot data DataFrame
    merged_df = pd.merge(
        shot_data_df,
        shooter_stats,
        left_on='PLAYER_ID',
        right_on='player_id',
        how='left'
    )

    # 4. Drop the redundant 'player_id' column from the merge
    merged_df.drop(columns=['player_id'], inplace=True, errors='ignore')

    # NOTE for the new logic: The OFF_ROLE and O_LEBRON for 2026 will be 'NA_INDEX' and NaN, respectively.
    # This is the desired behavior for now.
    print(f"Successfully merged shooter stats for {year}.")
    return merged_df


# ==============================================================================
# DATA SAVING FUNCTIONS (UNCHANGED)
# ==============================================================================

def save_merged_data_by_team_year(merged_df, base_path='team', vs = False):
    """
    Saves the merged shot data back into a structured directory,
    split by team, year, and season type.
    """
    team_map=dict(zip(GAME_DATES['team'],GAME_DATES['TEAM_ID']))

    opp_info = GAME_DATES[['GAME_ID','TEAM_ID','opp_team']]
    print('data')
    print(merged_df['TEAM_ID'].dtype)
    print(opp_info['TEAM_ID'].dtype)
    print(merged_df['GAME_ID'].dtype)
    print(opp_info['GAME_ID'].dtype)
    
    opp_info['OPP_ID']= opp_info['opp_team'].map(team_map)
    print(opp_info[['GAME_ID','TEAM_ID']])
    print(merged_df[['GAME_ID','TEAM_ID']])
    print(len(merged_df))
    merged_df = merged_df.merge(opp_info,on=['GAME_ID','TEAM_ID'])
    print(len(merged_df))

    print("--- Saving Data by Team/Year ---")
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    
    if 'TEAM_ID' not in merged_df.columns and 'team_id' in merged_df.columns:
        merged_df = merged_df.rename(columns={'team_id': 'TEAM_ID'})

    unique_teams_years = merged_df[['TEAM_ID', 'year']].drop_duplicates()
    
    for index, row in unique_teams_years.iterrows():
        team_id, year = row['TEAM_ID'], row['year']
        team_year_data = merged_df[(merged_df['TEAM_ID'] == team_id) & (merged_df['year'] == year)]
        team_year_data.sort_values(by=['GAME_DATE','GAME_ID','GAME_EVENT_ID'],inplace=True)
        
        reg_data = team_year_data[team_year_data['season_type'] == 'REG'].copy()

        if vs == False:
            if not reg_data.empty:
                reg_data.sort_values(by=['GAME_DATE','GAME_ID','GAME_EVENT_ID'], inplace=True)
                reg_dir = os.path.join(base_path, str(int(year)))
                os.makedirs(reg_dir, exist_ok=True)
                reg_data.to_csv(os.path.join(reg_dir, f'{int(team_id)}.csv'), index=False)
                
            ps_data = team_year_data[team_year_data['season_type'] == 'PS'].copy()
            if not ps_data.empty:
                ps_data.sort_values(by=['GAME_DATE','GAME_ID','GAME_EVENT_ID'], inplace=True)
                ps_dir = os.path.join(base_path, f'{int(year)}ps')
                os.makedirs(ps_dir, exist_ok=True)
                ps_data.to_csv(os.path.join(ps_dir, f'{int(team_id)}.csv'), index=False)
            team_year_data = merged_df[(merged_df['TEAM_ID'] == team_id) & (merged_df['year'] == year)]


       
        else:

            if not reg_data.empty:
                reg_data.sort_values(by=['GAME_DATE','GAME_ID','GAME_EVENT_ID'], inplace=True)
                reg_dir = os.path.join(base_path, str(int(year)))
                os.makedirs(reg_dir, exist_ok=True)
                reg_data.to_csv(os.path.join(reg_dir, f'{int(team_id)}vs.csv'), index=False)
                
            ps_data = team_year_data[team_year_data['season_type'] == 'PS'].copy()
            if not ps_data.empty:
                ps_data.sort_values(by=['GAME_DATE','GAME_ID','GAME_EVENT_ID'], inplace=True)
                ps_dir = os.path.join(base_path, f'{int(year)}ps')
                os.makedirs(ps_dir, exist_ok=True)
                ps_data.to_csv(os.path.join(ps_dir, f'{int(team_id)}vs.csv'), index=False)
            team_year_data = merged_df[(merged_df['TEAM_ID'] == team_id) & (merged_df['year'] == year)]
        
            
    print("Finished saving data by team for the year.")


def save_merged_data_by_defender(merged_df, base_path='defender'):
    """
    Saves the merged shot data into a directory structured by defender,
    year, and season type.
    """
    print("--- Saving Data by Defender/Year ---")
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    
    df_filtered = merged_df.dropna(subset=['DEF_ID']).copy()
    df_filtered['DEF_ID'] = df_filtered['DEF_ID'].astype(str)
    
    s = df_filtered['DEF_ID'].str.split('|').explode()
    unique_defender_ids = s.unique()
    
    print(f"Found {len(unique_defender_ids)} unique defender IDs to process for the year.")

    for def_id in unique_defender_ids:
        defender_data = df_filtered[df_filtered['DEF_ID'].str.contains(f'\\b{def_id}\\b', regex=True)]
        years_for_defender = defender_data['year'].unique()
        for year in years_for_defender:
            defender_year_data = defender_data[defender_data['year'] == year]
            
            reg_data = defender_year_data[defender_year_data['season_type'] == 'REG'].copy()
            if not reg_data.empty:
                reg_data.sort_values(by=['GAME_ID', 'GAME_EVENT_ID'], inplace=True)
                reg_dir = os.path.join(base_path, str(int(year)))
                os.makedirs(reg_dir, exist_ok=True)
                reg_data.to_csv(os.path.join(reg_dir, f'{def_id}.csv'), index=False)
                
            ps_data = defender_year_data[defender_year_data['season_type'] == 'PS'].copy()
            if not ps_data.empty:
                ps_data.sort_values(by=['GAME_ID', 'GAME_EVENT_ID'], inplace=True)
                ps_dir = os.path.join(base_path, f'{int(year)}ps')
                os.makedirs(ps_dir, exist_ok=True)
                ps_data.to_csv(os.path.join(ps_dir, f'{def_id}.csv'), index=False)
                
    print("Finished saving data by defender for the year.")

def save_merged_data_by_shooter(merged_df, base_path='shooter'):
    """
    Saves the merged shot data into a directory structured by defender,
    year, and season type.
    """
    print("--- Saving Data by Defender/Year ---")
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    
    df_filtered = merged_df.dropna(subset=['PLAYER_ID']).copy()
    df_filtered['PLAYER_ID'] = df_filtered['PLAYER_ID'].astype(str)
    
    s = df_filtered['PLAYER_ID']
    unique_shooter_ids = s.unique()
    
    print(f"Found {len(unique_shooter_ids)} unique shooter IDs to process for the year.")

    for player_id in unique_shooter_ids:
        defender_data = df_filtered[df_filtered['PLAYER_ID']==player_id]
        years_for_defender = defender_data['year'].unique()
        for year in years_for_defender:
            defender_year_data = defender_data[defender_data['year'] == year]
            
            reg_data = defender_year_data[defender_year_data['season_type'] == 'REG'].copy()
            if not reg_data.empty:
                reg_data.sort_values(by=['GAME_ID', 'GAME_EVENT_ID'], inplace=True)
                reg_dir = os.path.join(base_path, str(int(year)))
                os.makedirs(reg_dir, exist_ok=True)
                reg_data.to_csv(os.path.join(reg_dir, f'{player_id}.csv'), index=False)
                
            ps_data = defender_year_data[defender_year_data['season_type'] == 'PS'].copy()
            if not ps_data.empty:
                ps_data.sort_values(by=['GAME_ID', 'GAME_EVENT_ID'], inplace=True)
                ps_dir = os.path.join(base_path, f'{int(year)}ps')
                os.makedirs(ps_dir, exist_ok=True)
                ps_data.to_csv(os.path.join(ps_dir, f'{player_id}.csv'), index=False)
                
    print("Finished saving data by defender for the year.")
# ==============================================================================
# MAIN EXECUTION PIPELINE (MODIFIED END_YEAR)
# ==============================================================================

def process_year(year, lebron_df):
    """
    Runs the full data pipeline for a single year.
    
    Args:
        year (int): The year to process.
        lebron_df (pd.DataFrame): The pre-loaded DataFrame of LEBRON stats.
    """
    print(f"\n{'='*25} PROCESSING YEAR: {year} {'='*25}")
    
    # Step 1: Load the raw defender tracking data for the year
    dfga_year = load_defender_data_for_year(year)
    print(dfga_year[dfga_year.team_id.isna()])
    if dfga_year.empty:
        return # Skip to the next year if no data

    # Step 2: Drop duplicates from defender data
    pre_dup_len = len(dfga_year)
    dfga_year.drop_duplicates(subset=['gi', 'ei', 'def_id'], inplace=True)
    print(f"Defender data length for {year}: {pre_dup_len} -> {len(dfga_year)} (after dropping duplicates)")
    
    # Step 3: Load the corresponding shot data
    shot_data_year = load_shot_data_for_year(dfga_year, year)

    shot_data_year_vs = load_shot_vs_data_for_year(dfga_year, year)
    if shot_data_year.empty:
        return # Skip to next year if no shot data
        
    # Step 4: Process and merge defender data
    merged_data_year = add_defender_stats(shot_data_year, dfga_year, lebron_df, year)
    
    # Step 5: Process and merge shooter data
    merged_data_year = add_shooter_stats(merged_data_year, lebron_df, year)
    

    merged_vs_data_year = add_defender_stats(shot_data_year_vs, dfga_year, lebron_df, year)
    
    # Step 5: Process and merge shooter data
    merged_vs_data_year = add_shooter_stats(merged_vs_data_year, lebron_df, year)
    
    # Step 6: Show coverage stats for the year
    coverage = 100 - (100 * merged_data_year['DEF_ID'].isna().sum() / len(merged_data_year))
    print(f"\n--- Coverage Stats for {year} ---")
    print(f"For {year}, {coverage:.2f}% of shots have logged defenders.")
    
    # Step 7: Save the processed data into the required structures
    save_merged_data_by_team_year(merged_data_year, base_path='team')
    save_merged_data_by_team_year(merged_vs_data_year, base_path='team',vs=True)
    save_merged_data_by_defender(merged_data_year, base_path='defender')
    save_merged_data_by_shooter(merged_data_year,base_path='shooter')
    
    print(f"--- Successfully completed processing for year {year}. ---\n")


if __name__ == '__main__':
    START_YEAR = 2026
    # MODIFICATION: Updated END_YEAR to 2026 to include the current season.
    END_YEAR = 2026 

    # Load LEBRON stats once to be used for every year's processing
    # The default current_season_year=2026 handles the check for the missing data.
    lebron_stats = load_lebron_data()

    if not lebron_stats.empty:
        # Loop through each year and process its data individually (inclusive of END_YEAR)
        for year_to_process in range(START_YEAR, END_YEAR + 1):
            process_year(year_to_process, lebron_stats)
        
        print("\nPIPELINE EXECUTION COMPLETE FOR ALL YEARS.")
    else:
        print("\nPipeline execution stopped because LEBRON stats could not be loaded.")