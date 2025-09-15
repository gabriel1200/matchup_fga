# FILE: reorganize_dfga.by_year.py

import pandas as pd
import os
import numpy as np

# ==============================================================================
# DATA LOADING FUNCTIONS (MODIFIED FOR YEAR-BY-YEAR PROCESSING)
# ==============================================================================

def load_lebron_data(url='https://raw.githubusercontent.com/gabriel1200/site_Data/refs/heads/main/lebron.csv'):
    """
    Loads LEBRON player stats from a URL. This is loaded once and reused.
    It now loads both offensive and defensive stats.

    Args:
        url (str): The URL to the LEBRON stats CSV file.

    Returns:
        pd.DataFrame: A DataFrame containing LEBRON stats with a corrected year format.
    """
    print("--- Loading LEBRON Stats (once for all years) ---")
    try:
        lebron_df = pd.read_csv(url)
        
        # Convert "YYYY-YY" Season format to a single integer year.
        lebron_df['year'] = lebron_df['Season'].str.split('-').str[0].astype(int) + 1
        
        # Select all relevant offensive and defensive columns
        lebron_df = lebron_df[['year', 'Pos', 'Defensive Role', 'Offensive Archetype', 'NBA ID', 'D-LEBRON', 'O-LEBRON']].rename(columns={
            'Pos': 'POSITION', 
            'Defensive Role': 'DEF_ROLE', 
            'Offensive Archetype': 'OFF_ROLE',
            'NBA ID': 'player_id', 
            'D-LEBRON': 'D_LEBRON',
            'O-LEBRON': 'O_LEBRON'
        })
        
        lebron_df['player_id'] = pd.to_numeric(lebron_df['player_id'], errors='coerce').dropna().astype(int)
        print("Successfully loaded and transformed LEBRON stats.")
        return lebron_df
    except Exception as e:
        print(f"Error: Could not load LEBRON stats from {url}. Error: {e}")
        return pd.DataFrame()


def load_defender_data_for_year(year, path_template='scraped_data/{year}_dfgtotal.csv'):
    """
    Loads the raw defender tracking data for a single specified year.

    Args:
        year (int): The year to load data for.
        path_template (str): A string template for the file path.

    Returns:
        pd.DataFrame: A DataFrame containing the defender tracking data for one year.
    """
    print(f"--- Loading Raw Defender Data for {year} ---")
    file_path = path_template.format(year=year)
    try:
        frame = pd.read_csv(file_path)
        print(f"Successfully loaded {file_path}")
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


# ==============================================================================
# DATA PROCESSING FUNCTIONS (Now accepts lebron_df as an argument)
# ==============================================================================

def add_defender_stats(shot_data_df, defender_df, lebron_df, year):
    """
    Merges defender tracking data with shot data, adding defender position,
    role, and D-LEBRON rating, but ONLY for single-defender plays.

    Args:
        shot_data_df (pd.DataFrame): The DataFrame with shot details for one year.
        defender_df (pd.DataFrame): DataFrame with defender tracking data for one year.
        lebron_df (pd.DataFrame): The pre-loaded DataFrame with LEBRON stats for ALL years.
        year (int): The specific year being processed, used to filter LEBRON stats.

    Returns:
        pd.DataFrame: The shot_data_df merged with the processed defender stats.
    """
    print("--- Adding Defender Stats ---")
    if defender_df.empty or shot_data_df.empty:
        print("Input DataFrame is empty. Returning shot data without changes.")
        shot_data_df['DEF_ID'] = np.nan
        shot_data_df['DEF_POSITION'] = np.nan
        shot_data_df['DEF_ROLE'] = np.nan
        shot_data_df['D_LEBRON'] = np.nan
        return shot_data_df

    # 1. Group defender_df to handle single vs. multi-defender plays
    defender_df['def_id'] = defender_df['def_id'].astype(str)
    
    def_ids_agg = defender_df.groupby(['gi', 'ei'])['def_id'].apply('|'.join).reset_index()
    def_ids_agg.rename(columns={'def_id': 'DEF_ID'}, inplace=True)

    # 2. Identify single-defender plays to fetch their stats
    defender_counts = defender_df.groupby(['gi', 'ei']).size().reset_index(name='counts')
    single_defender_plays = defender_counts[defender_counts['counts'] == 1][['gi', 'ei']]
    single_defender_rows = defender_df.merge(single_defender_plays, on=['gi', 'ei'], how='inner')
    
    # Filter the lebron_df for the current processing year BEFORE merging.
    lebron_for_year = lebron_df[lebron_df['year'] == year].copy()
    print(f"Filtered LEBRON stats for year {year}. Found {len(lebron_for_year)} records.")

    # 3. Merge with pre-loaded LEBRON stats ONLY for single-defender rows
    single_defender_rows['def_id'] = pd.to_numeric(single_defender_rows['def_id'])
    
    # Select and rename LEBRON columns for the defensive merge
    defender_lebron_stats = lebron_for_year[['player_id', 'POSITION', 'DEF_ROLE', 'D_LEBRON']].rename(columns={'POSITION': 'DEF_POSITION'})
    
    single_defender_stats = pd.merge(single_defender_rows, defender_lebron_stats, left_on='def_id', right_on='player_id', how='left')

    # 4. Combine the aggregated DEF_IDs with the single-defender stats
    df_combined = pd.merge(def_ids_agg, 
                             single_defender_stats[['gi', 'ei', 'DEF_POSITION', 'DEF_ROLE', 'D_LEBRON']], 
                             on=['gi', 'ei'], how='left')
    
    # 5. Finalize the merge with the main shot data
    df_combined.rename(columns={'gi': 'GAME_ID', 'ei': 'GAME_EVENT_ID'}, inplace=True)
    df_combined.drop_duplicates(subset=['GAME_ID','GAME_EVENT_ID'],inplace=True)
    shot_data_df.drop_duplicates(subset=['GAME_ID','GAME_EVENT_ID'],inplace=True)
    merged_data = shot_data_df.merge(df_combined, on=['GAME_ID', 'GAME_EVENT_ID'], how='left')
    merged_data.drop_duplicates(subset=['GAME_ID','GAME_EVENT_ID'],inplace=True)
    print("Successfully merged defender stats for the year.")
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

    print(f"Successfully merged shooter stats for {year}.")
    return merged_df


# ==============================================================================
# DATA SAVING FUNCTIONS (UNCHANGED - Their logic already handles yearly directories)
# ==============================================================================

def save_merged_data_by_team_year(merged_df, base_path='team'):
    """
    Saves the merged shot data back into a structured directory,
    split by team, year, and season type.
    """
    print("--- Saving Data by Team/Year ---")
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    
    if 'TEAM_ID' not in merged_df.columns and 'team_id' in merged_df.columns:
        merged_df = merged_df.rename(columns={'team_id': 'TEAM_ID'})

    unique_teams_years = merged_df[['TEAM_ID', 'year']].drop_duplicates()
    
    for index, row in unique_teams_years.iterrows():
        team_id, year = row['TEAM_ID'], row['year']
        team_year_data = merged_df[(merged_df['TEAM_ID'] == team_id) & (merged_df['year'] == year)]
        
        reg_data = team_year_data[team_year_data['season_type'] == 'REG'].copy()
        if not reg_data.empty:
            reg_data.sort_values(by=['GAME_ID', 'GAME_EVENT_ID'], inplace=True)
            reg_dir = os.path.join(base_path, str(int(year)))
            os.makedirs(reg_dir, exist_ok=True)
            reg_data.to_csv(os.path.join(reg_dir, f'{int(team_id)}.csv'), index=False)
            
        ps_data = team_year_data[team_year_data['season_type'] == 'PS'].copy()
        if not ps_data.empty:
            ps_data.sort_values(by=['GAME_ID', 'GAME_EVENT_ID'], inplace=True)
            ps_dir = os.path.join(base_path, f'{int(year)}ps')
            os.makedirs(ps_dir, exist_ok=True)
            ps_data.to_csv(os.path.join(ps_dir, f'{int(team_id)}.csv'), index=False)
            
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
# MAIN EXECUTION PIPELINE (REORGANIZED)
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
    if dfga_year.empty:
        return # Skip to the next year if no data

    # Step 2: Drop duplicates from defender data
    pre_dup_len = len(dfga_year)
    dfga_year.drop_duplicates(subset=['gi', 'ei', 'def_id'], inplace=True)
    print(f"Defender data length for {year}: {pre_dup_len} -> {len(dfga_year)} (after dropping duplicates)")
    
    # Step 3: Load the corresponding shot data
    shot_data_year = load_shot_data_for_year(dfga_year, year)
    if shot_data_year.empty:
        return # Skip to next year if no shot data
        
    # Step 4: Process and merge defender data
    merged_data_year = add_defender_stats(shot_data_year, dfga_year, lebron_df, year)
    
    # Step 5: Process and merge shooter data
    merged_data_year = add_shooter_stats(merged_data_year, lebron_df, year)
    
    # Step 6: Show coverage stats for the year
    coverage = 100 - (100 * merged_data_year['DEF_ID'].isna().sum() / len(merged_data_year))
    print(f"\n--- Coverage Stats for {year} ---")
    print(f"For {year}, {coverage:.2f}% of shots have logged defenders.")
    
    # Step 7: Save the processed data into the required structures
    save_merged_data_by_team_year(merged_data_year, base_path='team')
    save_merged_data_by_defender(merged_data_year, base_path='defender')
    save_merged_data_by_shooter(merged_data_year,base_path='shooter')
    
    print(f"--- Successfully completed processing for year {year}. ---\n")


if __name__ == '__main__':
    START_YEAR = 2017
    END_YEAR = 2025

    # Load LEBRON stats once to be used for every year's processing
    lebron_stats = load_lebron_data()

    if not lebron_stats.empty:
        # Loop through each year and process its data individually
        for year_to_process in range(START_YEAR, END_YEAR + 1):
            process_year(year_to_process, lebron_stats)
        
        print("\nPIPELINE EXECUTION COMPLETE FOR ALL YEARS.")
    else:
        print("\nPipeline execution stopped because LEBRON stats could not be loaded.")