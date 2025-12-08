import pandas as pd
import os
import glob
import numpy as np

# --- Configuration ---
START_YEAR = 2018
END_YEAR = 2025 # Process up to 2025, as 2026 is currently handled separately
BASE_URL_TEMPLATE = 'https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/assists/{year}{prefix}/ast.csv'

# Paths based on the saving structure in organize2.py
BASE_PATHS = ['team', 'defender', 'shooter']
# Mapping of file suffixes based on the save_merged_data_by_team_year function
TEAM_SUFFIXES = ['', 'vs'] 

# ==============================================================================
# DATA LOADING FUNCTIONS
# ==============================================================================

def load_assist_data(year, season_prefix):
    """
    Downloads and loads the ASSIST data for a specific year and season type.
    
    Args:
        year (int): The year of the season (e.g., 2024 for 2024-25).
        season_prefix (str): '' for regular season, 'ps' for playoffs.
        
    Returns:
        pd.DataFrame: DataFrame with 'GAME_ID', 'EVENTNUM', and 'ASSIST_ID'.
    """
    url = BASE_URL_TEMPLATE.format(year=year, prefix=season_prefix)
    try:
        # NOTE: Using 'EVENTNUM' as specified in the correction
        assist_df = pd.read_csv(url, usecols=['GAME_ID', 'EVENTNUM', 'ASSIST_ID'])
        
        # Normalize types for proper merge
        assist_df['GAME_ID'] = pd.to_numeric(assist_df['GAME_ID'], errors='coerce').astype('Int64')
        assist_df['EVENTNUM'] = pd.to_numeric(assist_df['EVENTNUM'], errors='coerce').astype('Int64')
        
        print(f"Loaded {len(assist_df)} assist records for {year}{season_prefix}.")
        return assist_df
    except Exception as e:
        print(f"Warning: Could not load assist data from {url}. Error: {e}")
        return pd.DataFrame()

# ==============================================================================
# DATA PROCESSING AND SAVING FUNCTIONS
# ==============================================================================

def merge_and_save_file(file_path, assist_df):
    """
    Reads an existing shot data file, merges in ASSIST_ID, and overwrites the file.
    
    Args:
        file_path (str): The path to the existing CSV file.
        assist_df (pd.DataFrame): The assist data for the relevant season.
    """
    try:
        shot_data = pd.read_csv(file_path)
        
        # Check if assist column already exists (to avoid re-merging)
        if 'ASSIST_ID' in shot_data.columns:
            print(f"Skipping {file_path}: 'ASSIST_ID' already present.")
            return

        # CRITICAL CORRECTION: Rename GAME_EVENT_ID to EVENTNUM in the shot data 
        # temporarily for the merge, as they are the same column logically.
        if 'GAME_EVENT_ID' in shot_data.columns:
             shot_data.rename(columns={'GAME_EVENT_ID': 'EVENTNUM'}, inplace=True)
        
        # Check for the standardized event ID column
        if 'EVENTNUM' not in shot_data.columns:
            print(f"Error merging {file_path}: Missing event ID column ('GAME_EVENT_ID' expected).")
            return
            
        # Normalize types before merge
        shot_data['GAME_ID'] = pd.to_numeric(shot_data['GAME_ID'], errors='coerce').astype('Int64')
        shot_data['EVENTNUM'] = pd.to_numeric(shot_data['EVENTNUM'], errors='coerce').astype('Int64')
        
        # Merge on the two matching keys: 'GAME_ID' and 'EVENTNUM'
        merged_df = shot_data.merge(
            assist_df,
            on=['GAME_ID', 'EVENTNUM'],
            how='left'
        )
        
        # Add a column of NaN if no assist data was available for this season
        if 'ASSIST_ID' not in merged_df.columns:
            merged_df['ASSIST_ID'] = np.nan
        
        # Cleanup: Revert 'EVENTNUM' back to 'GAME_EVENT_ID' before saving
        if 'GAME_EVENT_ID' in shot_data.columns and 'EVENTNUM' in merged_df.columns:
            merged_df.rename(columns={'EVENTNUM': 'GAME_EVENT_ID'}, inplace=True)

        # Overwrite the original file
        merged_df.to_csv(file_path, index=False)
        print(f"Successfully merged ASSIST_ID into: {file_path}")

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

def run_assist_merge_pipeline():
    """
    Main function to orchestrate the merging of assist data into all existing
    team, defender, and shooter shot logs.
    """
    
    for year in range(START_YEAR, END_YEAR + 1):
        for prefix, save_path_prefix in [('', str(year)), ('ps', f'{year}ps')]:
            
            print(f"\n{'='*15} Processing Year {year} Season: {prefix.upper() or 'REG'} {'='*15}")
            
            # 1. Load Assist Data for the current Year/Season
            assist_data = load_assist_data(year, prefix)
            if assist_data.empty:
                continue

            # 2. Iterate through all three base directories
            for base_dir in BASE_PATHS:
                
                # Handle the special case for 'team' base directory files (with/without 'vs')
                if base_dir == 'team':
                    for suffix in TEAM_SUFFIXES:
                        search_pattern = os.path.join(base_dir, save_path_prefix, f'*{suffix}.csv')
                        for file_path in glob.glob(search_pattern):
                            merge_and_save_file(file_path, assist_data)
                
                # Handle 'defender' and 'shooter' base directories
                else:
                    search_pattern = os.path.join(base_dir, save_path_prefix, '*.csv')
                    for file_path in glob.glob(search_pattern):
                        merge_and_save_file(file_path, assist_data)

    print("\nPIPELINE EXECUTION COMPLETE: ASSIST data merged into all historical files.")

if __name__ == '__main__':
    run_assist_merge_pipeline()