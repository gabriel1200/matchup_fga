import requests
import pandas as pd
import time
import os
from datetime import datetime
import sys
import json

def load_scrape_log(log_file="scrape_log.json"):
    """
    Load the scrape log that tracks all attempted scrapes
    Returns a dictionary with scrape history
    """
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r') as f:
                log_data = json.load(f)
            print(f"Loaded scrape log with {len(log_data)} previous attempts")
            return log_data
        except Exception as e:
            print(f"Error loading scrape log: {e}")
            return {}
    else:
        print("No existing scrape log found. Starting fresh.")
        return {}

def save_scrape_log(log_data, log_file="scrape_log.json"):
    """
    Save the scrape log to disk
    """
    try:
        with open(log_file, 'w') as f:
            json.dump(log_data, f, indent=2)
    except Exception as e:
        print(f"Error saving scrape log: {e}")

def update_scrape_log(log_data, player_id, game_id, team_id, player_name, year, 
                     success, record_count=0, error_msg=None, has_data=None):
    """
    Update the scrape log with a new attempt
    Added has_data parameter to distinguish between successful requests with/without data
    """
    key = f"{player_id}_{game_id}"
    log_data[key] = {
        "player_id": str(player_id),
        "game_id": str(game_id),
        "team_id": str(team_id),
        "player_name": player_name,
        "year": year,
        "timestamp": datetime.now().isoformat(),
        "success": success,
        "record_count": record_count,
        "has_data": has_data if has_data is not None else (record_count > 0),
        "error_msg": error_msg
    }
    return log_data

def load_already_scraped_combinations_from_files(output_dir="scraped_data"):
    """
    Load all previously scraped combinations from existing batch files
    Returns a set of tuples (player_id, game_id) that have data in CSV files
    """
    already_scraped = set()
    
    if not os.path.exists(output_dir):
        print("No existing scraped data directory found.")
        return already_scraped
    
    # Find all year directories
    year_dirs = [d for d in os.listdir(output_dir) if d.startswith('year_') and os.path.isdir(os.path.join(output_dir, d))]
    
    if not year_dirs:
        print("No existing year directories found.")
        return already_scraped
    
    print(f"Checking existing scraped data in {len(year_dirs)} year directories...")
    
    for year_dir in year_dirs:
        year_path = os.path.join(output_dir, year_dir)
        year = year_dir.replace('year_', '')
        
        # Check for combined file first
        combined_file = os.path.join(year_path, f"combined_video_details_{year}.csv")
        
        if os.path.exists(combined_file):
            print(f"  Loading from combined file: {combined_file}")
            try:
                df = pd.read_csv(combined_file)
                if 'def_id' in df.columns and 'gi' in df.columns:
                    combinations = set(zip(df['def_id'].astype(str), df['gi'].astype(str)))
                    already_scraped.update(combinations)
                    print(f"    Found {len(combinations)} combinations for {year}")
                else:
                    print(f"    Warning: Expected columns not found in {combined_file}")
            except Exception as e:
                print(f"    Error reading {combined_file}: {e}")
        else:
            # Check batch files
            batch_files = [f for f in os.listdir(year_path) if f.startswith('batch_') and f.endswith('.csv')]
            year_combinations = 0
            
            for batch_file in batch_files:
                batch_path = os.path.join(year_path, batch_file)
                try:
                    df = pd.read_csv(batch_path)
                    if 'def_id' in df.columns and 'gi' in df.columns:
                        combinations = set(zip(df['def_id'].astype(str), df['gi'].astype(str)))
                        already_scraped.update(combinations)
                        year_combinations += len(combinations)
                    else:
                        print(f"    Warning: Expected columns not found in {batch_path}")
                except Exception as e:
                    print(f"    Error reading {batch_path}: {e}")
            
            if year_combinations > 0:
                print(f"  Found {year_combinations} combinations from {len(batch_files)} batch files for {year}")
    
    print(f"Total unique combinations with data in CSV files: {len(already_scraped)}")
    return already_scraped

def migrate_csv_data_to_log(csv_combinations, log_data):
    """
    Migrate existing CSV data to the scrape log for combinations not already in the log
    """
    migrated_count = 0
    for player_id, game_id in csv_combinations:
        key = f"{player_id}_{game_id}"
        if key not in log_data:
            # We know this was successful since it's in CSV files, but we don't have other details
            log_data[key] = {
                "player_id": str(player_id),
                "game_id": str(game_id),
                "team_id": "unknown",
                "player_name": "unknown",  # We don't have this from CSV alone
                "year": 2025,  # We don't have this from CSV alone
                "timestamp": "migrated_from_csv",
                "success": True,
                "record_count": 1,  # We know there was at least some data
                "error_msg": None
            }
            migrated_count += 1
    
    if migrated_count > 0:
        print(f"Migrated {migrated_count} successful scrapes from CSV files to log")
    
    return log_data

def fetch_details(game_id, player_id, team_id, context_measure="DEF_FGA"):
    """
    Fetch video details for a specific player in a specific game
    """
    base_url = "https://stats.nba.com/stats/videodetailsasset"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://www.nba.com",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.nba.com"
    }
    
    params = {
        "GameID": '00'+str(game_id),
        "GameEventID": "",
        "PlayerID": str(player_id),
        "TeamID": str(team_id),
        "Season": "",
        "SeasonType": "",
        "AheadBehind": "",
        "CFID": "",
        "CFPARAMS": "",
        "ClutchTime": "",
        "Conference": "",
        "ContextFilter": "",
        "ContextMeasure": context_measure,
        "DateFrom": "",
        "DateTo": "",
        "Division": "",
        "EndPeriod": 0,
        "EndRange": 40800,
        "GROUP_ID": "",
        "GameSegment": "",
        "GroupID": "",
        "GroupMode": "",
        "GroupQuantity": 5,
        "LastNGames": 0,
        "Location": "",
        "Month": 0,
        "OnOff": "",
        "OppPlayerID": "",
        "OpponentTeamID": 0,
        "Outcome": "",
        "PORound": 0,
        "Period": 0,
        "PlayerID1": "",
        "PlayerID2": "",
        "PlayerID3": "",
        "PlayerID4": "",
        "PlayerID5": "",
        "PlayerPosition": "",
        "PointDiff": "",
        "Position": "",
        "RangeType": 0,
        "RookieYear": "",
        "SeasonSegment": "",
        "ShotClockRange": "",
        "StartPeriod": 0,
        "StartRange": 0,
        "StarterBench": "",
        "VsConference": "",
        "VsDivision": "",
        "VsPlayerID1": "",
        "VsPlayerID2": "",
        "VsPlayerID3": "",
        "VsPlayerID4": "",
        "VsPlayerID5": "",
        "VsTeamID": ""
    }
    
    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Request failed with status code {response.status_code} for Player {player_id}, Game {game_id}")
            return None
    except requests.RequestException as e:
        print(f"Request error for Player {player_id}, Game {game_id}: {e}")
        return None

def process_video_data(video_json, player_id, team_id, player_name, year):
    """
    Process the video JSON response into a DataFrame
    """
    try:
        if video_json and 'resultSets' in video_json and 'playlist' in video_json['resultSets']:
            playlist = video_json['resultSets']['playlist']
            if playlist:  # Check if playlist is not empty
                df = pd.DataFrame(playlist)
                if not df.empty and all(col in df.columns for col in ['gi', 'ei', 'dsc']):
                    df = df[['gi', 'ei', 'dsc']]
                    df['def_id'] = player_id
                    df['team_id'] = team_id
                    df['player_name'] = player_name
                    df['year'] = year
                    return df
        return None
    except Exception as e:
        print(f"Error processing data for Player {player_id}: {e}")
        return None

def save_batch_data_by_year(year_data_dict, batch_num, output_dir="scraped_data"):
    """
    Save batch data organized by year
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    total_records_saved = 0
    
    for year, data_list in year_data_dict.items():
        if data_list:
            # Create year-specific directory
            year_dir = os.path.join(output_dir, f"year_{year}")
            if not os.path.exists(year_dir):
                os.makedirs(year_dir)
            
            # Combine all data for this year
            combined_df = pd.concat(data_list, ignore_index=True)
            
            # Save with timestamp and batch number
            filename = f"{year_dir}/batch_{batch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            combined_df.to_csv(filename, index=False)
            
            records_count = len(combined_df)
            total_records_saved += records_count
            print(f"Saved {year} batch {batch_num} with {records_count} records to {filename}")
    
    return total_records_saved

def scrape_nba_video_details(master_record_path, context_measure="DEF_FGA", 
                           delay_between_requests=2, batch_size=50, 
                           force_retry_failed=False, retry_no_data=False):
    """
    Main scraper function that processes all unique player_id and game_id combinations,
    organizing data by year, while skipping already attempted combinations
    
    Args:
        force_retry_failed: If True, will retry previously failed attempts
        retry_no_data: If True, will retry successful attempts that had no data
    """
    # Load scrape log
    print("=== LOADING SCRAPE LOG ===")
    log_data = load_scrape_log()
    
    # Load already scraped combinations from CSV files
    print("\n=== LOADING EXISTING CSV DATA ===")
    csv_combinations = load_already_scraped_combinations_from_files()
    
    # Migrate CSV data to log if needed
    log_data = migrate_csv_data_to_log(csv_combinations, log_data)
    
    # Load master record
    try:
        master_record = pd.read_csv(master_record_path)
        master_record['TEAM_ID']=master_record['TEAM_ID'].astype(int)
        master_record['PLAYER_ID']=master_record['PLAYER_ID'].astype(int)

        master_record=master_record[master_record.year>=2019]
        print(f"\nLoaded master record with {len(master_record)} rows")
        print(f"Columns: {list(master_record.columns)}")
    except Exception as e:
        print(f"Error loading master record: {e}")
        return
    
    # Get unique combinations of player_id and game_id
    unique_combinations = master_record[['PLAYER_ID', 'GAME_ID', 'TEAM_ID', 'PLAYER_NAME', 'year']].drop_duplicates()
    total_combinations = len(unique_combinations)
    print(f"\nFound {total_combinations} unique player-game combinations in master record")
    
    # Filter based on scrape log
    print(f"\n=== FILTERING BASED ON SCRAPE LOG ===")
    unique_combinations['scrape_key'] = unique_combinations['PLAYER_ID'].astype(str) + "_" + unique_combinations['GAME_ID'].astype(str)
    
    # Check what's in the log
    already_attempted = set(log_data.keys())
    
    # Categorize combinations - FIXED LOGIC HERE
    def categorize_attempt(scrape_key):
        if scrape_key not in already_attempted:
            return 'never_attempted'
        entry = log_data[scrape_key]
        if not entry['success']:
            return 'failed'
        elif entry.get('has_data', entry['record_count'] > 0):  # Has data
            return 'successful_with_data'
        else:  # Successful but no data
            return 'successful_no_data'
    
    unique_combinations['log_status'] = unique_combinations['scrape_key'].apply(categorize_attempt)
    
    never_attempted = unique_combinations[unique_combinations['log_status'] == 'never_attempted']
    previously_successful_with_data = unique_combinations[unique_combinations['log_status'] == 'successful_with_data']
    previously_successful_no_data = unique_combinations[unique_combinations['log_status'] == 'successful_no_data']
    previously_failed = unique_combinations[unique_combinations['log_status'] == 'failed']
    
    print(f"Never attempted: {len(never_attempted)} combinations")
    print(f"Previously successful with data: {len(previously_successful_with_data)} combinations")
    print(f"Previously successful but no data: {len(previously_successful_no_data)} combinations")
    print(f"Previously failed: {len(previously_failed)} combinations")
    
    # Determine what to scrape
    to_scrape_parts = [never_attempted]
    
    if force_retry_failed:
        to_scrape_parts.append(previously_failed)
        print(f"Force retry failed enabled: Including {len(previously_failed)} failed attempts")
    
    if retry_no_data:
        to_scrape_parts.append(previously_successful_no_data)
        print(f"Retry no data enabled: Including {len(previously_successful_no_data)} no-data attempts")
    
    to_scrape_df = pd.concat(to_scrape_parts) if len(to_scrape_parts) > 1 else to_scrape_parts[0]
    
    print(f"\nWill attempt {len(to_scrape_df)} combinations total")
    if len(previously_successful_no_data) > 0 and not retry_no_data:
        print(f"Skipping {len(previously_successful_no_data)} successful-no-data combinations (use retry_no_data=True to retry)")
    if len(previously_failed) > 0 and not force_retry_failed:
        print(f"Skipping {len(previously_failed)} previously failed combinations (use force_retry_failed=True to retry)")
    
    # Show breakdown by year
    if len(never_attempted) > 0:
        print("\nNever attempted by year:")
        never_by_year = never_attempted['year'].value_counts().sort_index()
        for year, count in never_by_year.items():
            print(f"  {year}: {count} combinations")
    
    if len(previously_successful_with_data) > 0:
        print("\nPreviously successful with data by year:")
        success_by_year = previously_successful_with_data['year'].value_counts().sort_index()
        for year, count in success_by_year.items():
            print(f"  {year}: {count} combinations")
    
    if len(previously_successful_no_data) > 0:
        print("\nPreviously successful but no data by year:")
        no_data_by_year = previously_successful_no_data['year'].value_counts().sort_index()
        for year, count in no_data_by_year.items():
            print(f"  {year}: {count} combinations")
    
    if len(previously_failed) > 0:
        print("\nPreviously failed by year:")
        failed_by_year = previously_failed['year'].value_counts().sort_index()
        for year, count in failed_by_year.items():
            print(f"  {year}: {count} combinations")
    
    if len(to_scrape_df) == 0:
        print("\n🎉 No combinations to scrape!")
        return
    
    # Initialize tracking variables
    year_data = {}  # Dictionary to hold data by year
    successful_requests = 0
    failed_requests = 0
    no_data_requests = 0
    batch_num = 1
    total_records_saved = 0
    
    # Get the next batch number by checking existing files
    if os.path.exists("scraped_data"):
        existing_batches = []
        for year_dir in os.listdir("scraped_data"):
            if year_dir.startswith('year_'):
                year_path = os.path.join("scraped_data", year_dir)
                if os.path.isdir(year_path):
                    batch_files = [f for f in os.listdir(year_path) if f.startswith('batch_')]
                    for batch_file in batch_files:
                        try:
                            batch_num_str = batch_file.split('_')[1]
                            existing_batches.append(int(batch_num_str))
                        except:
                            pass
        if existing_batches:
            batch_num = max(existing_batches) + 1
            print(f"\nStarting with batch number: {batch_num}")
    
    print(f"\n=== STARTING SCRAPING PROCESS ===")
    
    # Process each combination that needs to be scraped
    for idx, row in to_scrape_df.iterrows():
        player_id = str(row['PLAYER_ID'])
        game_id = str(row['GAME_ID'])
        team_id = str(row['TEAM_ID'])
        player_name = row['PLAYER_NAME']
        year = row['year']
        
        current_position = len(to_scrape_df) - len(to_scrape_df.loc[idx:])
        if current_position % 50 == 0:
            print(f"Processing {current_position + 1}/{len(to_scrape_df)}: Player {player_name} ({player_id}) in Game {game_id} - {year}")
        
        # Fetch video details
        video_json = fetch_details(game_id, player_id, team_id, context_measure)
        
        if video_json:
            # Process the data
            processed_df = process_video_data(video_json, player_id, team_id, player_name, year)
            
            if processed_df is not None and not processed_df.empty:
                # Initialize year key if it doesn't exist
                if year not in year_data:
                    year_data[year] = []
                
                # Add data to the appropriate year
                year_data[year].append(processed_df)
                successful_requests += 1
                record_count = len(processed_df)
                print(f"  ✓ Found {record_count} video records for {year}")
                
                # Update log with success AND data
                log_data = update_scrape_log(log_data, player_id, game_id, team_id, 
                                           player_name, year, True, record_count, has_data=True)
            else:
                print(f"  - No video data available for {year}")
                no_data_requests += 1
                # Update log with success but NO data - key change here
                log_data = update_scrape_log(log_data, player_id, game_id, team_id, 
                                           player_name, year, True, 0, has_data=False)
        else:
            failed_requests += 1
            # Update log with failure
            log_data = update_scrape_log(log_data, player_id, game_id, team_id, 
                                       player_name, year, False, 0, "API request failed")
        
        # Save log periodically (every 10 requests)
        if (current_position + 1) % 10 == 0:
            save_scrape_log(log_data)
        
        # Check if we should save a batch (based on total successful requests WITH DATA)
        if successful_requests > 0 and successful_requests % batch_size == 0:
            records_saved = save_batch_data_by_year(year_data, batch_num)
            total_records_saved += records_saved
            year_data = {}  # Clear all year data after saving
            batch_num += 1
        
        # Add delay between requests to be respectful to the API
        if current_position < len(to_scrape_df) - 1:  # Don't delay after the last request
            time.sleep(delay_between_requests)
    
    # Save any remaining data
    if any(year_data.values()):  # Check if there's any data left to save
        records_saved = save_batch_data_by_year(year_data, batch_num)
        total_records_saved += records_saved
    
    # Save final log
    save_scrape_log(log_data)
    
    # Print summary
    print(f"\n=== SCRAPING COMPLETE ===")
    print(f"Total combinations in master record: {total_combinations}")
    print(f"Attempted to scrape: {len(to_scrape_df)}")
    print(f"Successful requests with data: {successful_requests}")
    print(f"Successful requests with no data: {no_data_requests}")
    print(f"Failed requests: {failed_requests}")
    print(f"New video records saved: {total_records_saved}")
    if len(to_scrape_df) > 0:
        success_rate = ((successful_requests + no_data_requests)/len(to_scrape_df))*100
        print(f"Overall success rate for new requests: {success_rate:.1f}%")
        if successful_requests > 0:
            data_rate = (successful_requests/len(to_scrape_df))*100
            print(f"Data found rate: {data_rate:.1f}%")

def analyze_scrape_log(log_file="scrape_log.json"):
    """
    Analyze the scrape log and provide statistics
    """
    log_data = load_scrape_log(log_file)
    
    if not log_data:
        print("No scrape log data found")
        return
    
    successful = sum(1 for entry in log_data.values() if entry['success'])
    failed = len(log_data) - successful
    total_records = sum(entry.get('record_count', 0) for entry in log_data.values() if entry['success'])
    
    print(f"\n=== SCRAPE LOG ANALYSIS ===")
    print(f"Total attempts logged: {len(log_data)}")
    print(f"Successful attempts: {successful}")
    print(f"Failed attempts: {failed}")
    print(f"Success rate: {(successful/len(log_data))*100:.1f}%")
    print(f"Total video records found: {total_records}")
    
    # Analyze by year if available
    year_stats = {}
    for entry in log_data.values():
        year = entry.get('year', 'unknown')
        if year not in year_stats:
            year_stats[year] = {'attempts': 0, 'success': 0, 'records': 0}
        year_stats[year]['attempts'] += 1
        if entry['success']:
            year_stats[year]['success'] += 1
            year_stats[year]['records'] += entry.get('record_count', 0)
    
    print(f"\nBreakdown by year:")
    
    # Separate numeric years from non-numeric ones for proper sorting
    numeric_years = []
    non_numeric_years = []
    
    for year in year_stats.keys():
        try:
            # Try to convert to int - if successful, it's a numeric year
            numeric_years.append(int(year))
        except (ValueError, TypeError):
            # If conversion fails, it's non-numeric (like 'unknown')
            non_numeric_years.append(str(year))
    
    # Sort numeric years numerically and non-numeric years alphabetically
    sorted_years = sorted(numeric_years) + sorted(non_numeric_years)
    
    for year in sorted_years:
        stats = year_stats[year]
        success_rate = (stats['success'] / stats['attempts']) * 100 if stats['attempts'] > 0 else 0
        print(f"  {year}: {stats['attempts']} attempts, {stats['success']} successful ({success_rate:.1f}%), {stats['records']} records")

def combine_batches_by_year(output_dir="scraped_data"):
    """
    Combine all batch files within each year into a single CSV per year
    """
    if not os.path.exists(output_dir):
        print(f"Output directory {output_dir} does not exist")
        return
    
    # Find all year directories
    year_dirs = [d for d in os.listdir(output_dir) if d.startswith('year_') and os.path.isdir(os.path.join(output_dir, d))]
    
    if not year_dirs:
        print("No year directories found")
        return
    
    print(f"Found {len(year_dirs)} year directories: {sorted(year_dirs)}")
    
    for year_dir in sorted(year_dirs):
        year_path = os.path.join(output_dir, year_dir)
        year = year_dir.replace('year_', '')
        
        # Find all batch files for this year
        batch_files = [f for f in os.listdir(year_path) if f.startswith('batch_') and f.endswith('.csv')]
        
        if not batch_files:
            print(f"No batch files found for {year}")
            continue
        
        print(f"\nProcessing {year}: Found {len(batch_files)} batch files")
        
        # Load and combine all batches for this year
        year_batches = []
        for file in batch_files:
            file_path = os.path.join(year_path, file)
            df = pd.read_csv(file_path)
            year_batches.append(df)
            print(f"  Loaded {file}: {len(df)} records")
        
        # Combine all batches for this year
        if year_batches:
            final_df = pd.concat(year_batches, ignore_index=True)
            
            # Remove duplicates if any
            initial_count = len(final_df)
            final_df = final_df.drop_duplicates()
            final_count = len(final_df)
            
            if initial_count != final_count:
                print(f"  Removed {initial_count - final_count} duplicate records for {year}")
            
            # Save final combined file for this year
            final_filename = f"combined_video_details_{year}.csv"
            final_path = os.path.join(year_path, final_filename)
            final_df.to_csv(final_path, index=False)
            print(f"  ✓ Combined file saved: {final_path} with {final_count} total records")

def get_year_summary(output_dir="scraped_data"):
    """
    Print a summary of data collected by year
    """
    if not os.path.exists(output_dir):
        print(f"Output directory {output_dir} does not exist")
        return
    
    year_dirs = [d for d in os.listdir(output_dir) if d.startswith('year_') and os.path.isdir(os.path.join(output_dir, d))]
    
    if not year_dirs:
        print("No year directories found")
        return
    
    print(f"\n=== DATA SUMMARY BY YEAR ===")
    total_records = 0
    
    for year_dir in sorted(year_dirs):
        year = year_dir.replace('year_', '')
        year_path = os.path.join(output_dir, year_dir)
        
        # Look for combined file first, otherwise count batch files
        combined_file = os.path.join(year_path, f"combined_video_details_{year}.csv")
        
        if os.path.exists(combined_file):
            df = pd.read_csv(combined_file)
            record_count = len(df)
            print(f"{year}: {record_count:,} records (combined)")
        else:
            # Count records in batch files
            batch_files = [f for f in os.listdir(year_path) if f.startswith('batch_') and f.endswith('.csv')]
            record_count = 0
            for file in batch_files:
                file_path = os.path.join(year_path, file)
                df = pd.read_csv(file_path)
                record_count += len(df)
            print(f"{year}: {record_count:,} records ({len(batch_files)} batch files)")
        
        total_records += record_count
    
    print(f"\nTotal records across all years: {total_records:,}")

# Example usage
if __name__ == "__main__":
    # Analyze existing log
    analyze_scrape_log()
    
    # Run the scraper (set force_retry_failed=True to retry previously failed attempts)
    scrape_nba_video_details(
        master_record_path='master_record.csv',
        context_measure="DEF_FGA",
        delay_between_requests=.001,  # 2 seconds between requests
        batch_size=50,  # Save every 50 successful requests
        force_retry_failed=False  # Set to True to retry failed attempts
    )
    
    # Combine all batch files by year
    combine_batches_by_year()
    
    # Print summary
    get_year_summary()
    
    # Final log analysis
    analyze_scrape_log()