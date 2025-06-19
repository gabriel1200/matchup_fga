import requests
import pandas as pd
import time
import os
from datetime import datetime
import sys
import json
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict

class NBAScraper:
    def __init__(self, max_concurrent=8, delay_between_batches=0.5, max_retries=3):
        """
        Initialize the scraper with concurrency controls
        
        Args:
            max_concurrent: Maximum number of concurrent requests
            delay_between_batches: Delay between batches of concurrent requests
            max_retries: Maximum number of retry attempts for failed requests
        """
        self.max_concurrent = max_concurrent
        self.delay_between_batches = delay_between_batches
        self.max_retries = max_retries
        self.session = None
        self.lock = threading.Lock()
        
        # Headers for requests
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://www.nba.com",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.nba.com"
        }

    def load_scrape_log(self, log_file="scrape_log.json"):
        """Load the scrape log that tracks all attempted scrapes"""
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

    def save_scrape_log(self, log_data, log_file="scrape_log.json"):
        """Save the scrape log to disk"""
        try:
            with open(log_file, 'w') as f:
                json.dump(log_data, f, indent=2)
        except Exception as e:
            print(f"Error saving scrape log: {e}")

    def update_scrape_log(self, log_data, player_id, game_id, team_id, player_name, year, 
                         success, record_count=0, error_msg=None):
        """Update the scrape log with a new attempt"""
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
            "error_msg": error_msg
        }
        return log_data

    def load_already_scraped_combinations_from_files(self, output_dir="scraped_data"):
        """Load all previously scraped combinations from existing batch files"""
        already_scraped = set()
        
        if not os.path.exists(output_dir):
            print("No existing scraped data directory found.")
            return already_scraped
        
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
                    except Exception as e:
                        print(f"    Error reading {batch_path}: {e}")
                
                if year_combinations > 0:
                    print(f"  Found {year_combinations} combinations from {len(batch_files)} batch files for {year}")
        
        print(f"Total unique combinations with data in CSV files: {len(already_scraped)}")
        return already_scraped

    def migrate_csv_data_to_log(self, csv_combinations, log_data):
        """Migrate existing CSV data to the scrape log"""
        migrated_count = 0
        for player_id, game_id in csv_combinations:
            key = f"{player_id}_{game_id}"
            if key not in log_data:
                log_data[key] = {
                    "player_id": str(player_id),
                    "game_id": str(game_id),
                    "team_id": "unknown",
                    "player_name": "unknown",
                    "year": 2025,
                    "timestamp": "migrated_from_csv",
                    "success": True,
                    "record_count": 1,
                    "error_msg": None
                }
                migrated_count += 1
        
        if migrated_count > 0:
            print(f"Migrated {migrated_count} successful scrapes from CSV files to log")
        
        return log_data

    def build_params(self, game_id, player_id, team_id, context_measure="DEF_FGA"):
        """Build API parameters"""
        return {
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

    def fetch_details_sync(self, game_id, player_id, team_id, context_measure="DEF_FGA"):
        """Synchronous fetch function for ThreadPoolExecutor"""
        base_url = "https://stats.nba.com/stats/videodetailsasset"
        params = self.build_params(game_id, player_id, team_id, context_measure)
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(base_url, headers=self.headers, params=params, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limited
                    wait_time = 2 ** attempt  # Exponential backoff
                    print(f"Rate limited, waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"Request failed with status code {response.status_code} for Player {player_id}, Game {game_id}")
                    return None
            except requests.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    print(f"Request error (attempt {attempt + 1}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"Final request error for Player {player_id}, Game {game_id}: {e}")
                    return None
        
        return None

    def process_video_data(self, video_json, player_id, team_id, player_name, year):
        """Process the video JSON response into a DataFrame"""
        try:
            if video_json and 'resultSets' in video_json and 'playlist' in video_json['resultSets']:
                playlist = video_json['resultSets']['playlist']
                if playlist:
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

    def save_batch_data_by_year(self, year_data_dict, batch_num, output_dir="scraped_data"):
        """Save batch data organized by year"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        total_records_saved = 0
        
        for year, data_list in year_data_dict.items():
            if data_list:
                year_dir = os.path.join(output_dir, f"year_{year}")
                if not os.path.exists(year_dir):
                    os.makedirs(year_dir)
                
                combined_df = pd.concat(data_list, ignore_index=True)
                filename = f"{year_dir}/batch_{batch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                combined_df.to_csv(filename, index=False)
                
                records_count = len(combined_df)
                total_records_saved += records_count
                print(f"Saved {year} batch {batch_num} with {records_count} records to {filename}")
        
        return total_records_saved

    def process_batch_concurrent(self, batch_df, log_data, context_measure="DEF_FGA"):
        """Process a batch of requests concurrently"""
        year_data = defaultdict(list)
        results = []
        
        # Create list of tasks
        tasks = []
        for idx, row in batch_df.iterrows():
            tasks.append((
                str(row['PLAYER_ID']),
                str(row['GAME_ID']),
                str(row['TEAM_ID']),
                row['PLAYER_NAME'],
                row['year']
            ))
        
        # Process tasks concurrently
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(
                    self.fetch_details_sync,
                    task[1],  # game_id
                    task[0],  # player_id
                    task[2],  # team_id
                    context_measure
                ): task for task in tasks
            }
            
            # Process completed tasks
            for future in as_completed(future_to_task):
                player_id, game_id, team_id, player_name, year = future_to_task[future]
                
                try:
                    video_json = future.result()
                    
                    if video_json:
                        processed_df = self.process_video_data(video_json, player_id, team_id, player_name, year)
                        
                        if processed_df is not None and not processed_df.empty:
                            year_data[year].append(processed_df)
                            record_count = len(processed_df)
                            
                            # Thread-safe log update
                            with self.lock:
                                log_data = self.update_scrape_log(
                                    log_data, player_id, game_id, team_id,
                                    player_name, year, True, record_count
                                )
                            
                            results.append(('success', player_id, game_id, year, record_count))
                        else:
                            # Thread-safe log update
                            with self.lock:
                                log_data = self.update_scrape_log(
                                    log_data, player_id, game_id, team_id,
                                    player_name, year, True, 0
                                )
                            
                            results.append(('no_data', player_id, game_id, year, 0))
                    else:
                        # Thread-safe log update
                        with self.lock:
                            log_data = self.update_scrape_log(
                                log_data, player_id, game_id, team_id,
                                player_name, year, False, 0, "API request failed"
                            )
                        
                        results.append(('failed', player_id, game_id, year, 0))
                
                except Exception as e:
                    # Thread-safe log update
                    with self.lock:
                        log_data = self.update_scrape_log(
                            log_data, player_id, game_id, team_id,
                            player_name, year, False, 0, str(e)
                        )
                    
                    results.append(('error', player_id, game_id, year, 0))
        
        return dict(year_data), results, log_data

    def scrape_nba_video_details(self, master_record_path, context_measure="DEF_FGA", 
                               batch_size=50, force_retry_failed=False):
        """
        Main scraper function with concurrent processing
        """
        print("=== LOADING SCRAPE LOG ===")
        log_data = self.load_scrape_log()
        
        print("\n=== LOADING EXISTING CSV DATA ===")
        csv_combinations = self.load_already_scraped_combinations_from_files()
        
        log_data = self.migrate_csv_data_to_log(csv_combinations, log_data)
        
        # Load master record
        try:
            master_record = pd.read_csv(master_record_path)
            master_record['TEAM_ID'] = master_record['TEAM_ID'].astype(int)
            master_record['PLAYER_ID'] = master_record['PLAYER_ID'].astype(int)
            master_record = master_record[master_record.year >= 2019]
            print(f"\nLoaded master record with {len(master_record)} rows")
        except Exception as e:
            print(f"Error loading master record: {e}")
            return
        
        # Get unique combinations and filter
        unique_combinations = master_record[['PLAYER_ID', 'GAME_ID', 'TEAM_ID', 'PLAYER_NAME', 'year']].drop_duplicates()
        total_combinations = len(unique_combinations)
        print(f"\nFound {total_combinations} unique player-game combinations")
        
        # Filter based on scrape log
        unique_combinations['scrape_key'] = unique_combinations['PLAYER_ID'].astype(str) + "_" + unique_combinations['GAME_ID'].astype(str)
        already_attempted = set(log_data.keys())
        
        unique_combinations['log_status'] = unique_combinations['scrape_key'].apply(
            lambda x: 'never_attempted' if x not in already_attempted else 
                     ('successful' if log_data[x]['success'] else 'failed')
        )
        
        never_attempted = unique_combinations[unique_combinations['log_status'] == 'never_attempted']
        previously_failed = unique_combinations[unique_combinations['log_status'] == 'failed']
        
        if force_retry_failed:
            to_scrape_df = pd.concat([never_attempted, previously_failed])
            print(f"\nWill attempt {len(to_scrape_df)} combinations (including {len(previously_failed)} retries)")
        else:
            to_scrape_df = never_attempted
            print(f"\nWill attempt {len(to_scrape_df)} never-attempted combinations")
        
        if len(to_scrape_df) == 0:
            print("\n🎉 No combinations to scrape!")
            return
        
        # Initialize tracking
        successful_requests = 0
        failed_requests = 0
        total_records_saved = 0
        batch_num = 1
        
        # Get next batch number
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
        
        print(f"\n=== STARTING CONCURRENT SCRAPING ===")
        print(f"Concurrency: {self.max_concurrent} simultaneous requests")
        print(f"Batch delay: {self.delay_between_batches}s between batches")
        
        # Process in batches
        batch_size_concurrent = self.max_concurrent * 4  # Process more per batch since we're concurrent
        
        for i in range(0, len(to_scrape_df), batch_size_concurrent):
            batch_df = to_scrape_df.iloc[i:i + batch_size_concurrent]
            batch_progress = i // batch_size_concurrent + 1
            total_batches = (len(to_scrape_df) + batch_size_concurrent - 1) // batch_size_concurrent
            
            print(f"\nProcessing batch {batch_progress}/{total_batches} ({len(batch_df)} combinations)...")
            
            # Process batch concurrently
            year_data, results, log_data = self.process_batch_concurrent(batch_df, log_data, context_measure)
            
            # Count results
            batch_success = sum(1 for r in results if r[0] in ['success', 'no_data'])
            batch_failed = sum(1 for r in results if r[0] in ['failed', 'error'])
            batch_records = sum(r[4] for r in results if r[0] == 'success')
            
            successful_requests += batch_success
            failed_requests += batch_failed
            
            print(f"  Batch results: {batch_success} successful, {batch_failed} failed, {batch_records} records")
            
            # Save data if we have any
            if year_data:
                records_saved = self.save_batch_data_by_year(year_data, batch_num)
                total_records_saved += records_saved
                batch_num += 1
            
            # Save log after each batch
            self.save_scrape_log(log_data)
            
            # Small delay between batches
            if i + batch_size_concurrent < len(to_scrape_df):
                time.sleep(self.delay_between_batches)
        
        # Final summary
        print(f"\n=== SCRAPING COMPLETE ===")
        print(f"Total combinations attempted: {len(to_scrape_df)}")
        print(f"Successful requests: {successful_requests}")
        print(f"Failed requests: {failed_requests}")
        print(f"Total video records saved: {total_records_saved}")
        if len(to_scrape_df) > 0:
            print(f"Success rate: {(successful_requests/len(to_scrape_df))*100:.1f}%")
        
        # Calculate speed improvement
        old_time_estimate = len(to_scrape_df) * 2.001  # Old delay + request time
        new_time_estimate = (len(to_scrape_df) / self.max_concurrent) + (total_batches * self.delay_between_batches)
        print(f"Estimated time saved: {old_time_estimate/60:.1f} min -> {new_time_estimate/60:.1f} min")
        print(f"Speed improvement: ~{old_time_estimate/new_time_estimate:.1f}x faster")

    def combine_batches_by_year(self, output_dir="scraped_data"):
        """Combine all batch files within each year into a single CSV per year"""
        if not os.path.exists(output_dir):
            print(f"Output directory {output_dir} does not exist")
            return
        
        year_dirs = [d for d in os.listdir(output_dir) if d.startswith('year_') and os.path.isdir(os.path.join(output_dir, d))]
        
        if not year_dirs:
            print("No year directories found")
            return
        
        print(f"Found {len(year_dirs)} year directories: {sorted(year_dirs)}")
        
        for year_dir in sorted(year_dirs):
            year_path = os.path.join(output_dir, year_dir)
            year = year_dir.replace('year_', '')
            
            batch_files = [f for f in os.listdir(year_path) if f.startswith('batch_') and f.endswith('.csv')]
            
            if not batch_files:
                print(f"No batch files found for {year}")
                continue
            
            print(f"\nProcessing {year}: Found {len(batch_files)} batch files")
            
            year_batches = []
            for file in batch_files:
                file_path = os.path.join(year_path, file)
                df = pd.read_csv(file_path)
                year_batches.append(df)
                print(f"  Loaded {file}: {len(df)} records")
            
            if year_batches:
                final_df = pd.concat(year_batches, ignore_index=True)
                
                initial_count = len(final_df)
                final_df = final_df.drop_duplicates()
                final_count = len(final_df)
                
                if initial_count != final_count:
                    print(f"  Removed {initial_count - final_count} duplicate records for {year}")
                
                final_filename = f"combined_video_details_{year}.csv"
                final_path = os.path.join(year_path, final_filename)
                final_df.to_csv(final_path, index=False)
                print(f"  ✓ Combined file saved: {final_path} with {final_count} total records")

    def analyze_scrape_log(self, log_file="scrape_log.json"):
        """Analyze the scrape log and provide statistics"""
        log_data = self.load_scrape_log(log_file)
        
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


# Example usage
if __name__ == "__main__":
    # Initialize scraper with optimized settings
    scraper = NBAScraper(
        max_concurrent=3,        # 8 simultaneous requests
        delay_between_batches=0.01,  # 0.5s between batches
        max_retries=3           # 3 retry attempts
    )
    
    # Analyze existing log
    scraper.analyze_scrape_log()
    
    # Run the optimized scraper
    scraper.scrape_nba_video_details(
        master_record_path='master_record.csv',
        context_measure="DEF_FGA",
        batch_size=50,
        force_retry_failed=True
    )
    
    # Combine all batch files by year
    scraper.combine_batches_by_year()
    
    # Final analysis
    scraper.analyze_scrape_log()