import asyncio
import aiohttp
import pandas as pd
import time
import os
from datetime import datetime
import sys
import json
from concurrent.futures import ThreadPoolExecutor
import threading
from queue import Queue

class OptimizedNBAScraper:
    def __init__(self, max_concurrent=20, delay_between_batches=0.1):
        self.max_concurrent = max_concurrent
        self.delay_between_batches = delay_between_batches
        self.session = None
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://www.nba.com",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.nba.com"
        }
        
        # Thread-safe logging
        self.log_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        
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
        """Save the scrape log to disk (thread-safe)"""
        with self.log_lock:
            try:
                with open(log_file, 'w') as f:
                    json.dump(log_data, f, indent=2)
            except Exception as e:
                print(f"Error saving scrape log: {e}")

    def update_scrape_log(self, log_data, player_id, game_id, team_id, player_name, year, 
                         success, record_count=0, error_msg=None):
        """Update the scrape log with a new attempt (thread-safe)"""
        key = f"{player_id}_{game_id}"
        with self.log_lock:
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

    async def fetch_details_async(self, session, game_id, player_id, team_id, context_measure="DEF_FGA"):
        """Async version of fetch_details"""
        base_url = "https://stats.nba.com/stats/videodetailsasset"
        
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
            async with session.get(base_url, headers=self.headers, params=params, timeout=30) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return None
        except Exception as e:
            print(f"Request error for Player {player_id}, Game {game_id}: {e}")
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
        """Save batch data organized by year (thread-safe)"""
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

    async def process_combination_batch(self, session, batch_data, log_data, progress_counter):
        """Process a batch of combinations asynchronously"""
        tasks = []
        for row_data in batch_data:
            task = self.process_single_combination(session, row_data, log_data, progress_counter)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if not isinstance(r, Exception)]

    async def process_single_combination(self, session, row_data, log_data, progress_counter):
        """Process a single player-game combination"""
        player_id = str(row_data['PLAYER_ID'])
        game_id = str(row_data['GAME_ID'])
        team_id = str(row_data['TEAM_ID'])
        player_name = row_data['PLAYER_NAME']
        year = row_data['year']
        
        # Fetch video details
        video_json = await self.fetch_details_async(session, game_id, player_id, team_id)
        
        # Update progress
        with self.progress_lock:
            progress_counter['completed'] += 1
            if progress_counter['completed'] % 100 == 0:
                print(f"Progress: {progress_counter['completed']}/{progress_counter['total']} ({progress_counter['completed']/progress_counter['total']*100:.1f}%)")
        
        if video_json:
            processed_df = self.process_video_data(video_json, player_id, team_id, player_name, year)
            
            if processed_df is not None and not processed_df.empty:
                record_count = len(processed_df)
                self.update_scrape_log(log_data, player_id, game_id, team_id, 
                                     player_name, year, True, record_count)
                return {
                    'data': processed_df,
                    'year': year,
                    'success': True,
                    'record_count': record_count
                }
            else:
                self.update_scrape_log(log_data, player_id, game_id, team_id, 
                                     player_name, year, True, 0)
                return {
                    'success': True,
                    'record_count': 0
                }
        else:
            self.update_scrape_log(log_data, player_id, game_id, team_id, 
                                 player_name, year, False, 0, "API request failed")
            return {
                'success': False,
                'record_count': 0
            }

    async def scrape_nba_video_details_async(self, master_record_path, context_measure="DEF_FGA", 
                                           batch_size=50, force_retry_failed=False):
        """Main async scraper function"""
        # Load scrape log
        print("=== LOADING SCRAPE LOG ===")
        log_data = self.load_scrape_log()
        
        # Load already scraped combinations from CSV files
        print("\n=== LOADING EXISTING CSV DATA ===")
        csv_combinations = self.load_already_scraped_combinations_from_files()
        
        # Load master record
        try:
            master_record = pd.read_csv(master_record_path)
            master_record['TEAM_ID'] = master_record['TEAM_ID'].astype(int)
            master_record['PLAYER_ID'] = master_record['PLAYER_ID'].astype(int)
            master_record = master_record[master_record.year > 2024]
            print(f"\nLoaded master record with {len(master_record)} rows")
        except Exception as e:
            print(f"Error loading master record: {e}")
            return
        
        # Get unique combinations and filter
        unique_combinations = master_record[['PLAYER_ID', 'GAME_ID', 'TEAM_ID', 'PLAYER_NAME', 'year']].drop_duplicates()
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
        else:
            to_scrape_df = never_attempted
        
        print(f"\nWill attempt {len(to_scrape_df)} combinations")
        
        if len(to_scrape_df) == 0:
            print("\n🎉 No combinations to scrape!")
            return
        
        # Initialize tracking
        year_data = {}
        successful_requests = 0
        failed_requests = 0
        batch_num = 1
        total_records_saved = 0
        
        progress_counter = {'completed': 0, 'total': len(to_scrape_df)}
        
        # Create async session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent * 2,  # Total connection pool size
            limit_per_host=self.max_concurrent,  # Connections per host
            ttl_dns_cache=300,  # DNS cache TTL
            use_dns_cache=True,
        )
        
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(
            connector=connector, 
            timeout=timeout,
            headers=self.headers
        ) as session:
            
            # Process in chunks to manage memory and intermediate saves
            chunk_size = batch_size * 4  # Process 4 batches worth at a time
            
            for chunk_start in range(0, len(to_scrape_df), chunk_size):
                chunk_end = min(chunk_start + chunk_size, len(to_scrape_df))
                chunk_df = to_scrape_df.iloc[chunk_start:chunk_end]
                
                print(f"\nProcessing chunk {chunk_start//chunk_size + 1}/{(len(to_scrape_df)-1)//chunk_size + 1}")
                
                # Convert chunk to list of dicts for easier async processing
                chunk_data = chunk_df.to_dict('records')
                
                # Split chunk into smaller batches for concurrent processing
                concurrent_batch_size = self.max_concurrent
                
                for batch_start in range(0, len(chunk_data), concurrent_batch_size):
                    batch_end = min(batch_start + concurrent_batch_size, len(chunk_data))
                    batch_data = chunk_data[batch_start:batch_end]
                    
                    # Process batch concurrently
                    results = await self.process_combination_batch(session, batch_data, log_data, progress_counter)
                    
                    # Process results
                    for result in results:
                        if result.get('success'):
                            successful_requests += 1
                            if 'data' in result:
                                year = result['year']
                                if year not in year_data:
                                    year_data[year] = []
                                year_data[year].append(result['data'])
                        else:
                            failed_requests += 1
                    
                    # Small delay between batches to be respectful
                    if batch_end < len(chunk_data):
                        await asyncio.sleep(self.delay_between_batches)
                
                # Save intermediate results
                if any(year_data.values()):
                    records_saved = self.save_batch_data_by_year(year_data, batch_num)
                    total_records_saved += records_saved
                    year_data = {}
                    batch_num += 1
                
                # Save log periodically
                self.save_scrape_log(log_data)
        
        # Save any remaining data
        if any(year_data.values()):
            records_saved = self.save_batch_data_by_year(year_data, batch_num)
            total_records_saved += records_saved
        
        # Save final log
        self.save_scrape_log(log_data)
        
        # Print summary
        print(f"\n=== SCRAPING COMPLETE ===")
        print(f"Attempted to scrape: {len(to_scrape_df)}")
        print(f"Successful requests: {successful_requests}")
        print(f"Failed requests: {failed_requests}")
        print(f"New video records saved: {total_records_saved}")
        if len(to_scrape_df) > 0:
            print(f"Success rate: {(successful_requests/len(to_scrape_df))*100:.1f}%")

    def scrape_sync_wrapper(self, *args, **kwargs):
        """Synchronous wrapper for the async scraper"""
        return asyncio.run(self.scrape_nba_video_details_async(*args, **kwargs))

# Helper functions (keeping the same ones from your original code)
def combine_batches_by_year(output_dir="scraped_data"):
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

def analyze_scrape_log(log_file="scrape_log.json"):
    """Analyze the scrape log and provide statistics"""
    scraper = OptimizedNBAScraper()
    log_data = scraper.load_scrape_log(log_file)
    
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
    # Create scraper instance
    scraper = OptimizedNBAScraper(
        max_concurrent=7,  # Adjust based on your needs and API limits
        delay_between_batches=0.1  # Much shorter delay
    )
    
    # Analyze existing log
    analyze_scrape_log()
    
    # Run the optimized scraper
    scraper.scrape_sync_wrapper(
        master_record_path='master_record.csv',
        context_measure="DEF_FGA",
        batch_size=50,
        force_retry_failed=False
    )
    
    # Combine all batch files by year
    combine_batches_by_year()
    
    # Final analysis
    analyze_scrape_log()