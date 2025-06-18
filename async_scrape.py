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
from queue import Queue
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NBAScraper:
    def __init__(self, max_concurrent_requests=10, request_delay=0.1, 
                 batch_size=50, output_dir="scraped_data"):
        self.max_concurrent_requests = max_concurrent_requests
        self.request_delay = request_delay
        self.batch_size = batch_size
        self.output_dir = output_dir
        self.session = None
        self.log_data = {}
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
                    self.log_data = json.load(f)
                logger.info(f"Loaded scrape log with {len(self.log_data)} previous attempts")
            except Exception as e:
                logger.error(f"Error loading scrape log: {e}")
                self.log_data = {}
        else:
            logger.info("No existing scrape log found. Starting fresh.")
            self.log_data = {}

    def save_scrape_log(self, log_file="scrape_log.json"):
        """Save the scrape log to disk"""
        try:
            with open(log_file, 'w') as f:
                json.dump(self.log_data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving scrape log: {e}")

    def update_scrape_log(self, player_id, game_id, team_id, player_name, year, 
                         success, record_count=0, error_msg=None):
        """Thread-safe update of scrape log"""
        key = f"{player_id}_{game_id}"
        with self.lock:
            self.log_data[key] = {
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
                    logger.warning(f"Request failed with status {response.status} for Player {player_id}, Game {game_id}")
                    return None
        except Exception as e:
            logger.error(f"Request error for Player {player_id}, Game {game_id}: {e}")
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
            logger.error(f"Error processing data for Player {player_id}: {e}")
            return None

    async def process_single_request(self, session, player_id, game_id, team_id, player_name, year, context_measure="DEF_FGA"):
        """Process a single request asynchronously"""
        # Add small delay to avoid overwhelming the server
        await asyncio.sleep(self.request_delay)
        
        video_json = await self.fetch_details_async(session, game_id, player_id, team_id, context_measure)
        
        if video_json:
            processed_df = self.process_video_data(video_json, player_id, team_id, player_name, year)
            
            if processed_df is not None and not processed_df.empty:
                record_count = len(processed_df)
                self.update_scrape_log(player_id, game_id, team_id, player_name, year, True, record_count)
                return processed_df, True, record_count
            else:
                self.update_scrape_log(player_id, game_id, team_id, player_name, year, True, 0)
                return None, True, 0
        else:
            self.update_scrape_log(player_id, game_id, team_id, player_name, year, False, 0, "API request failed")
            return None, False, 0

    async def scrape_batch_async(self, batch_data, context_measure="DEF_FGA"):
        """Scrape a batch of requests asynchronously"""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent_requests, limit_per_host=self.max_concurrent_requests)
        timeout = aiohttp.ClientTimeout(total=60)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Create semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            
            async def limited_request(row):
                async with semaphore:
                    return await self.process_single_request(
                        session, row['PLAYER_ID'], row['GAME_ID'], row['TEAM_ID'], 
                        row['PLAYER_NAME'], row['year'], context_measure
                    )
            
            # Execute all requests concurrently
            tasks = [limited_request(row) for _, row in batch_data.iterrows()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            return results

    def save_batch_data_by_year(self, year_data_dict, batch_num):
        """Save batch data organized by year"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        total_records_saved = 0
        
        for year, data_list in year_data_dict.items():
            if data_list:
                # Create year-specific directory
                year_dir = os.path.join(self.output_dir, f"year_{year}")
                if not os.path.exists(year_dir):
                    os.makedirs(year_dir)
                
                # Combine all data for this year
                combined_df = pd.concat(data_list, ignore_index=True)
                
                # Save with timestamp and batch number
                filename = f"{year_dir}/batch_{batch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                combined_df.to_csv(filename, index=False)
                
                records_count = len(combined_df)
                total_records_saved += records_count
                logger.info(f"Saved {year} batch {batch_num} with {records_count} records to {filename}")
        
        return total_records_saved

    def load_already_scraped_combinations_from_files(self):
        """Load all previously scraped combinations from existing batch files"""
        already_scraped = set()
        
        if not os.path.exists(self.output_dir):
            logger.info("No existing scraped data directory found.")
            return already_scraped
        
        # Find all year directories
        year_dirs = [d for d in os.listdir(self.output_dir) if d.startswith('year_') and os.path.isdir(os.path.join(self.output_dir, d))]
        
        if not year_dirs:
            logger.info("No existing year directories found.")
            return already_scraped
        
        logger.info(f"Checking existing scraped data in {len(year_dirs)} year directories...")
        
        for year_dir in year_dirs:
            year_path = os.path.join(self.output_dir, year_dir)
            year = year_dir.replace('year_', '')
            
            # Check for combined file first
            combined_file = os.path.join(year_path, f"combined_video_details_{year}.csv")
            
            if os.path.exists(combined_file):
                logger.info(f"  Loading from combined file: {combined_file}")
                try:
                    df = pd.read_csv(combined_file)
                    if 'def_id' in df.columns and 'gi' in df.columns:
                        combinations = set(zip(df['def_id'].astype(str), df['gi'].astype(str)))
                        already_scraped.update(combinations)
                        logger.info(f"    Found {len(combinations)} combinations for {year}")
                except Exception as e:
                    logger.error(f"    Error reading {combined_file}: {e}")
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
                        logger.error(f"    Error reading {batch_path}: {e}")
                
                if year_combinations > 0:
                    logger.info(f"  Found {year_combinations} combinations from {len(batch_files)} batch files for {year}")
        
        logger.info(f"Total unique combinations with data in CSV files: {len(already_scraped)}")
        return already_scraped

    def scrape_nba_video_details(self, master_record_path, context_measure="DEF_FGA", 
                               force_retry_failed=False):
        """Main scraper function with async processing"""
        
        # Load scrape log
        logger.info("=== LOADING SCRAPE LOG ===")
        self.load_scrape_log()
        
        # Load already scraped combinations from CSV files
        logger.info("=== LOADING EXISTING CSV DATA ===")
        csv_combinations = self.load_already_scraped_combinations_from_files()
        
        # Migrate CSV data to log if needed
        self.migrate_csv_data_to_log(csv_combinations)
        
        # Load master record
        try:
            master_record = pd.read_csv(master_record_path)
            master_record['TEAM_ID'] = master_record['TEAM_ID'].astype(int)
            master_record['PLAYER_ID'] = master_record['PLAYER_ID'].astype(int)
            master_record = master_record[master_record.year > 2024]
            logger.info(f"Loaded master record with {len(master_record)} rows")
        except Exception as e:
            logger.error(f"Error loading master record: {e}")
            return
        
        # Get unique combinations and filter
        unique_combinations = master_record[['PLAYER_ID', 'GAME_ID', 'TEAM_ID', 'PLAYER_NAME', 'year']].drop_duplicates()
        unique_combinations['scrape_key'] = unique_combinations['PLAYER_ID'].astype(str) + "_" + unique_combinations['GAME_ID'].astype(str)
        
        # Filter based on scrape log
        already_attempted = set(self.log_data.keys())
        unique_combinations['log_status'] = unique_combinations['scrape_key'].apply(
            lambda x: 'never_attempted' if x not in already_attempted else 
                     ('successful' if self.log_data[x]['success'] else 'failed')
        )
        
        never_attempted = unique_combinations[unique_combinations['log_status'] == 'never_attempted']
        previously_failed = unique_combinations[unique_combinations['log_status'] == 'failed']
        
        # Determine what to scrape
        if force_retry_failed:
            to_scrape_df = pd.concat([never_attempted, previously_failed])
            logger.info(f"Force retry enabled: Will attempt {len(to_scrape_df)} combinations")
        else:
            to_scrape_df = never_attempted
            logger.info(f"Will attempt {len(to_scrape_df)} never-attempted combinations")
        
        if len(to_scrape_df) == 0:
            logger.info("🎉 No combinations to scrape!")
            return
        
        # Process in batches
        logger.info("=== STARTING ASYNC SCRAPING PROCESS ===")
        
        total_batches = (len(to_scrape_df) + self.batch_size - 1) // self.batch_size
        successful_requests = 0
        failed_requests = 0
        total_records_saved = 0
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * self.batch_size
            end_idx = min((batch_idx + 1) * self.batch_size, len(to_scrape_df))
            batch_data = to_scrape_df.iloc[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_idx + 1}/{total_batches} ({len(batch_data)} items)")
            
            # Process batch asynchronously
            try:
                results = asyncio.run(self.scrape_batch_async(batch_data, context_measure))
                
                # Process results
                year_data = {}
                batch_successful = 0
                batch_failed = 0
                
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Exception in batch processing: {result}")
                        batch_failed += 1
                        continue
                    
                    processed_df, success, record_count = result
                    row = batch_data.iloc[i]
                    
                    if success:
                        batch_successful += 1
                        if processed_df is not None and not processed_df.empty:
                            year = row['year']
                            if year not in year_data:
                                year_data[year] = []
                            year_data[year].append(processed_df)
                    else:
                        batch_failed += 1
                
                # Save batch data
                if any(year_data.values()):
                    records_saved = self.save_batch_data_by_year(year_data, batch_idx + 1)
                    total_records_saved += records_saved
                
                successful_requests += batch_successful
                failed_requests += batch_failed
                
                logger.info(f"Batch {batch_idx + 1} complete: {batch_successful} successful, {batch_failed} failed")
                
                # Save log periodically
                if (batch_idx + 1) % 5 == 0:
                    self.save_scrape_log()
                
            except Exception as e:
                logger.error(f"Error processing batch {batch_idx + 1}: {e}")
                failed_requests += len(batch_data)
        
        # Save final log
        self.save_scrape_log()
        
        # Print summary
        logger.info("=== SCRAPING COMPLETE ===")
        logger.info(f"Attempted to scrape: {len(to_scrape_df)}")
        logger.info(f"Successful requests: {successful_requests}")
        logger.info(f"Failed requests: {failed_requests}")
        logger.info(f"New video records saved: {total_records_saved}")
        if len(to_scrape_df) > 0:
            logger.info(f"Success rate: {(successful_requests/len(to_scrape_df))*100:.1f}%")

    def migrate_csv_data_to_log(self, csv_combinations):
        """Migrate existing CSV data to the scrape log"""
        migrated_count = 0
        for player_id, game_id in csv_combinations:
            key = f"{player_id}_{game_id}"
            if key not in self.log_data:
                self.log_data[key] = {
                    "player_id": str(player_id),
                    "game_id": str(game_id),
                    "team_id": "unknown",
                    "player_name": "unknown",
                    "year": "unknown",
                    "timestamp": "migrated_from_csv",
                    "success": True,
                    "record_count": 1,
                    "error_msg": None
                }
                migrated_count += 1
        
        if migrated_count > 0:
            logger.info(f"Migrated {migrated_count} successful scrapes from CSV files to log")


def main():
    """Main function to run the optimized scraper"""
    scraper = NBAScraper(
        max_concurrent_requests=15,  # Adjust based on your system and API limits
        request_delay=0.05,  # Much smaller delay between requests
        batch_size=100,  # Larger batch size for efficiency
        output_dir="scraped_data"
    )
    
    scraper.scrape_nba_video_details(
        master_record_path='master_record.csv',
        context_measure="DEF_FGA",
        force_retry_failed=False
    )

if __name__ == "__main__":
    main()