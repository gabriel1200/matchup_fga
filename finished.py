import requests
import pandas as pd
import time
import os
from datetime import datetime
import sys
import json
import tempfile
import shutil
from typing import Dict, Set, Tuple, Optional
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SafeJSONManager:
    """
    Manages JSON files with atomic writes and corruption recovery
    """
    
    @staticmethod
    def safe_write_json(data: dict, filepath: str, backup_count: int = 3) -> bool:
        """
        Safely write JSON data with atomic operations and backups
        """
        try:
            # Create backup of existing file if it exists
            if os.path.exists(filepath) and backup_count > 0:
                SafeJSONManager._rotate_backups(filepath, backup_count)
            
            # Write to temporary file first
            temp_filepath = f"{filepath}.tmp"
            with open(temp_filepath, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Atomic move to final location
            shutil.move(temp_filepath, filepath)
            
            # Verify the file can be read back
            with open(filepath, 'r') as f:
                json.load(f)
            
            return True
            
        except Exception as e:
            logger.error(f"Error safely writing JSON to {filepath}: {e}")
            # Clean up temp file if it exists
            if os.path.exists(f"{filepath}.tmp"):
                try:
                    os.remove(f"{filepath}.tmp")
                except:
                    pass
            return False
    
    @staticmethod
    def _rotate_backups(filepath: str, backup_count: int):
        """
        Rotate backup files, keeping the most recent N backups
        """
        # Rotate existing backups
        for i in range(backup_count - 1, 0, -1):
            old_backup = f"{filepath}.backup{i}"
            new_backup = f"{filepath}.backup{i + 1}"
            if os.path.exists(old_backup):
                if os.path.exists(new_backup):
                    os.remove(new_backup)
                shutil.move(old_backup, new_backup)
        
        # Create new backup from current file
        if os.path.exists(filepath):
            backup_path = f"{filepath}.backup1"
            if os.path.exists(backup_path):
                os.remove(backup_path)
            shutil.copy2(filepath, backup_path)
    
    @staticmethod
    def safe_load_json(filepath: str) -> dict:
        """
        Safely load JSON with automatic corruption recovery
        """
        if not os.path.exists(filepath):
            logger.info(f"No existing JSON file found at {filepath}")
            return {}
        
        # Try to load the main file
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            logger.info(f"Successfully loaded JSON from {filepath} with {len(data)} entries")
            return data
        except json.JSONDecodeError as e:
            logger.warning(f"JSON corruption detected in {filepath}: {e}")
            return SafeJSONManager._recover_from_corruption(filepath)
        except Exception as e:
            logger.error(f"Unexpected error loading {filepath}: {e}")
            return SafeJSONManager._recover_from_corruption(filepath)
    
    @staticmethod
    def _recover_from_corruption(filepath: str) -> dict:
        """
        Attempt to recover from JSON corruption using backups
        """
        logger.info("Attempting JSON corruption recovery...")
        
        # Try backup files
        for i in range(1, 6):  # Try up to 5 backups
            backup_path = f"{filepath}.backup{i}"
            if os.path.exists(backup_path):
                try:
                    with open(backup_path, 'r') as f:
                        data = json.load(f)
                    logger.info(f"Successfully recovered from backup {backup_path} with {len(data)} entries")
                    
                    # Restore the good backup as the main file
                    shutil.copy2(backup_path, filepath)
                    return data
                except json.JSONDecodeError:
                    logger.warning(f"Backup {backup_path} is also corrupted, trying next...")
                    continue
                except Exception as e:
                    logger.warning(f"Error reading backup {backup_path}: {e}")
                    continue
        
        # If all backups failed, try manual recovery
        logger.warning("All backups failed, attempting manual recovery...")
        return SafeJSONManager._manual_recovery(filepath)
    
    @staticmethod
    def _manual_recovery(filepath: str) -> dict:
        """
        Last resort: try to manually recover what we can from corrupted JSON
        """
        try:
            with open(filepath, 'r') as f:
                content = f.read()
            
            # Try to find complete entries using regex
            import re
            pattern = r'"(\d+_\d+)"\s*:\s*(\{(?:[^{}]|(?:\{[^{}]*\}))*\})'
            matches = re.finditer(pattern, content)
            
            recovered_data = {}
            for match in matches:
                key = match.group(1)
                entry_json = match.group(2)
                try:
                    entry_data = json.loads(entry_json)
                    recovered_data[key] = entry_data
                except json.JSONDecodeError:
                    continue
            
            if recovered_data:
                logger.info(f"Manual recovery found {len(recovered_data)} entries")
                return recovered_data
            else:
                logger.warning("Manual recovery found no valid entries")
                return {}
                
        except Exception as e:
            logger.error(f"Manual recovery failed: {e}")
            return {}

class ImprovedNBAScraper:
    """
    Improved NBA video details scraper with enhanced error handling and corruption resistance
    """
    
    def __init__(self, master_record_path: str, output_dir: str = "scraped_data", 
                 log_file: str = "scrape_log.json"):
        self.master_record_path = master_record_path
        self.output_dir = output_dir
        self.log_file = log_file
        self.json_manager = SafeJSONManager()
        
        # Load master record
        self.master_record = self._load_master_record()
        
        # Load scrape log
        self.log_data = self.json_manager.safe_load_json(self.log_file)
        
        # Migrate CSV data to log if needed
        self._migrate_csv_data_to_log()
    
    def _load_master_record(self) -> pd.DataFrame:
        """Load and prepare master record"""
        try:
            df = pd.read_csv(self.master_record_path)
            df['TEAM_ID'] = df['TEAM_ID'].astype(int)
            df['PLAYER_ID'] = df['PLAYER_ID'].astype(int)
            df = df[df.year >= 2019]
            logger.info(f"Loaded master record with {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error loading master record: {e}")
            raise
    
    def _migrate_csv_data_to_log(self):
        """Migrate existing CSV data to log"""
        csv_combinations = self._load_existing_csv_combinations()
        if not csv_combinations:
            return
        
        migrated_count = 0
        for player_id, game_id in csv_combinations:
            key = f"{player_id}_{game_id}"
            if key not in self.log_data:
                self.log_data[key] = {
                    "player_id": str(player_id),
                    "game_id": str(game_id),
                    "team_id": "unknown",
                    "player_name": "unknown",
                    "year": 2025,
                    "timestamp": "migrated_from_csv",
                    "success": True,
                    "record_count": 1,
                    "has_data": True,
                    "error_msg": None
                }
                migrated_count += 1
        
        if migrated_count > 0:
            logger.info(f"Migrated {migrated_count} successful scrapes from CSV files to log")
            self._save_log()
    
    def _load_existing_csv_combinations(self) -> Set[Tuple[str, str]]:
        """Load all existing player-game combinations from CSV files"""
        if not os.path.exists(self.output_dir):
            return set()
        
        combinations = set()
        year_dirs = [d for d in os.listdir(self.output_dir) 
                    if d.startswith('year_') and os.path.isdir(os.path.join(self.output_dir, d))]
        
        for year_dir in year_dirs:
            year_path = os.path.join(self.output_dir, year_dir)
            year = year_dir.replace('year_', '')
            
            # Check combined file first
            combined_file = os.path.join(year_path, f"combined_video_details_{year}.csv")
            if os.path.exists(combined_file):
                try:
                    df = pd.read_csv(combined_file)
                    if 'def_id' in df.columns and 'gi' in df.columns:
                        year_combinations = set(zip(df['def_id'].astype(str), df['gi'].astype(str)))
                        combinations.update(year_combinations)
                except Exception as e:
                    logger.warning(f"Error reading {combined_file}: {e}")
            else:
                # Check batch files
                batch_files = [f for f in os.listdir(year_path) 
                             if f.startswith('batch_') and f.endswith('.csv')]
                for batch_file in batch_files:
                    try:
                        batch_path = os.path.join(year_path, batch_file)
                        df = pd.read_csv(batch_path)
                        if 'def_id' in df.columns and 'gi' in df.columns:
                            batch_combinations = set(zip(df['def_id'].astype(str), df['gi'].astype(str)))
                            combinations.update(batch_combinations)
                    except Exception as e:
                        logger.warning(f"Error reading {batch_path}: {e}")
        
        logger.info(f"Found {len(combinations)} existing combinations in CSV files")
        return combinations
    
    def _save_log(self) -> bool:
        """Safely save the scrape log"""
        return self.json_manager.safe_write_json(self.log_data, self.log_file)
    
    def _update_log(self, player_id: str, game_id: str, team_id: str, player_name: str, 
                   year: int, success: bool, record_count: int = 0, has_data: bool = None, 
                   error_msg: str = None):
        """Update the scrape log with a new attempt"""
        key = f"{player_id}_{game_id}"
        self.log_data[key] = {
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
    
    def _fetch_video_details(self, game_id: str, player_id: str, team_id: str, 
                           context_measure: str = "DEF_FGA") -> Optional[dict]:
        """Fetch video details from NBA API with improved error handling"""
        base_url = "https://stats.nba.com/stats/videodetailsasset"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://www.nba.com",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.nba.com"
        }
        
        params = {
            "GameID": f"00{game_id}",
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
        
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                response = requests.get(base_url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limited
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Rate limited for Player {player_id}, Game {game_id}. Waiting {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    logger.warning(f"Request failed with status {response.status_code} for Player {player_id}, Game {game_id}")
                    if attempt == max_retries - 1:
                        return None
                    
            except requests.RequestException as e:
                logger.warning(f"Request error for Player {player_id}, Game {game_id} (attempt {attempt + 1}): {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(base_delay * (attempt + 1))
        
        return None
    
    def _process_video_data(self, video_json: dict, player_id: str, team_id: str, 
                          player_name: str, year: int) -> Optional[pd.DataFrame]:
        """Process video JSON response into DataFrame"""
        try:
            if not (video_json and 'resultSets' in video_json and 'playlist' in video_json['resultSets']):
                return None
            
            playlist = video_json['resultSets']['playlist']
            if not playlist:
                return None
            
            df = pd.DataFrame(playlist)
            if df.empty or not all(col in df.columns for col in ['gi', 'ei', 'dsc']):
                return None
            
            df = df[['gi', 'ei', 'dsc']]
            df['def_id'] = player_id
            df['team_id'] = team_id
            df['player_name'] = player_name
            df['year'] = year
            return df
            
        except Exception as e:
            logger.error(f"Error processing video data for Player {player_id}: {e}")
            return None
    
    def _save_batch_data(self, year_data_dict: Dict[int, list], batch_num: int) -> int:
        """Save batch data organized by year with improved error handling"""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        total_records_saved = 0
        
        for year, data_list in year_data_dict.items():
            if not data_list:
                continue
            
            try:
                # Create year directory
                year_dir = os.path.join(self.output_dir, f"year_{year}")
                os.makedirs(year_dir, exist_ok=True)
                
                # Combine data
                combined_df = pd.concat(data_list, ignore_index=True)
                
                # Save with atomic write
                filename = f"batch_{batch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                filepath = os.path.join(year_dir, filename)
                temp_filepath = f"{filepath}.tmp"
                
                # Write to temp file first
                combined_df.to_csv(temp_filepath, index=False)
                
                # Atomic move
                shutil.move(temp_filepath, filepath)
                
                records_count = len(combined_df)
                total_records_saved += records_count
                logger.info(f"Saved {year} batch {batch_num} with {records_count} records to {filename}")
                
            except Exception as e:
                logger.error(f"Error saving batch data for year {year}: {e}")
                # Clean up temp file if it exists
                temp_path = os.path.join(year_dir, f"batch_{batch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv.tmp")
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except:
                        pass
        
        return total_records_saved
    
    def analyze_scrape_status(self):
        """Analyze current scrape status"""
        unique_combinations = self.master_record[['PLAYER_ID', 'GAME_ID', 'TEAM_ID', 'PLAYER_NAME', 'year']].drop_duplicates()
        total_combinations = len(unique_combinations)
        
        unique_combinations['scrape_key'] = unique_combinations['PLAYER_ID'].astype(str) + "_" + unique_combinations['GAME_ID'].astype(str)
        
        def categorize_attempt(scrape_key):
            if scrape_key not in self.log_data:
                return 'never_attempted'
            entry = self.log_data[scrape_key]
            if not entry['success']:
                return 'failed'
            elif entry.get('has_data', entry['record_count'] > 0):
                return 'successful_with_data'
            else:
                return 'successful_no_data'
        
        unique_combinations['log_status'] = unique_combinations['scrape_key'].apply(categorize_attempt)
        
        status_counts = unique_combinations['log_status'].value_counts()
        
        logger.info("=== SCRAPE STATUS ANALYSIS ===")
        logger.info(f"Total combinations: {total_combinations}")
        for status, count in status_counts.items():
            logger.info(f"{status}: {count} combinations")
        
        # Show breakdown by year
        for status in status_counts.index:
            status_data = unique_combinations[unique_combinations['log_status'] == status]
            if len(status_data) > 0:
                logger.info(f"\n{status} by year:")
                year_counts = status_data['year'].value_counts().sort_index()
                for year, count in year_counts.items():
                    logger.info(f"  {year}: {count} combinations")
        
        return unique_combinations
    
    def scrape(self, context_measure: str = "DEF_FGA", delay_between_requests: float = 2.0,
              batch_size: int = 50, save_log_frequency: int = 10, 
              force_retry_failed: bool = False, retry_no_data: bool = False):
        """
        Main scraping function with improved error handling and progress tracking
        """
        logger.info("=== STARTING IMPROVED NBA VIDEO SCRAPER ===")
        
        # Analyze current status
        unique_combinations = self.analyze_scrape_status()
        
        # Determine what to scrape
        to_scrape_parts = [unique_combinations[unique_combinations['log_status'] == 'never_attempted']]
        
        if force_retry_failed:
            failed_combinations = unique_combinations[unique_combinations['log_status'] == 'failed']
            to_scrape_parts.append(failed_combinations)
            logger.info(f"Force retry failed enabled: Including {len(failed_combinations)} failed attempts")
        
        if retry_no_data:
            no_data_combinations = unique_combinations[unique_combinations['log_status'] == 'successful_no_data']
            to_scrape_parts.append(no_data_combinations)
            logger.info(f"Retry no data enabled: Including {len(no_data_combinations)} no-data attempts")
        
        to_scrape_df = pd.concat(to_scrape_parts) if len(to_scrape_parts) > 1 else to_scrape_parts[0]
        
        if len(to_scrape_df) == 0:
            logger.info("🎉 No combinations to scrape!")
            return
        
        logger.info(f"Will attempt {len(to_scrape_df)} combinations")
        
        # Initialize tracking variables
        year_data = {}
        successful_requests = 0
        failed_requests = 0
        no_data_requests = 0
        batch_num = self._get_next_batch_number()
        total_records_saved = 0
        
        logger.info(f"Starting with batch number: {batch_num}")
        
        # Process each combination
        for idx, (_, row) in enumerate(to_scrape_df.iterrows()):
            try:
                player_id = str(row['PLAYER_ID'])
                game_id = str(row['GAME_ID'])
                team_id = str(row['TEAM_ID'])
                player_name = row['PLAYER_NAME']
                year = row['year']
                
                if (idx + 1) % 50 == 0:
                    logger.info(f"Processing {idx + 1}/{len(to_scrape_df)}: Player {player_name} ({player_id}) in Game {game_id} - {year}")
                
                # Fetch video details
                video_json = self._fetch_video_details(game_id, player_id, team_id, context_measure)
                
                if video_json:
                    processed_df = self._process_video_data(video_json, player_id, team_id, player_name, year)
                    
                    if processed_df is not None and not processed_df.empty:
                        if year not in year_data:
                            year_data[year] = []
                        
                        year_data[year].append(processed_df)
                        successful_requests += 1
                        record_count = len(processed_df)
                        
                        self._update_log(player_id, game_id, team_id, player_name, year, 
                                       True, record_count, has_data=True)
                    else:
                        no_data_requests += 1
                        self._update_log(player_id, game_id, team_id, player_name, year, 
                                       True, 0, has_data=False)
                else:
                    failed_requests += 1
                    self._update_log(player_id, game_id, team_id, player_name, year, 
                                   False, 0, error_msg="API request failed")
                
                # Save log periodically
                if (idx + 1) % save_log_frequency == 0:
                    if not self._save_log():
                        logger.error(f"Failed to save log at iteration {idx + 1}")
                
                # Save batch data when threshold is reached
                if successful_requests > 0 and successful_requests % batch_size == 0:
                    records_saved = self._save_batch_data(year_data, batch_num)
                    total_records_saved += records_saved
                    year_data = {}
                    batch_num += 1
                
                # Rate limiting
                if idx < len(to_scrape_df) - 1:
                    time.sleep(delay_between_requests)
                    
            except KeyboardInterrupt:
                logger.info("Scraping interrupted by user. Saving progress...")
                break
            except Exception as e:
                logger.error(f"Unexpected error processing row {idx}: {e}")
                continue
        
        # Save any remaining data
        if any(year_data.values()):
            records_saved = self._save_batch_data(year_data, batch_num)
            total_records_saved += records_saved
        
        # Final log save
        if not self._save_log():
            logger.error("Failed to save final log")
        
        # Print summary
        logger.info("=== SCRAPING COMPLETE ===")
        logger.info(f"Attempted to scrape: {len(to_scrape_df)}")
        logger.info(f"Successful requests with data: {successful_requests}")
        logger.info(f"Successful requests with no data: {no_data_requests}")
        logger.info(f"Failed requests: {failed_requests}")
        logger.info(f"New video records saved: {total_records_saved}")
        
        if len(to_scrape_df) > 0:
            success_rate = ((successful_requests + no_data_requests) / len(to_scrape_df)) * 100
            logger.info(f"Overall success rate: {success_rate:.1f}%")
            if successful_requests > 0:
                data_rate = (successful_requests / len(to_scrape_df)) * 100
                logger.info(f"Data found rate: {data_rate:.1f}%")
    
    def _get_next_batch_number(self) -> int:
        """Get the next batch number by examining existing files"""
        if not os.path.exists(self.output_dir):
            return 1
        
        existing_batches = []
        for year_dir in os.listdir(self.output_dir):
            if year_dir.startswith('year_'):
                year_path = os.path.join(self.output_dir, year_dir)
                if os.path.isdir(year_path):
                    batch_files = [f for f in os.listdir(year_path) if f.startswith('batch_')]
                    for batch_file in batch_files:
                        try:
                            batch_num_str = batch_file.split('_')[1]
                            existing_batches.append(int(batch_num_str))
                        except (ValueError, IndexError):
                            pass
        
        return max(existing_batches) + 1 if existing_batches else 1
    
    def combine_batches_by_year(self):
        """Combine all batch files within each year into a single CSV per year"""
        if not os.path.exists(self.output_dir):
            logger.info(f"Output directory {self.output_dir} does not exist")
            return
        
        year_dirs = [d for d in os.listdir(self.output_dir) 
                    if d.startswith('year_') and os.path.isdir(os.path.join(self.output_dir, d))]
        
        if not year_dirs:
            logger.info("No year directories found")
            return
        
        logger.info(f"Found {len(year_dirs)} year directories")
        
        for year_dir in sorted(year_dirs):
            year_path = os.path.join(self.output_dir, year_dir)
            year = year_dir.replace('year_', '')
            
            # Check if combined file already exists
            combined_file = os.path.join(year_path, f"combined_video_details_{year}.csv")
            if os.path.exists(combined_file):
                logger.info(f"Combined file already exists for {year}, skipping...")
                continue
            
            # Find batch files
            batch_files = [f for f in os.listdir(year_path) 
                         if f.startswith('batch_') and f.endswith('.csv')]
            
            if not batch_files:
                logger.info(f"No batch files found for {year}")
                continue
            
            logger.info(f"Processing {year}: Found {len(batch_files)} batch files")
            
            try:
                # Load and combine all batches
                year_batches = []
                for file in batch_files:
                    file_path = os.path.join(year_path, file)
                    df = pd.read_csv(file_path)
                    year_batches.append(df)
                
                if year_batches:
                    final_df = pd.concat(year_batches, ignore_index=True)
                    
                    # Remove duplicates
                    initial_count = len(final_df)
                    final_df = final_df.drop_duplicates()
                    final_count = len(final_df)
                    
                    if initial_count != final_count:
                        logger.info(f"Removed {initial_count - final_count} duplicate records for {year}")
                    
                    # Save with atomic write
                    temp_path = f"{combined_file}.tmp"
                    final_df.to_csv(temp_path, index=False)
                    shutil.move(temp_path, combined_file)
                    
                    logger.info(f"✓ Combined file saved: {combined_file} with {final_count} total records")
                    
            except Exception as e:
                logger.error(f"Error combining batches for {year}: {e}")
    
    def get_summary(self):
        """Print a summary of collected data by year."""
        if not os.path.exists(self.output_dir):
            logger.info(f"Output directory {self.output_dir} does not exist.")
            return

        year_dirs = [d for d in os.listdir(self.output_dir)
                     if d.startswith('year_') and os.path.isdir(os.path.join(self.output_dir, d))]

        if not year_dirs:
            logger.info("No year directories found.")
            return

        logger.info("\n=== DATA SUMMARY BY YEAR ===")
        total_records = 0

        for year_dir in sorted(year_dirs):
            year = year_dir.replace('year_', '')
            year_path = os.path.join(self.output_dir, year_dir)

            # Look for combined file first, otherwise count batch files
            combined_file = os.path.join(year_path, f"combined_video_details_{year}.csv")

            if os.path.exists(combined_file):
                try:
                    df = pd.read_csv(combined_file)
                    record_count = len(df)
                    logger.info(f"{year}: {record_count:,} records (combined)")
                except Exception as e:
                    logger.warning(f"Error reading combined file {combined_file}: {e}")
                    record_count = 0 # reset record count if there was an error
            else:
                # Count records in batch files
                batch_files = [f for f in os.listdir(year_path) if f.startswith('batch_') and f.endswith('.csv')]
                record_count = 0
                num_batch_files = 0
                for file in batch_files:
                    file_path = os.path.join(year_path, file)
                    try:
                        df = pd.read_csv(file_path)
                        record_count += len(df)
                        num_batch_files += 1
                    except Exception as e:
                        logger.warning(f"Error reading batch file {file_path}: {e}")
                logger.info(f"{year}: {record_count:,} records ({num_batch_files} batch files)")
            
            total_records += record_count

        logger.info(f"\nTotal records across all years: {total_records:,}")