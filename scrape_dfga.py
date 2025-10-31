import pandas as pd
import requests
import sys 
import time
import os
import random
from urllib.parse import quote
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NBAScraper:
    def __init__(self, save_every=10):
        self.save_every = save_every
        self.links_file = "saved_urls.csv"
        # List of context measures to scrape for each game
        self.context_measures = ['DFGA']
        
        # User agents for rotation
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ]
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Load existing data
        self.all_links = set()
        # self.scraped_games is deprecated, using self.scraped_combinations
        self.scraped_combinations = set()
        self.load_existing_data()
        
    def get_random_headers(self):
        """Generate randomized headers for each request"""
        return {
            "Host": "stats.nba.com",
            "User-Agent": random.choice(self.user_agents),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": random.choice([
                "en-US,en;q=0.9",
                "en-US,en;q=0.5", 
                "en-GB,en-US;q=0.9,en;q=0.8",
                "en-US,en;q=0.8"
            ]),
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": random.choice([
                "https://stats.nba.com/"
            ]),
            "Cache-Control": random.choice(["no-cache"]),
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin"
        }
    
    def load_existing_data(self):
        """Load previously scraped data"""
        if os.path.exists(self.links_file):
            try:
                df_links = pd.read_csv(self.links_file)
                # Ensure ContextMeasure column exists for backwards compatibility
                if "ContextMeasure" not in df_links.columns:
                    logger.warning("Old 'saved_urls.csv' format. Will not skip any old games.")
                    return

                self.all_links = set(zip(
                    df_links["GAME_ID"].astype(str), 
                    df_links["ContextMeasure"], 
                    df_links["URL"]
                ))
                self.scraped_combinations = set(zip(
                    df_links["GAME_ID"].astype(str), 
                    df_links["ContextMeasure"]
                ))
                logger.info(f"Loaded {len(self.scraped_combinations)} previously scraped game/measure combinations")
            except Exception as e:
                logger.error(f"Error loading existing data: {e}")
                
    def extract_video_urls(self, data):
        """Parse JSON data to extract video URLs"""
        video_urls = []
        try:
            video_details_list = data.get('resultSets', {}).get('Meta', {}).get('videoUrls', [])
            for video in video_details_list:
                if 'surl' in video and video['surl'] is not None:
                    video_urls.append(video['surl'])
        except (KeyError, TypeError, AttributeError) as e:
            logger.warning(f"Error extracting video URLs: {e}")
        return list(set(video_urls))  # Remove duplicates
    
    def scrape_single_game(self, game_data, context_measure):
        """Scrape a single game/measure combo with retry logic and error handling"""
        game_id, season, is_playoffs = game_data
        game_id_str = str(game_id)
        
        # Skip if already scraped
        if (game_id_str, context_measure) in self.scraped_combinations:
            return game_id_str, context_measure, [], f"Already scraped"
            
        stype = "Playoffs" if is_playoffs else "Regular Season"
        
        # Build URL with proper encoding, including ContextMeasure
        url = (
            f"https://stats.nba.com/stats/videodetailsasset?"
            f"AheadBehind=&CFID=&CFPARAMS=&ClutchTime=&Conference=&ContextFilter=&ContextMeasure={quote(context_measure)}"
            f"&DateFrom=&DateTo=&Division=&EndPeriod=0&EndRange=&GROUP_ID="
            f"&GameEventID=&GameID=00{game_id}&GameSegment=&GroupID=&GroupMode=&GroupQuantity=5"
            f"&LastNGames=0&LeagueID=00&Location=&Month=0&OnOff=&OppPlayerID=&OpponentTeamID=0"
            f"&Outcome=&PORound=0&Period=0&PlayerID=0&PlayerID1=&PlayerID2=&PlayerID3=&PlayerID4="
            f"&PlayerID5=&PlayerPosition=&PointDiff=&Position=&RangeType=0&RookieYear=&Season={season}"
            f"&SeasonSegment=&SeasonType={quote(stype)}&ShotClockRange=&StartPeriod=0&StartRange=0&StarterBench="
            f"&TeamID=0&VsConference=&VsDivision=&VsPlayerID1=&VsPlayerID2=&VsPlayerID3=&VsPlayerID4="
            f"&VsPlayerID5=&VsTeamID="
        )
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Random delay to avoid rate limiting
                time.sleep(random.uniform(0.5, 1.5))
                
                headers = self.get_random_headers()
                response = self.session.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                links = self.extract_video_urls(data)
                
                return game_id_str, context_measure, links, f"Success: {len(links)} URLs found"
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    return game_id_str, context_measure, [], f"Failed after {max_retries} attempts: {str(e)}"
                logger.warning(f"Attempt {attempt + 1} failed for game {game_id} [{context_measure}]: {e}")
                time.sleep(random.uniform(2, 5))  # Exponential backoff
            except Exception as e:
                return game_id_str, context_measure, [], f"Unexpected error: {str(e)}"
        
        return game_id_str, context_measure, [], "Max retries exceeded"
    
    def save_progress(self, force=False):
        """Save current progress to CSV"""
        try:
            df_links = pd.DataFrame(sorted(self.all_links), columns=["GAME_ID", "ContextMeasure", "URL"])
            df_links.to_csv(self.links_file, index=False)
            if force:
                logger.info("💾 Final save completed")
            else:
                logger.info(f"💾 Checkpoint saved - {len(self.all_links)} total links")
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
    
    def run(self):
        """Main scraping function with serial processing"""
        logger.info("Loading game data...")
        
        # Load data
        game_frame = pd.read_csv(
            "https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv"
        )
        
        # Filter and prepare data
        game_frame = game_frame[game_frame.date >= 20251021]
        games = game_frame[["GAME_ID", "season", "playoffs"]].drop_duplicates()
        
        # Convert to list of tuples for processing
        games_to_scrape = [
            (row["GAME_ID"], row["season"], row["playoffs"]) 
            for _, row in games.iterrows()
        ]

        # Create a list of all jobs (game + context_measure)
        jobs_to_scrape = []
        for game_data in games_to_scrape:
            game_id_str = str(game_data[0])
            for measure in self.context_measures:
                if (game_id_str, measure) not in self.scraped_combinations:
                    jobs_to_scrape.append((game_data, measure))
        
        logger.info(f"Total games in dataset: {len(games)}")
        logger.info(f"Total jobs to scrape (game * measure): {len(jobs_to_scrape)}")
        logger.info(f"Previously scraped combinations: {len(self.scraped_combinations)}")
        
        if not jobs_to_scrape:
            logger.info("No new jobs to scrape!")
            return
            
        # Process jobs serially
        jobs_processed = 0
        successful_jobs = 0
        
        for game_data, context_measure in jobs_to_scrape:
            game_id = str(game_data[0])
            season = game_data[1]
            is_playoffs = game_data[2]
            stype = "Playoffs" if is_playoffs else "Regular Season"

            try:
                game_id_result, measure_result, links, status = self.scrape_single_game(game_data, context_measure)

                if links:  # Success case
                    for link in links:
                        self.all_links.add((game_id_result, measure_result, link))
                    self.scraped_combinations.add((game_id_result, measure_result))
                    successful_jobs += 1
                    logger.info(f"✅ {game_id} ({season}, {stype}) [{measure_result}]: {len(links)} URLs - {status}")
                else:
                    logger.warning(f"❌ {game_id} ({season}, {stype}) [{measure_result}]: {status}")

            except Exception as e:
                logger.error(f"❌ {game_id} ({season}, {stype}) [{context_measure}]: Exception - {str(e)}")

            jobs_processed += 1

            # Periodic progress save
            if jobs_processed % self.save_every == 0:
                self.save_progress()
                logger.info(f"Progress: {jobs_processed}/{len(jobs_to_scrape)} jobs processed")
        
        # Final save
        self.save_progress(force=True)
        
        logger.info("✅ Scraping complete!")
        logger.info(f"📊 Summary:")
        logger.info(f"   - Jobs processed: {jobs_processed}")
        logger.info(f"   - Successful jobs: {successful_jobs}")
        logger.info(f"   - Failed/Skipped jobs: {jobs_processed - successful_jobs}")
        logger.info(f"   - Total unique links: {len(self.all_links)}")

if __name__ == "__main__":
    # Configuration
    SAVE_EVERY = 10  # Save progress every N jobs
    
    scraper = NBAScraper(save_every=SAVE_EVERY)
    scraper.run()

