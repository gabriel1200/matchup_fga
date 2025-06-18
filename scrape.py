import pandas as pd
import requests
import time
import os

def pull_data(url):
    headers = {
        "Host": "stats.nba.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://stats.nba.com/"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    json = response.json()

    if len(json["resultSets"]) == 1:
        data = json["resultSets"][0]["rowSet"]
        columns = json["resultSets"][0]["headers"]
    else:
        data = json["resultSets"]["rowSet"]
        columns = json["resultSets"]["headers"][1]['columnNames']
    
    return pd.DataFrame.from_records(data, columns=columns)

def get_matchups_for_date(season, date, mode='Offense', is_playoffs=False):
    stype = 'Playoffs' if is_playoffs else 'Regular%20Season'
    url = (
        'https://stats.nba.com/stats/leagueseasonmatchups?'
        f'DateFrom={date}&DateTo={date}&DefPlayerID=&DefTeamID=&LeagueID=00'
        f'&Matchup={mode}&OffPlayerID=&OffTeamID=&Outcome=&PORound=0&PerMode=Totals'
        f'&Season={season}&SeasonType={stype}'
    )
    return pull_data(url)

# === Pipeline Start ===
df = pd.read_csv("game_dates.csv")
df['season_end_year'] = df['season'].str.split('-').str[0].astype(int) + 1
filtered_df = df[df['season_end_year'] >= 2017]
unique_dates = filtered_df[['date', 'season', 'season_end_year', 'playoffs']].drop_duplicates()

mode ='Defense'  # Change to 'Defense' if needed

output_dir = 'matchup_outputs'
os.makedirs(output_dir, exist_ok=True)

grouped = unique_dates.groupby('season_end_year')
for year, group in grouped:
    filename = os.path.join(output_dir, f"matchups_{mode.lower()}_{year}.csv")
    
    # If file exists, load it to get already scraped dates
    if os.path.exists(filename):
        existing_df = pd.read_csv(filename)
        scraped_dates = set(existing_df['game_date'].unique())
    else:
        existing_df = None
        scraped_dates = set()

    all_matchups = []

    for _, row in group.iterrows():
        date = row['date']
        if date in scraped_dates:
            print(f"⏩ Skipping {date} (already scraped)")
            continue

        season = row['season']
        is_playoffs = row['playoffs']

        print(f"🔍 Pulling {mode} matchup for {date} ({'Playoffs' if is_playoffs else 'Regular Season'})")

        try:
            matchups_df = get_matchups_for_date(season, date, mode=mode, is_playoffs=is_playoffs)
            matchups_df['game_date'] = date
            matchups_df['season'] = season
            matchups_df['mode'] = mode
            matchups_df['playoffs'] = is_playoffs
            all_matchups.append(matchups_df)
        except Exception as e:
            print(f"❌ Error on {date} ({season}) - {e}")

        time.sleep(0.3)

    if all_matchups:
        new_data = pd.concat(all_matchups, ignore_index=True)
        if existing_df is not None:
            combined = pd.concat([existing_df, new_data], ignore_index=True).drop_duplicates()
        else:
            combined = new_data
        combined.to_csv(filename, index=False)
        print(f"✅ Updated: {filename}")
    else:
        print(f"⚠️ No new data to update for {year}.")
