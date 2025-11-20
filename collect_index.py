import pandas as pd
import glob
import os

# --- Paths ---
regular_path = '../../web_app/data/pbp/regular_season/*.parquet'
playoffs_path = '../../web_app/data/pbp/playoffs/*.parquet'

columns = [
    'teamId', 'actionNumber', 'scoreHome', 'scoreAway',
    'game_id', 'playoffs', 'team'
]


def extract_year(filename):
    """
    Example filename: ATL_2021_rs.parquet
    Splits by '_' and returns second token => '2021'
    """
    base = os.path.basename(filename)
    parts = base.replace(".parquet", "").split("_")
    return parts[1]  # second token


def save_by_season(files, base_output_dir, columns):
    """
    Load all parquet files, group by year, save each year's combined df.
    """
    season_dfs = {}

    for f in files:
        try:
            df = pd.read_parquet(f)
            print(df.columns)
           
            shots=['3pt','2pt']
            df=df[df['actionType'].isin(shots)]
          
         
            year = extract_year(f)
            df['source_file'] = os.path.basename(f)

            if year not in season_dfs:
                season_dfs[year] = []
            season_dfs[year].append(df)

        except Exception as e:
            print(f"Error reading file {f}: {e}")

    # Save each season separately
    for year, df_list in season_dfs.items():
        combined = pd.concat(df_list, ignore_index=True)[columns]

        # Ensure directory exists
        out_dir = os.path.join(base_output_dir, year)
        os.makedirs(out_dir, exist_ok=True)

        out_path = os.path.join(out_dir, f"{year}_combined.csv")
        combined.sort_values(by=['game_id','actionNumber'],inplace=True)
        print(combined.head(1))
        print(combined.columns)
        print(combined.head()['teamId'])
        combined.to_csv(out_path, index=False)

        print(f"Saved {year} -> {out_path} ({len(combined)} rows)")


# --- Process Regular Season ---
regular_files = glob.glob(regular_path)
save_by_season(
    regular_files,
    base_output_dir='indexing/regular_season',
    columns=columns
)

# --- Process Playoffs ---
playoff_files = glob.glob(playoffs_path)
save_by_season(
    playoff_files,
    base_output_dir='indexing/playoffs',
    columns=columns
)
