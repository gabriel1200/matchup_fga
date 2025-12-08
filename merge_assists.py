import pandas as pd
frames = []
for year in range(2018,2026):
    for prefix in ['','ps']:
     
        year_base='https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/assists/{year}{prefix}/ast.csv'
        df = pd.read_csv(year_base)
        frames.append(df)

