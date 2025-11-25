"""
Notes on polygon.io
- max num rows is 50000, so we're only able to get ~3 months of data at a time
- for BTC, markets are 24/7, so we need more files here
"""
import os
import json
from pathlib import Path

import pandas as pd

RAW_POLYGON_PATH = Path("rawdata/polygon")
CLEANED_DATA_PATH = Path("data/polygon")

def parse_file(filepath: Path) -> pd.DataFrame:
    with open(filepath, "r") as f:
        raw_string = f.read()
    raw_data = json.loads(raw_string)
    data = []
    for row in raw_data["results"]:
        data.append([row["o"], row["h"], row["l"], row["c"], row["v"], row["t"]])
    df = pd.DataFrame(data, columns=["open", "high", "low", "close", "volume", "timestamp"])
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def display_datetime_range(filepath: Path) -> None:
    # show earliest datetime and latest datetime
    df = parse_file(filepath)
    print(filepath, df["datetime"].min(), df["datetime"].max())

for symbol in os.listdir(RAW_POLYGON_PATH):

    dfs = []

    for filename in sorted(os.listdir(RAW_POLYGON_PATH / symbol)):
        filepath = RAW_POLYGON_PATH / symbol / filename
        if not str(filepath).endswith(".json"):
            continue
        df = parse_file(filepath)
        dfs.append(df)
    
    df_agg = pd.concat(dfs).drop_duplicates()
    df_agg.to_csv(str(CLEANED_DATA_PATH / f"{symbol}.csv"), index=False)
    print(f"completed cleaning for {symbol}")

        
