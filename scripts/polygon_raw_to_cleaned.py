import os
import json
from pathlib import Path
from typing import Sequence

import pandas as pd

RAW_POLYGON_PATH = Path("rawdata/polygon")
CLEANED_DATA_PATH = Path("data/polygon")

def mkdir_if_not_exists(paths: Sequence[Path]) -> None:
    for path in paths:
        try:
            os.mkdir(path)
        except:
            pass

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

def resample_15min_hourly_daily(df):
    OPTIONS = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    df = df.copy()
    df.index = pd.to_datetime(df.datetime)
    return (
        df.resample("15T").agg(OPTIONS).reset_index(),
        df.resample("1H").agg(OPTIONS).reset_index(),
        df.resample("1D").agg(OPTIONS).reset_index(),
    )

for symbol in os.listdir(RAW_POLYGON_PATH):

    dfs = []

    for filename in sorted(os.listdir(RAW_POLYGON_PATH / symbol)):
        filepath = RAW_POLYGON_PATH / symbol / filename
        if not str(filepath).endswith(".json"):
            continue
        df = parse_file(filepath)
        dfs.append(df)
    
    df_minute = pd.concat(dfs).drop_duplicates()
    df_15, df_hourly, df_daily = resample_15min_hourly_daily(df_minute)

    mkdir_if_not_exists([CLEANED_DATA_PATH / interval for interval in ["minute", "15min", "hourly", "daily"]])

    df_minute.to_csv(str(CLEANED_DATA_PATH / f"minute/{symbol}.csv"), index=False)
    df_15.to_csv(str(CLEANED_DATA_PATH / f"15min/{symbol}.csv"), index=False)
    df_hourly.to_csv(str(CLEANED_DATA_PATH / f"hourly/{symbol}.csv"), index=False)
    df_daily.to_csv(str(CLEANED_DATA_PATH / f"daily/{symbol}.csv"), index=False)

    print(f"completed cleaning for {symbol}")


