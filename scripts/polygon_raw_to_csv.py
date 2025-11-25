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
    breakpoint()

for symbol in os.listdir(RAW_POLYGON_PATH):
    filepath = RAW_POLYGON_PATH / symbol
    df = parse_file(filepath)
    # df.to_csv(str(CLEANED_DATA_PATH / f"{symbol}.csv"), index=False)
    # print(f"finished parsing {symbol}")
