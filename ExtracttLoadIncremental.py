import requests
import pandas as pd
from sqlalchemy import create_engine
import logging
import json

logging.basicConfig(level=logging.INFO)

# Database connection
engine = create_engine('postgresql+psycopg2://postgres:mypass@localhost/jet_usecase')

# Step 1: Find the latest comic ID already in the database
query = "SELECT MAX(num) FROM comics;"
latest_comic_id = pd.read_sql(query, engine).iloc[0, 0] or 1  # Start from 1 if empty

# Step 2: Fetch new comics starting from the latest comic ID
new_comics_data = []
i = latest_comic_id + 1

while True:
    try:
        urlComic = f"https://xkcd.com/{i}/info.0.json"
        comic = requests.get(urlComic, timeout=100)
        if not comic.ok:
            break  # Exit when there are no more comics
        new_comics_data.append(comic.json())

        if i == 403:  # XKCD skips comic #404 intentionally
            i += 2
        else:
            i += 1
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for comic ID {i}: {e}")
        break

# Step 3: Insert new comics into the database
if new_comics_data:
    df_new = pd.DataFrame(new_comics_data)

    # Handle potential dict fields by converting them to JSON strings
    for col in df_new.columns:
        if df_new[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df_new[col] = df_new[col].apply(json.dumps)

    # Insert new data to the database
    df_new.to_sql('comics', engine, if_exists='append', index=False, chunksize=1000)
    logging.info(f"Inserted {len(df_new)} new comics successfully!")
else:
    logging.info("No new comics available.")
