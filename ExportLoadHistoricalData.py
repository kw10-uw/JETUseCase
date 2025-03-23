import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging
import json

logging.basicConfig(level=logging.INFO)

# Get the first comic 
url = "https://xkcd.com/1/info.0.json"
comics = requests.get(url, timeout=100)
comicsJSON = comics.json()

 # Combine all data from JSON into a list
comics_data = [comicsJSON] 
i = 2

# Get all comics 
while True:
    try:
        urlComic = f"https://xkcd.com/{i}/info.0.json"
        comic = requests.get(urlComic, timeout=100)
        if not comic.ok:
            break
        comics_data.append(comic.json()) 

        # Skip 404
        if i == 403:
            i += 2
        else:
            i += 1
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for comic no. {i}: {e}")
        break

# Convert list to DataFrame
df1 = pd.DataFrame(comics_data)

# Flattening columns/rows into strings - some columns return some problems due to the dictionary structure 
for col in df1.columns:
    if df1[col].apply(lambda x: isinstance(x, (dict, list))).any():
        df1[col] = df1[col].apply(json.dumps)  


# Fill missing values
df1.fillna('', inplace=True)

# Save locally and to Postgres 
df1.to_csv('/Users/konradwronski/Downloads/Comics.csv', index=False)

engine = create_engine('postgresql+psycopg2://postgres:legiunia98@localhost/jet_usecase')
df1.to_sql('comics', engine, if_exists='replace', index=False, chunksize=1000)

logging.info(f"Fetched {len(df1)} comics successfully!")


