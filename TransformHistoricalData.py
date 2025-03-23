import pandas as pd
import random
import logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)

# Create a connection to your Postgres database
engine = create_engine('postgresql+psycopg2://postgres:legiunia98@localhost/jet_usecase')

# Read historical data from the staging table "comics"
df = pd.read_sql("SELECT * FROM comics", engine)
logging.info(f"Loaded {len(df)} rows from the 'comics' table.")


# Create the dimension DataFrame
dim_df = pd.DataFrame({
    'comic_id': df['num'],
    'title': df['title'],
    'publication_month': df['month'].astype(int),  
    'publication_year': df['year'].astype(int),     
    'transcript': df['transcript'],
    'img_url': df['img'],
    'alt_text': df['alt']
})

logging.info("Prepared dimension DataFrame for 'dim_comic'.")

def calculate_views():
    return int(random.random() * 10000)

def calculate_cost(title):
    return len(title) * 5

def calculate_reviews():
    return round(random.uniform(1.0, 10.0), 1)

# Create the fact DataFrame
fact_df = pd.DataFrame({
    'comic_id': df['num'],
    'views': df['title'].apply(lambda x: calculate_views()),
    'cost': df['title'].apply(lambda x: calculate_cost(x)),
    'customer_reviews': df['title'].apply(lambda x: calculate_reviews())
})

logging.info("Prepared fact DataFrame for 'fact_comic_performance'.")

# Populate the dimension table 
dim_df.to_sql('dim_comic', engine, if_exists='replace', index=False, chunksize=1000)
logging.info("'dim_comic' table populated successfully.")

# Populate the fact table
fact_df.to_sql('fact_comic_performance', engine, if_exists='replace', index=False, chunksize=1000)
logging.info("'fact_comic_performance' table populated successfully.")
