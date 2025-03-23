from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import logging
import pandas as pd
import random
from sqlalchemy import create_engine

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'xkcd_elt_pipeline_full',
    default_args=default_args,
    schedule_interval='0 0 * * 1,3,5',
    catchup=False,
)

def extract_data(**kwargs):
    '''Extracting data from API and chekcing the latest comic. Pushin data to XCom'''
    logging.info("Starting extraction...")
    # Check latest comic id
    engine = create_engine('postgresql+psycopg2://postgres:legiunia98@localhost/jet_usecase')
    try:
        query = "SELECT MAX(num) FROM comics;"
        result = pd.read_sql(query, engine)
        latest_comic_id = result.iloc[0, 0] if not result.empty and result.iloc[0, 0] is not None else 0
    except Exception as e:
        logging.error("Error fetching latest comic id from DB: %s", e)
        latest_comic_id = 0

    start_comic = latest_comic_id + 1 if latest_comic_id > 0 else 1
    logging.info("Latest comic in DB: %s. Starting extraction from comic %s.", latest_comic_id, start_comic)

    extracted_comics = []
    i = start_comic
    while True:
        url = f"https://xkcd.com/{i}/info.0.json"
        try:
            response = requests.get(url, timeout=100)
        except Exception as e:
            logging.error("Exception fetching comic %s: %s", i, e)
            break

        if not response.ok:
            logging.info("No comic found at %s, stopping extraction.", i)
            break

        comic = response.json()
        extracted_comics.append(comic)
        logging.info("Extracted comic %s", i)

        # Skipping comic #404 - no such comic.
        if i == 403:
            i += 2
        else:
            i += 1

    if extracted_comics:
        kwargs['ti'].xcom_push(key='extracted_comics', value=extracted_comics)
        logging.info("Pushed %s new comics to XCom.", len(extracted_comics))
    else:
        logging.info("No new comics extracted.")

def load_data(**kwargs):
    '''Loading data to Postgres main table'''
    # Retrieve from XCom.
    extracted_comics = kwargs['ti'].xcom_pull(key='extracted_comics', task_ids='extract_task')
    if not extracted_comics:
        logging.info("No comics to load.")
        return

    df_new = pd.DataFrame(extracted_comics)
    # Flattening - some columns reate problems for pandas to process due to their format
    for col in df_new.columns:
        if df_new[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df_new[col] = df_new[col].apply(lambda x: json.dumps(x))
    
    engine = create_engine('postgresql+psycopg2://postgres:legiunia98@localhost/jet_usecase')
    try:
        df_new.to_sql('comics', engine, if_exists='append', index=False, chunksize=1000)
        logging.info("Loaded %s new comics into table 'comics'.", len(df_new))
    except Exception as e:
        logging.error("Error during load: %s", e)

def transform_data(**kwargs):
    '''Transforming data and splitting it into 2 destination tables'''
    logging.info("Starting transformation...")
    engine = create_engine('postgresql+psycopg2://postgres:legiunia98@localhost/jet_usecase')
    try:
        df = pd.read_sql("SELECT * FROM comics", engine)
    except Exception as e:
        logging.error("Error reading staging table: %s", e)
        return
    logging.info(f"Loaded {len(df)} rows from 'comics'")

    dim_df = pd.DataFrame({
        'comic_id': df['num'],
        'title': df['title'],
        'publication_month': df['month'].astype(int),
        'publication_year': df['year'].astype(int),
        'transcript': df['transcript'],
        'img_url': df['img'],
        'alt_text': df['alt']
    })

    def calculate_views():
        '''Views per comic - random number between 0 and 1 multiplied by 10000'''
        return int(random.random() * 10000)

    def calculate_cost(title):
        '''Cost for comic length of the title * 5'''
        return len(title) * 5

    def calculate_reviews():
        '''Number of reviews'''
        return round(random.uniform(1.0, 10.0), 1)

    fact_df = pd.DataFrame({
        'comic_id': df['num'],
        'views': df['title'].apply(lambda x: calculate_views()),
        'cost': df['title'].apply(lambda x: calculate_cost(x)),
        'customer_reviews': df['title'].apply(lambda x: calculate_reviews())
    })

    try:
        # Write the dim and fact DataFrames to target destination
        dim_df.to_sql('dim_comic', engine, if_exists='replace', index=False, chunksize=1000)
        logging.info("'dim_comic' table populated successfully.")
        fact_df.to_sql('fact_comic_performance', engine, if_exists='replace', index=False, chunksize=1000)
        logging.info("'fact_comic_performance' table populated successfully.")
    except Exception as e:
        logging.error("Error during transformation load: %s", e)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

# Set task dependencies: Extraction -> Load -> Transformation
extract_task >> load_task >> transform_task
