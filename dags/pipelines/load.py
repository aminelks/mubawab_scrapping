from sqlalchemy import create_engine
import pandas as pd
from airflow.decorators import task
import logging

@task
def load_data(df):

    if df is None or df.empty:
        logging.warning("No data to load: DataFrame is empty or None.")
        return
    
    logging.info("Loading data ...")
    try:
        engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
        engine.connect()

        num_rows_inserted=df.to_sql(name='estate_data', con=engine, if_exists='append', index=False)
        logging.info(f"{num_rows_inserted} rows inserted successfully")

    except Exception as e:
        logging.info("Data load error: " + str(e))


