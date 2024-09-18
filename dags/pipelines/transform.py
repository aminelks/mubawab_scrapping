import re
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.decorators import task
import logging


def extract_rooms(text):
    match = re.search(r'(\d+)\s*chambre', text)
    return int(match.group(1)) if match else pd.NA

def extract_area(text):
    match = re.search(r'(\d+)\s*m²', text)
    return int(match.group(1)) if match else pd.NA


def parse_publication_date(text):
    today = datetime.now()
    if 'aujourdhui' in text:
        return today
    elif 'jour' in text:
        match = re.search(r'(\d+)\s*jour', text)
        if match:
            days = int(match.group(1))
            return today - timedelta(days=days)
    elif 'semaine' in text:
        match = re.search(r'(\d+)\s*semaine', text)
        if match:
            weeks = int(match.group(1))
            return today - timedelta(weeks=weeks)
    elif 'mois' in text:
        match = re.search(r'(\d+)\s*mois', text)
        if match:
            months = int(match.group(1))
            return today - relativedelta(months=months)


@task
def transform_data(data):
    logging.info("Transforming data ...")
    try:
        df=pd.DataFrame(data,columns=['Id','Price','Title','Location','Area','Description','Publication date'])
        
        df=df[df['Price']!='Prix à consulter']
        if df.empty:
            logging.warning("No valid data to transform: all prices are 'Prix à consulter'.")
            return None
        
        df['Price']=df['Price'].str.replace('\xa0', '').str.replace(' DH', '').str.replace(' EUR', '0').astype(int)

        df['Location'] = df['Location'].apply(lambda x: ' '.join(x.split()))
        df[['Neighborhood','City']]=df['Location'].str.split('à ', expand=True)
        df.City=df.City.fillna(df.Neighborhood)

        df.Area=df.Area.fillna(df.Description)

        df['Rooms']=df.Area.apply(extract_rooms)
        df['Area']=df.Area.apply(extract_area)

        df['Date'] = df['Publication date'].apply(parse_publication_date)

        df=df[['Id','Title','City','Neighborhood','Rooms','Area','Price','Description','Date']]

        logging.info("Data successfully transformed")
        return df

    except Exception as e:
        logging.info("Data transform error: " + str(e))


    




