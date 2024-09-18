from math import ceil
import re
from bs4 import BeautifulSoup
import requests
import time
from airflow.decorators import task
import logging
import os

file_path = os.path.join(os.path.dirname(__file__), 'count.txt')

def load_results_count():
    try:
        with open(file_path, 'r') as file:
            count = int(file.read().strip())
            return count
    except FileNotFoundError:
        return 0

def save_results_count(count):
    with open(file_path, 'w') as file:
        file.write(f"{count}\n")


base_url='https://www.mubawab.ma/fr/sc/appartements-a-vendre:p:'

def get_information():
    for attempt in range(1,4):
        try:
            logging.info(f"Attempt {attempt} for getting informations")
            url=base_url+'1'
            html = requests.get(url)
            html.raise_for_status()
            logging.info("Success")
            soup = BeautifulSoup(html.text, "html.parser")
            texte=soup.find('p',class_='fSize11 centered').text
            results = int(re.search(r'de (\d+)', texte).group(1))
            return results
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error on attempt {attempt}")
            delay=120*attempt
            if attempt < 2:
                logging.info(f"Waiting {delay} minutes before retrying...")
                time.sleep(delay)
            else:
                raise Exception(f"Failed after 3 attempts...")


@task
def extract_data():
    data=[] 
    try:
        results= get_information()
        logging.info(f'{results} result')
        old_count = load_results_count()
        logging.info(f'{old_count} result already processed')
        new_results = results - old_count
        new_pages = ceil(new_results / 33)
        logging.info(f"{new_pages} pages to extract ...")

        for i in range(1, new_pages + 1):
            url = base_url + f'{i}'
            for attempt in range(1,4):
                try:
                    logging.info(f"Attempt {attempt} to fetch page {i}/{new_pages}")
                    html = requests.get(url)
                    html.raise_for_status()
                    logging.info(f"Success for page {i}/{new_pages}")
                    
                    soup = BeautifulSoup(html.text, "html.parser")
                    html_data = soup.findAll('li', class_='listingBox w100')
                    
                    for rows in html_data:
                        id = rows.find('input')['value']
                        price = rows.find('span').text.strip()
                        title = rows.find('h2').text.strip()
                        location = rows.find('h3').text.strip()
                        area = rows.find('h4').text.strip() if rows.find('h4') else None
                        description = rows.find('p').text.strip()
                        publication_date = rows.find('span', class_="listingDetails iconPadR").text.strip()
                        data.append([id, price, title, location, area, description, publication_date])
                    
                    logging.info(f"Page {i}/{new_pages} extracted")
                    break
                
                except requests.exceptions.RequestException as e:
                    logging.error(f"Error on attempt {attempt} for page {i}/{new_pages}: {e}")
                    if attempt < 2:
                        delay=attempt*120
                        logging.info(f"Waiting for {delay} minutes before retrying...")
                        time.sleep(delay)
                    else:
                        raise Exception(f"Failed after 3 attempts for page {i}/{new_pages}...")
        
        save_results_count(results)
        return data[:new_results]
    
    except Exception as e:
        logging.error("Data extraction error: " + str(e))