This project extracts real estate data from Mubawab using web scraping, processes it with Python, and loads it into a PostgreSQL database. The ETL pipeline is orchestrated with Apache Airflow and runs inside Docker containers for consistency and scalability.

Overview
This ETL pipeline retrieves data from the Mubawab website and performs the following steps:

Data Extraction: Using BeautifulSoup, the real estate listings are scraped from the Mubawab website.
Data Transformation: The extracted data is cleaned and structured in Python.
Data Loading: The cleaned data is inserted into a PostgreSQL database.
Scheduling & Orchestration: The entire ETL process is scheduled and monitored with Apache Airflow, running inside Docker containers for easy deployment.
