# **Mubawab ETL Process**

This project extracts real estate data from Mubawab using web scraping, processes it with Python, and loads it into a PostgreSQL database. The ETL pipeline is orchestrated with Apache Airflow and runs inside Docker containers for consistency and scalability. The data is then visualized using Metabase.

## **Overview**

This ETL pipeline retrieves data from the Mubawab website and performs the following steps:

1. **Data Extraction**: Using BeautifulSoup, the real estate listings are scraped from the Mubawab website.
2. **Data Transformation**: The extracted data is cleaned and structured in Python.
3. **Data Loading**: The cleaned data is inserted into a PostgreSQL database.
4. **Scheduling & Orchestration**: The entire ETL process is scheduled and monitored with Apache Airflow, running inside Docker containers for easy deployment.
5. **Data Visualization**: Visualize and analyze the data using Metabase.
