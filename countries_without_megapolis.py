import pandas as pd
import sqlite3
import logging
from sqlite3 import Error

# Set up logging
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

# URL of the CSV file
url = "https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/download/?format=csv&timezone=Europe/Berlin&lang=en&use_labels_for_header=true&csv_separator=%3B"

# Function to fetch the dataset from the URL
def fetch_data(url):
    try:
        data = pd.read_csv(url, sep=';')
        logging.info('Data fetched successfully')
        return data
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)

# Function to create a connection to the SQLite database
def create_connection():
    conn = None;
    try:
        # Create a connection to a SQLite database in memory
        conn = sqlite3.connect(':memory:')
        logging.info('Database connection created successfully')
    except Error as e:
        logging.error("Exception occurred", exc_info=True)

    return conn

# Function to create a table in the SQLite database
def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        # Here we'd enter the SQL commands to create a table
        c.execute(create_table_sql)
        logging.info('Table created successfully')
    except Error as e:
        logging.error("Exception occurred", exc_info=True)

# Function to insert data into the SQLite database
def insert_data(conn, data):
    try:
        # if cities table already exists, we replace it with the new version
        data.to_sql('cities', conn, if_exists='replace', index = False)
        logging.info('Data inserted successfully')
    except Error as e:
        logging.error("Exception occurred", exc_info=True)

# Function to query the SQLite database to find countries that don't host a megapolis
def query_data(conn):
    try:
        df = pd.read_sql_query('SELECT DISTINCT "Country Code", "Country name EN" FROM cities WHERE population <= 10000000 ORDER BY "Country name EN"', conn)
        logging.info('Data queried successfully')
        return df
    except Error as e:
        logging.error("Exception occurred", exc_info=True)

# Function to save the result in a TSV file
def save_data(df):
    try:
        df.to_csv('countries_without_megapolis.tsv', sep='\t', index=False)
        logging.info('Data saved successfully')
    except Error as e:
        logging.error("Exception occurred", exc_info=True)

# Main function to fetch the data, store it in the database, query the database, and save the result
def main():
    data = fetch_data(url)
    conn = create_connection()
    # Only create table if connection to db is established
    if conn is not None:
        create_table(conn, '''CREATE TABLE IF NOT EXISTS cities (
                                            name text,
                                            "Country code" text
                                            "Country name EN" text,
                                            Population integer
                                        ); ''')
        insert_data(conn, data)
        df = query_data(conn)
        save_data(df)
        
        # Commit changes and close the connection
        conn.commit()
        conn.close()
    else:
        logging.error("Error! cannot create the database connection.")

# Calling main function
if __name__ == "__main__":
    main()
