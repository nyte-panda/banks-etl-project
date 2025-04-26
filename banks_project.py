# Code for ETL operations on Largest Banks data

# Importing the required libraries
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import requests
from datetime import datetime
import sqlite3



def log_progress(message):
    '''This function logs the mentioned message at a given stage of the 
    code execution to a log file.'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # Get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    '''This function extracts the required information from the website
    and saves it to a DataFrame.'''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')  

    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict = {"Name" : bank_name, "MC_USD_Billion": float(market_cap)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df, exchange_rate_path):
    '''This function could transform the data if needed.
    Currently, it returns the DataFrame as is.'''
    
    #Read the exchange rate CSV
    exchange_df = pd.read_csv(exchange_rate_path)

    #Convert to dictionary
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    #Create new columns
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    '''This function saves the final DataFrame as a CSV file in the provided path.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    '''This function saves the DataFrame to a database table.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    '''This function runs the stated query on the database and prints the result.'''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
exchange_rate_path = './exchange_rate.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, exchange_rate_path)

log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * from {table_name} WHERE MC_USD_Billion >= 10"
run_query(query_statement, sql_connection)

# 1. Print the entire table
query1 = "SELECT * FROM Largest_banks"
run_query(query1, sql_connection)
log_progress('Query 1 executed: SELECT * FROM Largest_banks')

# 2. Print the average MC_GBP_Billion
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query2, sql_connection)
log_progress('Query 2 executed: SELECT AVG(MC_GBP_Billion) FROM Largest_banks')

# 3. Print the names of top 5 banks
query3 = "SELECT Name FROM Largest_banks LIMIT 5"
run_query(query3, sql_connection)
log_progress('Query 3 executed: SELECT Name FROM Largest_banks LIMIT 5')

log_progress('Process Complete.')
sql_connection.close()
