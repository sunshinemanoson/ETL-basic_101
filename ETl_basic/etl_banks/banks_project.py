import requests
import numpy as np
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attributes = ["Bankname", "MC_USD_Billion",
                    "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
csv_path = 'exchange_rate.csv'
output_csv = './Largest_banks_data.csv'
db_name = 'Bank.db'
table_name = 'Largest_banks'
log_file = 'codeBank_log.txt'


def log_progress(message):
    timestamp_format = '%d-%h-%Y-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./etl_bank_project.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attributes):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, "html.parser")
    df = pd.DataFrame(columns=table_attributes)
    count = 0
    tables = data.find_all('tbody')
    rows = tables[1].find_all('tr')

    for row in rows:
        if count <= 100:
            col = row.find_all('td')
            if len(col) != 0:
                if col[1].find('a') is not None:
                        data_dict = {
                                    "Bankname": col[1].get_text().strip(),
                                    "MC_USD_Billion": col[2].contents[0][:-1].strip()
                                    }
                        df1 = pd.DataFrame([data_dict])
                        df = pd.concat([df, df1], ignore_index=True)
                        count += 1
                        
        else: 
            break
    return df

def transform(df, csv_path):
    exchange_rate = pd.read_csv('exchange_rate.csv')
    # print("Exchange rate DataFrame:")
    # print(exchange_rate)
    # print("Size:", exchange_rate.shape)
    # Check if the DataFrame is not empty
    if not exchange_rate.empty:
        usd_to_gbp = exchange_rate.loc[exchange_rate['Currency'] == 'GBP', 'Rate'].values[0] 
        usd_to_eur = exchange_rate.loc[exchange_rate['Currency'] == 'EUR', 'Rate'].values[0] 
        usd_to_inr = exchange_rate.loc[exchange_rate['Currency'] == 'INR', 'Rate'].values[0] 
        # print("s1", usd_to_gbp, "s2", usd_to_eur, "s3", usd_to_inr)

        # Remove commas from 'MC_USD_Billion' column and convert to numeric
        df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'].str.replace(',', ''), errors='coerce')

        # Check if conversion was successful
        if df['MC_USD_Billion'].notnull().all():
            df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * usd_to_eur, 2)
            df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * usd_to_gbp, 2)
            df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * usd_to_inr, 2)
        else:
            print("Conversion of 'MC_USD_Billion' to numeric failed. Transformation not performed.")
    else:
        print("Exchange rate DataFrame is empty. Transformation not performed.")
    
    return df


def load_data_tofile(df, output_csv):
    df.to_csv(output_csv)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    return query_statement,sql_connection    
    
log_progress("Preliminaries complete. Initiating ETL process")

extract_data = extract(url, table_attributes)
print("Extracted Data:")
print(extract_data)
log_progress("Data extraction complete. Initiating Transformation process")

transform_data = transform(extract_data, csv_path)
print("Data transformation complete. Initiating Loading process:")
print(transform_data)

print("\nData loaded to file:", output_csv)
log_progress("Data saved to CSV_Output file")


# Connect to SQLite database
sql_connection = sqlite3.connect(db_name)
log_progress("Connection initiated")

# #  Save transformed data to CSV file
load_data_tofile(transform_data, output_csv)
print("\nData loaded to file:", output_csv)
log_progress("Data saved to CSV_Output file")

# # Load data to database
load_to_db(transform_data, sql_connection, table_name)
print("\nData loaded to database:", table_name)
log_progress("Data loaded to Database as a table, Executing queries")

# Run a query
# query_statement = f"SELECT * FROM Largest_banks"
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
# query_statement = f"SELECT Bankname from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)
log_progress("Process Complete")


# Close SQL connection
sql_connection.close()
log_progress("Server Connection closed")
log_progress("Process Completed.")
print('Process Completed.')

average_gbp = (393.41 + 213.16   + 175.56 + 142.99 + 140.55  +  135.87 + 124.90  + 122.44  + 121.58 + 120.31 ) / 10
print(average_gbp)