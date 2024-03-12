import sqlite3
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'


def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    count = 0
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        if count <= 25:
            col = row.find_all('td')
            if len(col) != 0:
                if col[0].find('a') is not None and '—' not in col[2]:
                    data_dict = {
                        "Country": col[0].a.contents[0],
                        "GDP_USD_millions": col[2].contents[0]
                    }
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df, df1], ignore_index=True)
                    count + 1
        else:
            break
    return df


def tranfrom(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(',')))for x in GDP_list]
    GDP_LIST = [np.round(x/1000, 2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})

    return df

def load_data_tofile(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    return query_statement,sql_connection

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# Extract data
extracted_data = extract(url, table_attribs)
print("Extracted Data:")
print(extracted_data)

# Transform data
transformed_data = tranfrom(extracted_data)
print("\nTransformed Data:")
print(transformed_data)

# Load data to file
load_data_tofile(transformed_data, csv_path)
print("\nData loaded to file:", csv_path)

# Connect to SQLite database
sql_connection = sqlite3.connect(db_name)

# Load data to database
load_to_db(transformed_data, sql_connection, table_name)
print("\nData loaded to database:", table_name)

# Run a query
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

print('Process Complete.')
# Close SQL connection
sql_connection.close()