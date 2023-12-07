import requests
from datetime import datetime
import json
from pyhive import hive

# Function to execute queries
def execute_query(query):
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE {database_name}")
            cursor.execute(query)
            return cursor
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

# Define URLs for your API endpoints
transactions_url = 'http://127.0.0.1:5000/api/transactions'
customers_url = 'http://127.0.0.1:5000/api/customers'
external_data_url = 'http://127.0.0.1:5000/api/externalData'

# Function to retrieve data from URL and convert it to Python objects
def get_data_as_object(url):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to retrieve data from {url}")
        return None

# Retrieve data from URLs
transactions_data = get_data_as_object(transactions_url)
customers_data = get_data_as_object(customers_url)
extern_data = get_data_as_object(external_data_url)

## date_time

for transaction in transactions_data:
    # Parse the date_time string into a datetime object
    date_time_obj = datetime.strptime(transaction["date_time"], "%Y-%m-%dT%H:%M:%S")
    
    # Assign date and time
    transaction["date"] = date_time_obj.date().strftime("%Y-%m-%d")
    transaction["time"] = date_time_obj.time().strftime("%H:%M:%S")
    
    # Extract year, month, and day
    transaction["year"] = date_time_obj.year
    transaction["month"] = date_time_obj.month
    transaction["day"] = date_time_obj.day


## 2-Customers_data

for customer in customers_data:
    customer["account_history"] = ", ".join(customer["account_history"])
    customer["avg_transaction_value"] = customer["behavioral_patterns"]["avg_transaction_value"]
    del customer["behavioral_patterns"]
    customer.update(customer["demographics"])
    del customer["demographics"]

print(json.dumps(customers_data, indent=2))


## 3-External_data

# Accessing the 'blacklist_info' key and joining the list elements into a string
extern_data['blacklist_info'] = ', '.join(extern_data['blacklist_info'])
# Example: Convert dictionaries to a list of dictionaries
external_data = []
for customer_id, credit_score in extern_data['credit_scores'].items():
    customer_info = {
        'customer_id': customer_id,
        'credit_score': credit_score,
        'fraud_report_count': extern_data['fraud_reports'].get(customer_id, 0)
        # Add more columns as needed
    }
    external_data.append(customer_info)

# III-Chargement

from pyhive import hive

# Establish a connection to Hive
conn = hive.Connection(host='localhost', port=10000)

# Create a database
database_name = 'financial_data'
with conn.cursor() as cursor:
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    cursor.execute(f"USE {database_name}")

# Define the table schemas
transaction_table_schema = """
CREATE TABLE IF NOT EXISTS transactions (
    amount FLOAT,
    currency STRING,
    amountUSD FLOAT,
    customer_id STRING,
    transaction_date STRING,
    time STRING,
    location STRING,
    merchant_details STRING,
    transaction_id STRING,
    transaction_type STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS ORC
"""

customer_table_schema = """
CREATE TABLE IF NOT EXISTS customers (
    account_history STRING,
    customer_id STRING,
    avg_transaction_value FLOAT,
    age INT,
    location STRING
)
STORED AS ORC
"""

external_table_schema = """
CREATE TABLE IF NOT EXISTS external_data (
    customer_id STRING,
    credit_score INT,
    fraud_report_count INT
)
STORED AS ORC
"""

blacklist_info_schema = """
CREATE TABLE IF NOT EXISTS blacklist_info (
    blacklist STRING
)
STORED AS ORC
"""

# Execute the table creation queries
with conn.cursor() as cursor:
    cursor.execute(transaction_table_schema)
    cursor.execute(customer_table_schema)
    cursor.execute(external_table_schema)
    cursor.execute(blacklist_info_schema)


## 2-Insertion data into tables
conn = hive.Connection(host='localhost', port=10000)
database_name = 'financial_data'

# Insert values into blacklist_info table
blacklist_info = extern_data['blacklist_info'].split(', ')
for merchant in blacklist_info:
    check_query = f"SELECT * FROM blacklist_info WHERE blacklist = '{merchant}'"
    existing_entry_cursor = execute_query(check_query)
    if existing_entry_cursor:
        existing_entry = existing_entry_cursor.fetchone()
        if not existing_entry:
            insert_query = f"INSERT INTO blacklist_info VALUES ('{merchant}')"
            execute_query(insert_query)
    else:
        print("Query execution failed.")

# Insert values into external_data table
for data in external_data:
    insert_query = f"INSERT INTO external_data VALUES ('{data['customer_id']}', {data['credit_score']}, {data['fraud_report_count']})"
    execute_query(insert_query)

# Insert values into customers table
for data in customers_data:
    insert_query = f"INSERT INTO customers VALUES ('{data['account_history']}', '{data['customer_id']}', {data['avg_transaction_value']}, {data['age']}, '{data['location']}')"
    execute_query(insert_query)

# Insert values into transactions table
for data in transactions_data:
    insert_query = f"""
        INSERT INTO transactions PARTITION (year={data['year']}, month={data['month']}, day={data['day']})
        VALUES ({data['amount']}, '{data['currency']}', '{data['amountUSD']}', '{data['customer_id']}', '{data['date']}', '{data['time']}', '{data['location']}', '{data['merchant_details']}', '{data['transaction_id']}', '{data['transaction_type']}')
    """
    execute_query(insert_query)
