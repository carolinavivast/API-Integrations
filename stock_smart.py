import os
import requests
import pandas as pd
import json
from clickhouse_connect import get_client
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Fetch data from the API
def fetch_data():
    url = "https://api.i-colors.ru/ms/stocks/smart/now"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")

# Transform the data into a DataFrame
def transform_data(data):
    stocks = data.get('stocks', [])

    # Normalize the nested JSON structure
    df = pd.json_normalize(stocks)

    # Convert the entire DataFrame to strings to avoid JSON serialization issues
    df = df.map(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x)

    # Serialize the DataFrame to JSON
    df['metadata_json'] = df.apply(lambda x: json.dumps(x.to_dict()), axis=1)

    return df

# Main execution
if __name__ == "__main__":
    try:
        # Fetch data
        data = fetch_data()
        df = transform_data(data)

        # Fetch ClickHouse password from environment variables
        password = os.getenv('ClickHouse')
        print("Password:", password)  # Debugging: Check if the password is correctly fetched

        # Define connection parameters
        client = get_client(
            host='rc1a-j5ou9lq30ldal602.mdb.yandexcloud.net',  # Your Yandex Cloud ClickHouse host
            port=8443,                                          # Yandex Cloud uses port 8443 for HTTPS
            username='user1',                                   # Your ClickHouse username
            password=password,                                  # Your ClickHouse password
            database='user1',                                   # Your database name
            secure=True,                                        # Use HTTPS
            verify=False                                        # Disable SSL certificate verification
        )

        # Ensure date columns are in the correct format for ClickHouse
        df['moment'] = pd.to_datetime(df['moment'])  # Convert to datetime

        # Debugging: Check the data types of the DataFrame
        print("Data types of df:")
        print(df.dtypes)

        # Rename columns to match ClickHouse table
        df = df.rename(columns={
            'assortment.type': 'assortment_type',
            'assortment.code': 'assortment_code',
            'assortment.name': 'assortment_name',
            'assortment.gtin': 'assortment_gtin',
            'assortment.brand': 'assortment_brand',
            'assortment.state': 'assortment_state',
            'assortment.folder': 'assortment_folder',
            'assortment.volume': 'assortment_volume',
            'stock.stock': 'stock_stock',
            'stock.transit': 'stock_transit',
            'stock.reserve': 'stock_reserve',
            'stock.quantity': 'stock_quantity',
            'stock.days': 'stock_days',
            'stock.cost': 'stock_cost'
        })

        # Ensure the DataFrame has the correct columns
        columns = [
            'moment', 'assortment_type', 'assortment_code', 'assortment_name',
            'assortment_gtin', 'assortment_brand', 'assortment_state',
            'assortment_folder', 'assortment_volume', 'stock_stock',
            'stock_transit', 'stock_reserve', 'stock_quantity',
            'stock_days', 'stock_cost', 'metadata_json'
        ]

        # Reorder columns to match the expected order
        df_copy = df[columns]

        # Convert DataFrame to a list of tuples for bulk insertion
        data = [tuple(row) for row in df_copy.to_numpy()]

        # Debugging: Check the structure of the data
        print("Sample data to insert:", data[:5])  # Print the first 5 rows to check the structure

        # Define the table name
        table_name = 'stocks'

        # Use the insert method for bulk insertion
        client.insert(table_name, data, column_names=columns)
        print("Data inserted successfully!")

    except Exception as e:
        print(e)
