import requests
import json
import pandas as pd
from clickhouse_connect import get_client
from datetime import date, timedelta
from dotenv import load_dotenv
import os
import time

load_dotenv()

# Retrieve API keys from environment variables
KeyGuten = os.getenv('KeyGuten')
KeyGiper = os.getenv('KeyGiper')
KeyKitchen = os.getenv('KeyKitchen')
KeySmart = os.getenv('KeySmart')
password = os.getenv('ClickHouse')

# Define headers for each project
headers_guten = {
    'Authorization': KeyGuten,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

headers_giper = {
    'Authorization': KeyGiper,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

headers_kitchen = {
    'Authorization': KeyKitchen,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

headers_smart = {
    'Authorization': KeySmart,
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

# Initialize variables
yesterday_start = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
yesterday_end = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d 23:59:59')

def get_report(url, headers, begin, end, page, project_name):
    all_data = []
    max_retries = 3
    retry_delay = 30  # seconds
    request_timeout = 60  # seconds
    
    while True:
        for attempt in range(max_retries):
            try:
                # Define the request body with all required parameters
                request_body = {
                    "period": {
                        "begin": begin,
                        "end": end
                    },
                    "orderBy": {
                        "field": "ordersSumRub",
                        "mode": "desc"
                    },
                    "page": page,
                    "timezone": "Europe/Moscow",
                    "brandNames": [],
                    "objectIDs": [],
                    "nmIDs": []
                }

                json_data = json.dumps(request_body, ensure_ascii=False)
                
                # Send the POST request with timeout
                response = requests.post(
                    url, 
                    headers=headers, 
                    data=json_data, 
                    timeout=request_timeout
                )
                
                # Handle rate limiting (429 status code)
                if response.status_code == 429:
                    wait_time = int(response.headers.get('Retry-After', retry_delay))
                    print(f"Rate limit exceeded for {project_name}. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                
                # Check for successful response
                if response.status_code != 200:
                    print(f"Request failed with status code {response.status_code} for {project_name}, page {page}")
                    print("Response text:", response.text)
                    if attempt == max_retries - 1:
                        return all_data
                    time.sleep(retry_delay)
                    continue
                
                data = response.json()
                
                # Validate response structure
                if not data.get('data') or not isinstance(data['data'].get('cards'), list):
                    print(f"Invalid data structure received for {project_name}, page {page}")
                    return all_data
                
                cards = data['data']['cards']
                
                # Check if we have any data
                if not cards:
                    print(f"No more data available for {project_name}")
                    return all_data
                
                all_data.extend(cards)
                print(f"Page {page} retrieved successfully for {project_name} (got {len(cards)} items)")
                
                # Check if there are more pages (using isNextPage flag)
                if not data['data'].get('isNextPage', False):
                    print(f"Reached last page for {project_name}")
                    return all_data
                
                # Prepare for next page
                page += 1
                time.sleep(5)  # Reduced delay between pages to 5 seconds
                break  # Success, break out of retry loop
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed for {project_name}, page {page}: {str(e)}")
                if attempt == max_retries - 1:
                    return all_data
                time.sleep(retry_delay)
                continue
            except Exception as e:
                print(f"Unexpected error for {project_name}, page {page}: {str(e)}")
                return all_data

# Function to flatten the JSON data for the current period
def flatten_json_current_period(cards):
    flattened_data = []
    for card in cards:
        nmID = card["nmID"]
        vendorCode = card["vendorCode"]
        brandName = card["brandName"]
        objectID = card["object"]["id"]
        objectName = card["object"]["name"]
        
        # Extract statistics for the selected period
        selected_period = card["statistics"]["selectedPeriod"]

        flattened_data.append({
            "nmID": nmID,
            "vendorCode": vendorCode,
            "brandName": brandName,
            "objectID": objectID,
            "objectName": objectName,
            "begin": selected_period["begin"],
            "end": selected_period["end"],
            "openCardCount": selected_period["openCardCount"],
            "addToCartCount": selected_period["addToCartCount"],
            "ordersCount": selected_period["ordersCount"],
            "ordersSumRub": selected_period["ordersSumRub"],
            "buyoutsCount": selected_period["buyoutsCount"],
            "buyoutsSumRub": selected_period["buyoutsSumRub"],
            "cancelCount": selected_period["cancelCount"],
            "cancelSumRub": selected_period["cancelSumRub"],
            "avgOrdersCountPerDay": selected_period["avgOrdersCountPerDay"],
            "avgPriceRub": selected_period["avgPriceRub"],
            "addToCartPercent": selected_period["conversions"]["addToCartPercent"],
            "cartToOrderPercent": selected_period["conversions"]["cartToOrderPercent"],
            "buyoutsPercent": selected_period["conversions"]["buyoutsPercent"],
            "stocksMp": card["stocks"]["stocksMp"],
            "stocksWb": card["stocks"]["stocksWb"]
        })

    return flattened_data

# Function to insert data into ClickHouse
def insert_into_clickhouse(client, table_name, data, columns):
    client.insert(table_name, data, column_names=columns)
    print("Data inserted successfully!")

def main():
    # Define the API endpoint
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"

    # Get data for each project starting from page 1
    data_guten = get_report(url, headers_guten, yesterday_start, yesterday_end, 1, 'WB-GutenTech')
    data_giper = get_report(url, headers_giper, yesterday_start, yesterday_end, 1, 'WB-ГиперМаркет')
    data_kitchen = get_report(url, headers_kitchen, yesterday_start, yesterday_end, 1, 'WB-KitchenAid')
    data_smart = get_report(url, headers_smart, yesterday_start, yesterday_end, 1, 'WB-Smart-Market')

    # Convert the flattened data to a DataFrame
    flattened_data_guten = flatten_json_current_period(data_guten)
    flattened_data_giper = flatten_json_current_period(data_giper)
    flattened_data_kitchen = flatten_json_current_period(data_kitchen)
    flattened_data_smart = flatten_json_current_period(data_smart)

    df_guten = pd.DataFrame(flattened_data_guten)
    df_giper = pd.DataFrame(flattened_data_giper)
    df_kitchen = pd.DataFrame(flattened_data_kitchen)
    df_smart = pd.DataFrame(flattened_data_smart)

    # Add the 'Project' column to each DataFrame before concatenation
    df_guten['Project'] = 'WB-GutenTech'
    df_giper['Project'] = 'WB-ГиперМаркет'
    df_kitchen['Project'] = 'WB-KitchenAid'
    df_smart['Project'] = 'WB-Smart-Market'

    # Combine all campaign data
    combined_df = pd.concat([df_guten, df_giper, df_kitchen, df_smart], ignore_index=True)
    combined_df['brandName'] = combined_df['brandName'].str.upper()
    combined_df['Marketplace'] = 'Wildberries'
    print("Columns in combined_campaigns:", combined_df.columns.tolist())
    
    # Keep only the desired columns
    columns_to_keep = ['nmID', 'vendorCode', 'brandName', 'objectID', 'objectName', 'begin', 'openCardCount', 
                     'addToCartCount', 'ordersCount', 'ordersSumRub', 'buyoutsCount', 'buyoutsSumRub', 
                     'cancelCount', 'cancelSumRub', 'stocksMp', 'stocksWb', 'Project', 'Marketplace']
    filtered_df = combined_df[columns_to_keep].copy()
    filtered_df['begin'] = pd.to_datetime(filtered_df['begin'])
    
    # Define connection parameters
    client = get_client(
        host='rc1a-j5ou9lq30ldal602.mdb.yandexcloud.net',
        port=8443,
        username='user1',
        password=password,
        database='user1',
        secure=True,
        verify=False
    )
    
    # Ensure the DataFrame has the correct columns
    columns = ['nmID', 'vendorCode', 'brandName', 'objectID', 'objectName', 'begin', 'openCardCount', 
              'addToCartCount', 'ordersCount', 'ordersSumRub', 'buyoutsCount', 'buyoutsSumRub', 'cancelCount', 
              'cancelSumRub', 'stocksMp', 'stocksWb', 'Project', 'Marketplace']

    # Reorder columns to match the expected order
    data_organized = filtered_df[columns]

    # Convert DataFrame to a list of tuples for bulk insertion
    data = [tuple(row) for row in data_organized.to_numpy()]
    
    # Define the table name
    table_name = 'order_history_wb'
    
    # Insert data into ClickHouse
    insert_into_clickhouse(client, table_name, data, columns)
    print(filtered_df.head())

if __name__ == "__main__":
    main()