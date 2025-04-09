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
    'Content-Type': 'application/json'  # Ensure this header is set
}

headers_giper = {
    'Authorization': KeyGiper,
    'Accept': 'application/json',
    'Content-Type': 'application/json'  # Ensure this header is set
}

headers_kitchen = {
    'Authorization': KeyKitchen,
    'Accept': 'application/json',
    'Content-Type': 'application/json'  # Ensure this header is set
}

headers_smart = {
    'Authorization': KeySmart,
    'Accept': 'application/json',
    'Content-Type': 'application/json'  # Ensure this header is set

}

#^ Initialize variables
# Automatically get yesterday's date
yesterday_start = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')  # Start of yesterday
yesterday_end = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d 23:59:59')    # End of yesterday
page = 1

def get_report(url, headers, begin, end,page, project_name):
    all_data = []
    while True:
        # Define the request body
        request_body = {
            "period": {
                "begin": begin,  # Replace with the actual start date
                "end": end  # Replace with the actual end date
            },
            "orderBy": {
                "field": "ordersSumRub",  # Replace with the desired sorting field
                "mode": "desc"  # Replace with 'asc' for ascending or 'desc' for descending
            },
            "page": page  # Replace with the desired page number
        }

        # Convert the request body to JSON
        json_data = json.dumps(request_body)

        # Send the POST request
        response = requests.post(url, headers=headers, data=json_data)
        
        # Check the response status and content
        if response.status_code == 200:
            data = response.json()
            if data['data']['cards'] is None: # Stop if no more data is returned
                # Stop if no more data is returned or isNextPage is false
                break
            all_data.extend(data['data']['cards'])   # Add the data to the list
            print(f"Page {page} retrieved successfully for {project_name}")
            page += 1  # Move to the next page
            time.sleep(20)
        else:
            print(f"Request failed with status code {response.status_code}.")
            print("Response text:", response.text)
            break
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
    
    # Fetch product statistics for each project
    projects = {
        'WB-GutenTech': headers_guten,
        'WB-ГиперМаркет': headers_giper,
        'WB-KitchenAid': headers_kitchen,
        'WB-Smart-Market': headers_smart
    }
    
    # Define the API endpoint
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"

    data_guten = get_report(url, headers_guten, yesterday_start, yesterday_end, page, 'WB-GutenTech' )
    data_giper = get_report(url, headers_giper, yesterday_start, yesterday_end, page, 'WB-ГиперМаркет')
    data_kitchen = get_report(url, headers_kitchen, yesterday_start, yesterday_end, page, 'WB-KitchenAid')
    data_smart = get_report(url, headers_smart, yesterday_start, yesterday_end, page, 'WB-Smart-Market')

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
    columns_to_keep = ['nmID', 'vendorCode', 'brandName', 'objectID', 'objectName', 'begin', 'openCardCount', 'addToCartCount', 'ordersCount', 'ordersSumRub', 'buyoutsCount', 'buyoutsSumRub', 'cancelCount', 'cancelSumRub', 'stocksMp', 'stocksWb', 'Project', 'Marketplace']
    # Ensure filtered_df is a copy of the slice, not a view
    filtered_df = combined_df[columns_to_keep].copy()
    filtered_df['begin'] = pd.to_datetime(filtered_df['begin'])
    
    # Define connection parameters
    client = get_client(
        host='rc1a-j5ou9lq30ldal602.mdb.yandexcloud.net',  # Your Yandex Cloud ClickHouse host
        port=8443,                                          # Yandex Cloud uses port 8443 for HTTPS
        username='user1',                           # Your ClickHouse username
        password= password,                           # Your ClickHouse password
        database='user1',                            # Your database name
        secure=True,                                        # Use HTTPS
        verify=False                                        # Disable SSL certificate verification 
        # Define the data to insert
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
    # Debugging: Check the structure of the data
    insert_into_clickhouse(client, table_name, data, columns)
    print(filtered_df.head())

if __name__ == "__main__":
    main()