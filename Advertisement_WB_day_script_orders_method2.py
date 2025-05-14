import requests
import pandas as pd
from clickhouse_connect import get_client
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()
KeyGuten = os.getenv('KeyGuten')
KeyGiper = os.getenv('KeyGiper')
KeyKitchen = os.getenv('KeyKitchen')
KeySmart = os.getenv("KeySmart")

# API endpoint
url_promotion_count = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'

# Headers including the API key for authentication
headers_guten = {
    'Authorization': KeyGuten,
    'Accept': 'application/json'
}

headers_giper = {
    'Authorization': KeyGiper,
    'Accept': 'application/json'
}

headers_kitchen = {
    'Authorization': KeyKitchen,
    'Accept': 'application/json'
}

headers_smart = {
    'Authorization': KeySmart,
    'Accept': 'application/json'
}

# Make the GET request
response_guten = requests.get(url_promotion_count, headers=headers_guten)
response_giper = requests.get(url_promotion_count, headers=headers_giper)
response_kitchen = requests.get(url_promotion_count, headers=headers_kitchen)
response_smart = requests.get(url_promotion_count, headers=headers_smart)

# Check if the request was successful
if response_guten.status_code == 200 and response_giper.status_code == 200 and response_kitchen.status_code == 200 and response_smart.status_code == 200:
    # Parse the JSON response
    data_guten = response_guten.json()
    data_giper = response_giper.json()
    data_kitchen = response_kitchen.json()
    data_smart = response_smart.json()
    print("Data retrieved successfully")
else:
    print(f"Failed to retrieve data. Status code: {response_guten.status_code, response_giper.status_code, response_giper.status_code, response_smart.status_code}")
    print(f"Response: {response_guten.text, response_giper.text, response_giper.text, response_smart.text}")

def process_advert_data(data):
    # Flatten the JSON data
    df = pd.json_normalize(
        data['adverts'],
        record_path='advert_list',
        meta=['type', 'status', 'count']
    )

    # Convert 'changeTime' to datetime format
    df['changeTime'] = pd.to_datetime(df['changeTime'])

    # Reset the index
    df = df.reset_index(drop=True)

    return df

# Process each dataset
df_guten = process_advert_data(data_guten)
df_giper = process_advert_data(data_giper)
df_kitchen = process_advert_data(data_kitchen)
df_smart = process_advert_data(data_smart)

# Define the API endpoint
url_promotion_adv = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"

# Define the query parameters
query_params = {
    "order": "create",
    "direction": "desc"
}

# Function to fetch campaign data
def fetch_campaign_data(df, headers, project_name, chunk_size=50):
    chunks = [df['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df), chunk_size)]
    all_campaign_data = []
    for idx, chunk in enumerate(chunks):
        response = requests.post(url_promotion_adv, params=query_params, json=chunk, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_campaign_data.extend(data)
            print(f"Data retrieved successfully for {project_name} chunk {idx + 1}")
        else:
            print(f"Error for {project_name} chunk {idx + 1}: {response.status_code}, {response.text}")
        time.sleep(1)  # Add a delay to avoid hitting API rate limits

    all_campaign_data = pd.DataFrame(all_campaign_data)
    all_campaign_data = all_campaign_data.sort_values(by='createTime', ascending=False)
    all_campaign_data['Project'] = project_name
    all_campaign_data['Marketplace'] = 'Wildberries'

    return all_campaign_data

# Fetch campaign data for each project
campaigns_guten = fetch_campaign_data(df_guten, headers_guten, 'WB-GutenTech')
campaigns_giper = fetch_campaign_data(df_giper, headers_giper, 'WB-ГиперМаркет')
campaigns_kitchen = fetch_campaign_data(df_kitchen, headers_kitchen, 'WB-KitchenAid')
campaigns_smart = fetch_campaign_data(df_smart, headers_smart, 'WB-Smart-Market')

# Concatenate the DataFrames
combined_campaigns = pd.concat([campaigns_guten, campaigns_giper, campaigns_kitchen, campaigns_smart], ignore_index=True)

print("Combined Campaign Data")

# Keep only the desired columns
columns_to_keep = ["endTime", "createTime", "startTime", "name", "advertId", "status", "type", "Project", "Marketplace"]
filtered_df = combined_campaigns[columns_to_keep].copy()
filtered_df['endTime'] = pd.to_datetime(filtered_df['endTime'], format='mixed').dt.date
filtered_df['createTime'] = pd.to_datetime(filtered_df['createTime'], format='mixed').dt.date
filtered_df['startTime'] = pd.to_datetime(filtered_df['startTime'], format='mixed').dt.date

# Mapping dictionaries for 'status' and 'type'
status_mapping = {
    -1: "Кампания в процессе удаления",
    4: "Готова к запуску",
    7: "Кампания завершена",
    8: "Отказался",
    9: "Идут показы",
    11: "Кампания на паузе"
}

type_mapping = {
    4: "Кампания в каталоге (устаревший тип)",
    5: "Кампания в карточке товара (устаревший тип)",
    6: "Кампания в поиске (устаревший тип)",
    7: "Кампания в рекомендациях на главной странице (устаревший тип)",
    8: "Автоматическая кампания",
    9: "Аукцион"
}

# Replace numeric values with their string descriptions
filtered_df['status'] = filtered_df['status'].replace(status_mapping)
filtered_df['type'] = filtered_df['type'].replace(type_mapping)

# Display the updated DataFrame
print(filtered_df)

# API endpoint
url_fullstats = "https://advert-api.wildberries.ru/adv/v2/fullstats"

#!DATE *********************************************************************
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
specific_date = str(yesterday)
#! *************************************************************************

def fetch_campaign_stats(campaign_ids, headers, date, base_url, project_name, chunk_size=100):
    chunks = [campaign_ids['advertId'][i:i + chunk_size].tolist() for i in range(0, len(campaign_ids), chunk_size)]
    all_data = []
    for idx, chunk in enumerate(chunks):
        payload = [{"id": campaign_id, "dates": [date]} for campaign_id in chunk]
        response = requests.post(base_url, headers=headers, json=payload)

        if response.status_code == 200:
            data = response.json()
            all_data.extend(data)
            print(f"Data retrieved successfully for chunk {idx + 1} for {project_name}")
        else:
            print(f"Error for chunk {idx + 1}: {response.status_code}, {response.text}")
            if "no companies with correct intervals" in response.text:
                print("Stopping execution due to invalid interval error")
                break

        time.sleep(65)

    return all_data if all_data else [{}]

# Fetch campaign data
all_campaign_data_guten = fetch_campaign_stats(campaigns_guten, headers_guten, specific_date, url_fullstats, "WB-GutenTech")
all_campaign_data_giper = fetch_campaign_stats(campaigns_giper, headers_giper, specific_date, url_fullstats, "WB-ГиперМаркет")
all_campaign_data_kitchen = fetch_campaign_stats(campaigns_kitchen, headers_kitchen, specific_date, url_fullstats, "WB-KitchenAid")
all_campaign_data_smart = fetch_campaign_stats(campaigns_smart, headers_smart, specific_date, url_fullstats, "WB-Smart-Market")

def flatten_campaign_data(campaign_data):
    flattened_data = []
    
    for entry in campaign_data:
        advertId = entry.get("advertId")
        for day in entry.get("days", []):
            date = day.get("date")
            for app in day.get("apps", []):
                for nm in app.get("nm", []):
                    flattened_data.append({
                        "date": date,
                        "nmId": nm.get("nmId"),
                        "name": nm.get("name"),
                        "views": nm.get("views"),
                        "clicks": nm.get("clicks"),
                        "ctr": nm.get("ctr"),
                        "cpc": nm.get("cpc"),
                        "sum": nm.get("sum"),
                        "atbs": nm.get("atbs"),
                        "orders": nm.get("orders"),
                        "cr": nm.get("cr"),
                        "shks": nm.get("shks"),
                        "sum_price": nm.get("sum_price"),
                        "advertId": advertId,
                    })
    
    # Create DataFrame
    df = pd.DataFrame(flattened_data)
    
    # Convert and clean date column
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    
    return df

# Process all datasets with one line each
df_guten = flatten_campaign_data(all_campaign_data_guten)
df_giper = flatten_campaign_data(all_campaign_data_giper)
df_kitchen = flatten_campaign_data(all_campaign_data_kitchen)
df_smart = flatten_campaign_data(all_campaign_data_smart)

print(df_guten)


def group_and_aggregate(df, project_name):
    grouped_df = (
        df.groupby([df["date"].dt.date, "nmId", "advertId"], as_index=False)
        .agg({
            "date": "first",
            "name": "first",
            "views": "sum",
            "clicks": "sum",
            "ctr": "mean",
            "cpc": "mean",
            "sum": "sum",
            "atbs": "sum",
            "orders": "sum",
            "cr": "mean",
            "shks": "sum",
            "sum_price": "sum",
            "advertId": "first"
        })
    )

    grouped_df['Project'] = project_name
    grouped_df['Marketplace'] = 'Wildberries'

    return grouped_df

# Process each DataFrame using the function
df_grouped_guten = group_and_aggregate(df_guten, 'WB-GutenTech')
df_grouped_giper = group_and_aggregate(df_giper, 'WB-ГиперМаркет')
df_grouped_kitchen = group_and_aggregate(df_kitchen, 'WB-KitchenAid')
df_grouped_smart = group_and_aggregate(df_smart, 'WB-Smart-Market')

# Combine all DataFrames
df_grouped_combined_campaigns = pd.concat([df_grouped_guten, df_grouped_giper, df_grouped_kitchen, df_grouped_smart], ignore_index=True)

# Display the grouped DataFrame
print(df_grouped_combined_campaigns)

# Merge the grouped DataFrame with the filtered_df to add additional columns
df_final = df_grouped_combined_campaigns.merge(
    filtered_df[["advertId", "endTime", "createTime", "startTime", "name", "status", "type"]],
    on="advertId",
    how="left"
)

# Drop the columns 'ctr', 'cpc', and 'cr'
df_final = df_final.drop(columns=["ctr", "cpc", "cr"])

# Rename the columns 'name_x' and 'name_y'
df_final.rename(
    columns={
        "name_x": "name_product",
        "name_y": "name_campaign"
    },
    inplace=True
)

# Display the final DataFrame
print(df_final)

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

# Define the API endpoint
url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail"

# Set fixed date range for testing
yesterday_start = f'{yesterday} 00:00:00'
yesterday_end = f'{yesterday} 23:59:59'

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
                    "timezone": "Europe/Moscow",  # Added as recommended in API docs
                    "brandNames": [],  # Empty array to get all brands
                    "objectIDs": [],   # Empty array to get all objects
                    "nmIDs": []        # Empty array to get all items
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

# Get data for each project
data_guten = get_report(url, headers_guten, yesterday_start, yesterday_end, 1, 'WB-GutenTech')
data_giper = get_report(url, headers_giper, yesterday_start, yesterday_end, 1, 'WB-ГиперМаркет')
data_kitchen = get_report(url, headers_kitchen, yesterday_start, yesterday_end, 1, 'WB-KitchenAid')
data_smart = get_report(url, headers_smart, yesterday_start, yesterday_end, 1, 'WB-Smart-Market')


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
combined_df['Marketplace'] = 'Wildberries'
print("Columns in combined_campaigns:", combined_df.columns.tolist())

# Display the combined DataFrame
print("Combined DataFrame:\n", combined_df.head())

df_copy = combined_df.copy()
df_final_copy = df_final.copy()

# Convert 'day' and 'dt' to datetime for accurate merging
df_final_copy['date'] = pd.to_datetime(df_final_copy['date']).dt.date
df_copy['begin'] = pd.to_datetime(df_copy['begin']).dt.date

# Rename columns in df2 to match df1 for merging
df_copy.rename(columns={'nmID': 'nmId', 'begin': 'day'}, inplace=True)
df_final_copy.rename(columns={'date': 'day'}, inplace=True)

# Merge the DataFrames on 'nmId', 'day', and 'Project'
merged_df_2 = pd.merge(
    df_final_copy,
    df_copy[['nmId', 'day', 'ordersCount', 'ordersSumRub','addToCartCount']],
    on=['nmId', 'day'],
    how='left'
)

# Fill NaN values with 0 for ordersCount and ordersSumRub
merged_df_2['ordersCount'].fillna(0, inplace=True)
merged_df_2['ordersSumRub'].fillna(0, inplace=True)
merged_df_2['addToCartCount'].fillna(0, inplace=True)

# Display the merged DataFrame
print(merged_df_2)

password = os.getenv('ClickHouse')

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

# Ensure date columns are in the correct format for ClickHouse
merged_df_2['day'] = pd.to_datetime(merged_df_2['day'])

# Debugging: Check the data types of the DataFrame
print("Data types of merged_df:")
print(merged_df_2.dtypes)

# Ensure the DataFrame has the correct columns
columns = [
    'nmId', 'day', 'name_product', 'views', 'clicks', 'sum', 'atbs', 'orders', 'shks',
    'sum_price', 'advertId', 'Project', 'Marketplace', 'endTime', 'createTime', 'startTime',
    'name_campaign', 'status', 'type', 'ordersCount', 'ordersSumRub', 'addToCartCount'
]

# Reorder columns to match the expected order
merget_df_copy_2 = merged_df_2[columns]

# Convert DataFrame to a list of tuples for bulk insertion
data = [tuple(row) for row in merget_df_copy_2.to_numpy()]

# Debugging: Check the structure of the data
print("Sample data to insert:", data[:5])

# Define the table name
table_name = 'campaign_data_wb'

# Use the insert method for bulk insertion
#client.insert(table_name, data, column_names=columns)
print("Data inserted successfully!")