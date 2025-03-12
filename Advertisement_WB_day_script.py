import requests
import json
import pandas as pd
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
import os
import time
from clickhouse_connect import get_client

# Load environment variables
load_dotenv()

# Retrieve API keys from environment variables
KeyGuten = os.getenv('KeyGuten')
KeyGiper = os.getenv('KeyGiper')
KeyKitchen = os.getenv('KeyKitchen')
KeySmart = os.getenv("KeySmart")

# Define headers for API requests
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

# API endpoint for campaign count
url_count = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'

# Make GET requests to fetch campaign counts
response_guten = requests.get(url_count, headers=headers_guten)
response_giper = requests.get(url_count, headers=headers_giper)
response_kitchen = requests.get(url_count, headers=headers_kitchen)
response_smart = requests.get(url_count, headers=headers_smart)

# Check if the requests were successful
if all(resp.status_code == 200 for resp in [response_guten, response_giper, response_kitchen, response_smart]):
    data_guten = response_guten.json()
    data_giper = response_giper.json()
    data_kitchen = response_kitchen.json()
    data_smart = response_smart.json()
    print("Data retrieved successfully")
else:
    print(f"Failed to retrieve data. Status codes: {response_guten.status_code}, {response_giper.status_code}, {response_kitchen.status_code}, {response_smart.status_code}")
    print(f"Responses: {response_guten.text}, {response_giper.text}, {response_kitchen.text}, {response_smart.text}")

# Flatten the JSON data into DataFrames
df_guten = pd.json_normalize(data_guten['adverts'], record_path='advert_list', meta=['type', 'status', 'count'])
df_giper = pd.json_normalize(data_giper['adverts'], record_path='advert_list', meta=['type', 'status', 'count'])
df_kitchen = pd.json_normalize(data_kitchen['adverts'], record_path='advert_list', meta=['type', 'status', 'count'])
df_smart = pd.json_normalize(data_smart['adverts'], record_path='advert_list', meta=['type', 'status', 'count'])

# Convert 'changeTime' to datetime format
for df in [df_guten, df_giper, df_kitchen, df_smart]:
    df['changeTime'] = pd.to_datetime(df['changeTime'])

# Reset the index for all DataFrames
df_guten = df_guten.reset_index(drop=True)
df_giper = df_giper.reset_index(drop=True)
df_kitchen = df_kitchen.reset_index(drop=True)
df_smart = df_smart.reset_index(drop=True)

# Function to fetch campaign data
def fetch_campaign_data(chunks, headers, project_name):
    all_campaign_data = []
    url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
    query_params = {"order": "create", "direction": "desc"}
    
    for idx, chunk in enumerate(chunks):
        response = requests.post(url, params=query_params, json=chunk, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_campaign_data.extend(data)
            print(f"Data retrieved successfully for {project_name} chunk {idx + 1}")
        else:
            print(f"Error for {project_name} chunk {idx + 1}: {response.status_code}, {response.text}")
        time.sleep(1)  # Add a delay to avoid hitting API rate limits
    return all_campaign_data

# Create chunks of 50 campaign IDs each
chunk_size = 50
campaign_chunks_guten = [df_guten['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_guten), chunk_size)]
campaign_chunks_giper = [df_giper['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_giper), chunk_size)]
campaign_chunks_kitchen = [df_kitchen['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_kitchen), chunk_size)]
campaign_chunks_smart = [df_smart['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_smart), chunk_size)]

# Fetch campaign data for each project
all_campaign_data_guten = fetch_campaign_data(campaign_chunks_guten, headers_guten, 'WB-GutenTech')
all_campaign_data_giper = fetch_campaign_data(campaign_chunks_giper, headers_giper, 'WB-ГиперМаркет')
all_campaign_data_kitchen = fetch_campaign_data(campaign_chunks_kitchen, headers_kitchen, 'WB-KitchenAid')
all_campaign_data_smart = fetch_campaign_data(campaign_chunks_smart, headers_smart, 'WB-Smart-Market')

# Create DataFrames from the campaign data
campaigns_guten = pd.DataFrame(all_campaign_data_guten)
campaigns_giper = pd.DataFrame(all_campaign_data_giper)
campaigns_kitchen = pd.DataFrame(all_campaign_data_kitchen)
campaigns_smart = pd.DataFrame(all_campaign_data_smart)

# Sort each DataFrame by 'createTime' in descending order
campaigns_guten = campaigns_guten.sort_values(by='createTime', ascending=False)
campaigns_giper = campaigns_giper.sort_values(by='createTime', ascending=False)
campaigns_kitchen = campaigns_kitchen.sort_values(by='createTime', ascending=False)
campaigns_smart = campaigns_smart.sort_values(by='createTime', ascending=False)

# Sort each DataFrame by 'createTime' in descending order
for df in [campaigns_guten, campaigns_giper, campaigns_kitchen, campaigns_smart]:
    df.sort_values(by='createTime', ascending=False, inplace=True)

# Add project and marketplace columns
campaigns_guten['Project'] = 'WB-GutenTech'
campaigns_giper['Project'] = 'WB-ГиперМаркет'
campaigns_kitchen['Project'] = 'WB-KitchenAid'
campaigns_smart['Project'] = 'WB-Smart-Market'

# Concatenate the DataFrames
combined_campaigns = pd.concat([campaigns_guten, campaigns_giper, campaigns_kitchen, campaigns_smart], ignore_index=True)
combined_campaigns['Marketplace'] = 'Wildberries'

# Keep only the desired columns
columns_to_keep = ["endTime", "createTime", "startTime", "name", "advertId", "status", "type", "Project", "Marketplace"]
filtered_df = combined_campaigns[columns_to_keep].copy()

# Convert date columns to datetime
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


# Create chunks of 100 campaign IDs each
chunk_size = 100
campaign_chunks_guten = [df_guten['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_guten), chunk_size)]
campaign_chunks_giper = [df_giper['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_giper), chunk_size)]
campaign_chunks_kitchen = [df_kitchen['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_kitchen), chunk_size)]
campaign_chunks_smart = [df_smart['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_smart), chunk_size)]

# API endpoint for campaign statistics
url_stats = "https://advert-api.wildberries.ru/adv/v2/fullstats"

# Automatically get yesterday's date
yesterday = date.today().replace(day=date.today().day - 1)
specific_date = str(yesterday)

def fetch_campaign_stats(chunks, headers, specific_date):
    all_data = []
    for idx, chunk in enumerate(chunks):
        payload = [{"id": campaign_id, "dates": [specific_date]} for campaign_id in chunk]
        response = requests.post(url_stats, headers=headers, json=payload)
        time.sleep(65)  # Add a delay to avoid hitting API rate limits
        
        if response.status_code == 200:
            try:
                data = response.json()
                if data is None or not isinstance(data, list):  # Validate JSON response
                    print(f"Empty or invalid JSON response for chunk {idx + 1}")
                    continue
                
                all_data.extend(data)
                print(f"Data retrieved successfully for chunk {idx + 1}")
            except ValueError:  # Catch JSON decoding errors
                print(f"Failed to decode JSON for chunk {idx + 1}: {response.text}")
        else:
            print(f"Error for chunk {idx + 1}: {response.status_code}, {response.text}")
    
    return all_data

# Fetch campaign statistics for each project
all_campaign_stats_guten = fetch_campaign_stats(campaign_chunks_guten, headers_guten, specific_date)
all_campaign_stats_giper = fetch_campaign_stats(campaign_chunks_giper, headers_giper, specific_date)
all_campaign_stats_kitchen = fetch_campaign_stats(campaign_chunks_kitchen, headers_kitchen, specific_date)
all_campaign_stats_smart = fetch_campaign_stats(campaign_chunks_smart, headers_smart, specific_date)

# Flatten the JSON data
def flatten_campaign_stats(all_campaign_stats):
    flattened_data = []
    for entry in all_campaign_stats:
        advertId = entry["advertId"]
        for day in entry["days"]:
            date = day["date"]
            for app in day["apps"]:
                for nm in app["nm"]:
                    flattened_data.append({
                        "date": date,
                        "nmId": nm["nmId"],
                        "name": nm["name"],
                        "views": nm["views"],
                        "clicks": nm["clicks"],
                        "ctr": nm["ctr"],
                        "cpc": nm["cpc"],
                        "sum": nm["sum"],
                        "atbs": nm["atbs"],
                        "orders": nm["orders"],
                        "cr": nm["cr"],
                        "shks": nm["shks"],
                        "sum_price": nm["sum_price"],
                        "advertId": advertId
                    })
    return flattened_data

# Flatten campaign statistics for each project
flattened_data_guten = flatten_campaign_stats(all_campaign_stats_guten)
flattened_data_giper = flatten_campaign_stats(all_campaign_stats_giper)
flattened_data_kitchen = flatten_campaign_stats(all_campaign_stats_kitchen)
flattened_data_smart = flatten_campaign_stats(all_campaign_stats_smart)

# Create DataFrames from the flattened data
df_guten_stats = pd.DataFrame(flattened_data_guten)
df_giper_stats = pd.DataFrame(flattened_data_giper)
df_kitchen_stats = pd.DataFrame(flattened_data_kitchen)
df_smart_stats = pd.DataFrame(flattened_data_smart)

# Convert the 'date' column to datetime and remove timezone information
for df in [df_guten_stats, df_giper_stats, df_kitchen_stats, df_smart_stats]:
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

# Group by 'date' (day only) and 'nmId', summing numeric columns except 'advertId'
def group_campaign_stats(df):
    return df.groupby([df["date"].dt.date, "nmId"], as_index=False).agg({
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

# Group campaign statistics for each project
df_grouped_guten = group_campaign_stats(df_guten_stats)
df_grouped_giper = group_campaign_stats(df_giper_stats)
df_grouped_kitchen = group_campaign_stats(df_kitchen_stats)
df_grouped_smart = group_campaign_stats(df_smart_stats)

# Rename the 'date' column to 'day' for clarity
for df in [df_grouped_guten, df_grouped_giper, df_grouped_kitchen, df_grouped_smart]:
    df.rename(columns={"date": "day"}, inplace=True)

# Add project and marketplace columns
df_grouped_guten['Project'] = 'WB-GutenTech'
df_grouped_giper['Project'] = 'WB-ГиперМаркет'
df_grouped_kitchen['Project'] = 'WB-KitchenAid'
df_grouped_smart['Project'] = 'WB-Smart-Market'

# Concatenate the DataFrames
df_grouped_combined = pd.concat([df_grouped_guten, df_grouped_giper, df_grouped_kitchen, df_grouped_smart], ignore_index=True)
df_grouped_combined['Marketplace'] = 'Wildberries'

# Merge the grouped DataFrame with the filtered_df to add additional columns
df_final = df_grouped_combined.merge(
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

print(df_final)
# Insert data into ClickHouse
password = os.getenv('ClickHouse')

# Define connection parameters
client = get_client(
    host='rc1a-vk5i3icccvmfk6cm.mdb.yandexcloud.net',
    port=8443,
    username='user1',
    password=password,
    database='user1',
    secure=True,
    verify=False
)

# Define the table name
table_name = 'campaign_data_wb'

# Convert DataFrame to a list of tuples for bulk insertion
data = [tuple(row) for row in df_final.to_numpy()]

# Define the column names
column_names = [
    'nmId', 'day', 'name_product', 'views', 'clicks', 'sum', 'atbs', 'orders', 'shks', 'sum_price',
    'advertId', 'Project', 'Marketplace', 'endTime', 'createTime', "startTime", 'name_campaign', 'status', 'type'
]

# Use the insert method for bulk insertion
client.insert(table_name, data, column_names=column_names)

print("Data inserted successfully!")
