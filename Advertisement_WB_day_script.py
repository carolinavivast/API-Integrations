import requests
import json
import pandas as pd
from datetime import datetime, timedelta, date
import openpyxl
from clickhouse_connect import get_client
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

# Retrieve API keys from environment variables
KeyGuten = os.getenv('KeyGuten')
KeyGiper = os.getenv('KeyGiper')
KeyKitchen = os.getenv('KeyKitchen')

# API endpoint for campaign count
url_count = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'

# Headers for API requests
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

# Make GET requests to retrieve campaign data
response_guten = requests.get(url_count, headers=headers_guten)
response_giper = requests.get(url_count, headers=headers_giper)
response_kitchen = requests.get(url_count, headers=headers_kitchen)

# Check if the requests were successful
if response_guten.status_code == 200 and response_giper.status_code == 200 and response_kitchen.status_code == 200:
    # Parse the JSON responses
    data_guten = response_guten.json()
    data_giper = response_giper.json()
    data_kitchen = response_kitchen.json()
    print("Data retrieved successfully")
else:
    print(f"Failed to retrieve data. Status codes: {response_guten.status_code}, {response_giper.status_code}, {response_kitchen.status_code}")
    print(f"Responses: {response_guten.text}, {response_giper.text}, {response_kitchen.text}")

# Flatten the JSON data and create DataFrames
df_guten = pd.json_normalize(
    data_guten['adverts'],
    record_path='advert_list',
    meta=['type', 'status', 'count']
)

df_giper = pd.json_normalize(
    data_giper['adverts'],
    record_path='advert_list',
    meta=['type', 'status', 'count']
)

df_kitchen = pd.json_normalize(
    data_kitchen['adverts'],
    record_path='advert_list',
    meta=['type', 'status', 'count']
)

# Convert 'changeTime' to datetime format
df_guten['changeTime'] = pd.to_datetime(df_guten['changeTime'])
df_giper['changeTime'] = pd.to_datetime(df_giper['changeTime'])
df_kitchen['changeTime'] = pd.to_datetime(df_kitchen['changeTime'])

# Filter rows where the year of 'changeTime' is 2024 or later
df_guten = df_guten[df_guten['changeTime'].dt.year >= 2024]
df_giper = df_giper[df_giper['changeTime'].dt.year >= 2024]
df_kitchen = df_kitchen[df_kitchen['changeTime'].dt.year >= 2024]

# Reset the index and drop the old index
df_guten = df_guten.reset_index(drop=True)
df_giper = df_giper.reset_index(drop=True)
df_kitchen = df_kitchen.reset_index(drop=True)

# Create chunks of 50 campaign IDs each
chunk_size = 50
campaign_chunks_guten = [df_guten['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_guten), chunk_size)]
campaign_chunks_giper = [df_giper['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_giper), chunk_size)]
campaign_chunks_kitchen = [df_kitchen['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df_kitchen), chunk_size)]

# Define the API endpoint for campaign details
url_details = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"

# Define the query parameters
query_params = {
    "order": "create",
    "direction": "desc"
}

# Lists to store all campaign data
all_campaign_data_guten = []
all_campaign_data_giper = []
all_campaign_data_kitchen = []

# Iterate over each chunk and send a POST request
for idx, (chunk_guten, chunk_giper, chunk_kitchen) in enumerate(zip(campaign_chunks_guten, campaign_chunks_giper, campaign_chunks_kitchen)):
    # Send the POST request for Guten
    response_guten = requests.post(url_details, params=query_params, json=chunk_guten, headers=headers_guten)
    time.sleep(1)
    
    # Send the POST request for Giper
    response_giper = requests.post(url_details, params=query_params, json=chunk_giper, headers=headers_giper)
    time.sleep(1)
    
    # Send the POST request for Kitchen
    response_kitchen = requests.post(url_details, params=query_params, json=chunk_kitchen, headers=headers_kitchen)
    time.sleep(1)
    
    # Check the response status for all requests
    if response_guten.status_code == 200 and response_giper.status_code == 200 and response_kitchen.status_code == 200:
        # Parse the JSON responses
        data_guten = response_guten.json()
        data_giper = response_giper.json()
        data_kitchen = response_kitchen.json()
        
        # Extend the lists with the retrieved data
        all_campaign_data_guten.extend(data_guten)
        all_campaign_data_giper.extend(data_giper)
        all_campaign_data_kitchen.extend(data_kitchen)
        print("Data retrieved successfully")
    else:
        print(f"Error for Chunk {idx + 1}: {response_guten.status_code}, {response_giper.status_code}, {response_kitchen.status_code}")
        print(f"Responses: {response_guten.text}, {response_giper.text}, {response_kitchen.text}")

# Create DataFrames from the retrieved data
campaigns_guten = pd.DataFrame(all_campaign_data_guten)
campaigns_giper = pd.DataFrame(all_campaign_data_giper)
campaigns_kitchen = pd.DataFrame(all_campaign_data_kitchen)

# Add project and marketplace columns
campaigns_guten['Project'] = 'WB-GutenTech'
campaigns_giper['Project'] = 'WB-ГиперМаркет'
campaigns_kitchen['Project'] = 'WB-KitchenAid'

# Concatenate the DataFrames
combined_campaigns = pd.concat([campaigns_guten, campaigns_giper, campaigns_kitchen], ignore_index=True)
combined_campaigns['Marketplace'] = 'Wildberries'

# Filter and rename columns
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

# API endpoint for full statistics
url_fullstats = "https://advert-api.wildberries.ru/adv/v2/fullstats"

# Automatically get yesterday's date
yesterday = date.today().replace(day=date.today().day - 1)
specific_date = str(yesterday)

# Create chunks of 100 campaign IDs each
chunk_size = 100

# Retrieve full statistics for Guten
campaign_chunks_guten = [campaigns_guten['advertId'][i:i + chunk_size].tolist() for i in range(0, len(campaigns_guten), chunk_size)]
all_campaign_data_guten = []

for idx, chunk in enumerate(campaign_chunks_guten):
    payload = [{"id": campaign_id, "dates": [specific_date]} for campaign_id in chunk]
    response = requests.post(url_fullstats, headers=headers_guten, json=payload)
    time.sleep(65)
    
    if response.status_code == 200:
        data = response.json()
        all_campaign_data_guten.extend(data)
        print("Data retrieved successfully")
    else:
        print(f"Error for Chunk {idx + 1}: {response.status_code}, {response.text}")

# Retrieve full statistics for Giper
campaign_chunks_giper = [campaigns_giper['advertId'][i:i + chunk_size].tolist() for i in range(0, len(campaigns_giper), chunk_size)]
all_campaign_data_giper = []

for idx, chunk in enumerate(campaign_chunks_giper):
    payload = [{"id": campaign_id, "dates": [specific_date]} for campaign_id in chunk]
    response = requests.post(url_fullstats, headers=headers_giper, json=payload)
    time.sleep(65)
    
    if response.status_code == 200:
        data = response.json()
        all_campaign_data_giper.extend(data)
        print("Data retrieved successfully")
    else:
        print(f"Error for Chunk {idx + 1}: {response.status_code}, {response.text}")

# Retrieve full statistics for Kitchen
campaign_chunks_kitchen = [campaigns_kitchen['advertId'][i:i + chunk_size].tolist() for i in range(0, len(campaigns_kitchen), chunk_size)]
all_campaign_data_kitchen = []

for idx, chunk in enumerate(campaign_chunks_kitchen):
    payload = [{"id": campaign_id, "dates": [specific_date]} for campaign_id in chunk]
    response = requests.post(url_fullstats, headers=headers_kitchen, json=payload)
    time.sleep(65)
    
    if response.status_code == 200:
        data = response.json()
        all_campaign_data_kitchen.extend(data)
        print("Data retrieved successfully")
    else:
        print(f"Error for Chunk {idx + 1}: {response.status_code}, {response.text}")

# Combine all campaign data into DataFrames
campaign_guten_df = pd.json_normalize(all_campaign_data_guten)
campaign_giper_df = pd.json_normalize(all_campaign_data_giper)
campaign_kitchen_df = pd.json_normalize(all_campaign_data_kitchen)

# Flatten the JSON data for Guten
flattened_data_guten = []
for entry in all_campaign_data_guten:
    advertId = entry["advertId"]
    for day in entry["days"]:
        date = day["date"]
        for app in day["apps"]:
            for nm in app["nm"]:
                flattened_data_guten.append({
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

# Flatten the JSON data for Giper
flattened_data_giper = []
for entry in all_campaign_data_giper:
    advertId = entry["advertId"]
    for day in entry["days"]:
        date = day["date"]
        for app in day["apps"]:
            for nm in app["nm"]:
                flattened_data_giper.append({
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

# Flatten the JSON data for Kitchen
flattened_data_kitchen = []
for entry in all_campaign_data_kitchen:
    advertId = entry["advertId"]
    for day in entry["days"]:
        date = day["date"]
        for app in day["apps"]:
            for nm in app["nm"]:
                flattened_data_kitchen.append({
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

# Create DataFrames from the flattened data
df_guten = pd.DataFrame(flattened_data_guten)
df_giper = pd.DataFrame(flattened_data_giper)
df_kitchen = pd.DataFrame(flattened_data_kitchen)

# Convert the 'date' column to datetime and remove timezone information
df_guten["date"] = pd.to_datetime(df_guten["date"]).dt.tz_localize(None)
df_giper["date"] = pd.to_datetime(df_giper["date"]).dt.tz_localize(None)
df_kitchen["date"] = pd.to_datetime(df_kitchen["date"]).dt.tz_localize(None)

# Group by 'date' and 'nmId', summing numeric columns
df_grouped_guten = (
    df_guten.groupby([df_guten["date"].dt.date, "nmId"], as_index=False)
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

df_grouped_giper = (
    df_giper.groupby([df_giper["date"].dt.date, "nmId"], as_index=False)
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

df_grouped_kitchen = (
    df_kitchen.groupby([df_kitchen["date"].dt.date, "nmId"], as_index=False)
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

# Rename the 'date' column to 'day' for clarity
df_grouped_guten.rename(columns={"date": "day"}, inplace=True)
df_grouped_giper.rename(columns={"date": "day"}, inplace=True)
df_grouped_kitchen.rename(columns={"date": "day"}, inplace=True)

# Add project and marketplace columns
df_grouped_guten['Project'] = 'WB-GutenTech'
df_grouped_giper['Project'] = 'WB-ГиперМаркет'
df_grouped_kitchen['Project'] = 'WB-KitchenAid'

# Concatenate the DataFrames
df_grouped_combined_campaigns = pd.concat([df_grouped_guten, df_grouped_giper, df_grouped_kitchen], ignore_index=True)
df_grouped_combined_campaigns['Marketplace'] = 'Wildberries'

# Merge with the filtered DataFrame to add additional columns
df_final = df_grouped_combined_campaigns.merge(
    filtered_df[["advertId", "endTime", "createTime", "startTime", "name", "status", "type"]],
    on="advertId",
    how="left"
)

# Drop unnecessary columns
df_final = df_final.drop(columns=["ctr", "cpc", "cr"])

# Rename columns
df_final.rename(
    columns={
        "name_x": "name_product",
        "name_y": "name_campaign"
    },
    inplace=True
)

output_file = 'Campaigns.xlsx'  # Name of the output file
df_final.to_excel(output_file, index=False)  # Set index=False to avoid saving row numbers


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