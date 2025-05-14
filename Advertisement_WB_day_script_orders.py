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

# API endpoint
url_history = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"

# Define headers and project names
projects = {
    'WB-GutenTech': headers_guten,
    'WB-ГиперМаркет': headers_giper,
    'WB-KitchenAid': headers_kitchen,
    'WB-Smart-Market': headers_smart
}

# Define the period for the report
period = {
    "begin": yesterday,
    "end": yesterday
}

# Function to send a single POST request
def send_request(nm_ids, period, headers):
    request_body = {
        "nmIDs": nm_ids,
        "period": period
    }
    response = requests.post(url_history, headers=headers, json=request_body)
    return response

# Function to fetch data in batches
def fetch_data_in_batches(nm_ids, period, headers, project_name):
    all_data = []
    batch_size = 20
    requests_per_minute = 3
    interval = 60 / requests_per_minute

    for i in range(0, len(nm_ids), batch_size):
        batch = nm_ids[i:i + batch_size]
        response = send_request(batch, period, headers)

        if response.status_code == 200:
            try:
                data = response.json()
                if not data.get('error') and 'data' in data:
                    all_data.extend(data['data'])
                    print(f"Data retrieved successfully for batch {i // batch_size + 1} for {project_name}")
                else:
                    print(f"Error in response for batch {i // batch_size + 1}: {data.get('errorText', 'No error text')}")
            except ValueError as e:
                print(f"Failed to decode JSON for batch {i // batch_size + 1}: {e}")
        else:
            print(f"Error for batch {i // batch_size + 1}: {response.status_code}, {response.text}")
            break

        time.sleep(interval)

    return all_data

# List to store DataFrames for each project
dataframes = []

# Process each project
for project_name, headers in projects.items():
    filtered_df = df_final[df_final['Project'] == project_name]
    unique_nmId_values = filtered_df['nmId'].unique().tolist()
    print(f"Total unique nmId values for {project_name}:", len(unique_nmId_values))

    all_data = fetch_data_in_batches(unique_nmId_values, period, headers, project_name)

    flattened_data = []
    for item in all_data:
        nmID = item['nmID']
        imtName = item['imtName']
        vendorCode = item['vendorCode']
        for history in item['history']:
            history_entry = {
                'nmID': nmID,
                'imtName': imtName,
                'vendorCode': vendorCode,
                'dt': history['dt'],
                'openCardCount': history['openCardCount'],
                'addToCartCount': history['addToCartCount'],
                'addToCartConversion': history['addToCartConversion'],
                'ordersCount': history['ordersCount'],
                'ordersSumRub': history['ordersSumRub'],
                'cartToOrderConversion': history['cartToOrderConversion'],
                'buyoutsCount': history['buyoutsCount'],
                'buyoutsSumRub': history['buyoutsSumRub'],
                'buyoutPercent': history['buyoutPercent']
            }
            flattened_data.append(history_entry)

    df_copy = pd.DataFrame(flattened_data)
    print(f"Data for {project_name}:\n", df_copy.head())

    dataframes.append(df_copy)

# Concatenate all DataFrames into a single DataFrame
combined_df = pd.concat(dataframes, ignore_index=True)

# Display the combined DataFrame
print("Combined DataFrame:\n", combined_df.head())

df_copy = combined_df.copy()
df_final_copy = df_final.copy()

# Convert 'day' and 'dt' to datetime for accurate merging
df_final_copy['date'] = pd.to_datetime(df_final_copy['date']).dt.date
df_copy['dt'] = pd.to_datetime(df_copy['dt']).dt.date

# Rename columns in df2 to match df1 for merging
df_copy.rename(columns={'nmID': 'nmId', 'dt': 'day'}, inplace=True)
df_final_copy.rename(columns={'date': 'day'}, inplace=True)

# Merge the DataFrames on 'nmId', 'day', and 'Project'
merged_df_2 = pd.merge(
    df_final_copy,
    df_copy[['nmId', 'day', 'ordersCount', 'ordersSumRub', 'addToCartCount']],
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
client.insert(table_name, data, column_names=columns)
print("Data inserted successfully!")