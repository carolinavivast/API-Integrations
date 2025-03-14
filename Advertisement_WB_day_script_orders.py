import requests
import json
import pandas as pd
from datetime import datetime, timedelta, date
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
KeySmart = os.getenv('KeySmart')

# Define headers for each project
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

# Function to fetch campaign data
def fetch_campaign_data(url, headers, project_name, chunk_size=50):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data['adverts'], record_path='advert_list', meta=['type', 'status', 'count'])
        df['changeTime'] = pd.to_datetime(df['changeTime'])
        df = df.reset_index(drop=True)
        return df
    else:
        print(f"Failed to retrieve data for {project_name}. Status code: {response.status_code}")
        return pd.DataFrame()

# Function to fetch campaign details
def fetch_campaign_details(url, headers, project_name, campaign_ids, chunk_size=50):
    all_campaign_data = []
    campaign_chunks = [campaign_ids[i:i + chunk_size] for i in range(0, len(campaign_ids), chunk_size)]
    
    for idx, chunk in enumerate(campaign_chunks):
        response = requests.post(url, headers=headers, json=chunk)
        if response.status_code == 200:
            data = response.json()
            all_campaign_data.extend(data)
            print(f"Data retrieved successfully for {project_name} chunk {idx + 1}")
        else:
            print(f"Error for {project_name} chunk {idx + 1}: {response.status_code}, {response.text}")
        time.sleep(1)  # Add a delay to avoid hitting API rate limits
    
    return pd.DataFrame(all_campaign_data)

# Function to fetch campaign statistics
def fetch_campaign_statistics(url, headers, specific_date, campaign_ids, chunk_size=100):
    all_campaign_data = []
    campaign_chunks = [campaign_ids[i:i + chunk_size] for i in range(0, len(campaign_ids), chunk_size)]
    
    for idx, chunk in enumerate(campaign_chunks):
        payload = [{"id": campaign_id, "dates": [specific_date]} for campaign_id in chunk]
        response = requests.post(url, headers=headers, json=payload)
        time.sleep(65)  # Respect the rate limit
        
        if response.status_code == 200:
            try:
                data = response.json()
                if data is not None:  # Check if data is not None
                    all_campaign_data.extend(data)
                    print(f"Data retrieved successfully for chunk {idx + 1}")
                else:
                    print(f"Error: Empty response for chunk {idx + 1}")
            except ValueError as e:
                print(f"Failed to decode JSON for chunk {idx + 1}: {e}")
        else:
            print(f"Error for chunk {idx + 1}: {response.status_code}, {response.text}")
    
    return all_campaign_data

# Function to fetch product statistics
def fetch_product_statistics(url, headers, period, nm_ids, batch_size=20, requests_per_minute=3):
    all_data = []
    interval = 60 / requests_per_minute  # Time interval between requests in seconds
    
    for i in range(0, len(nm_ids), batch_size):
        batch = nm_ids[i:i + batch_size]
        request_body = {"nmIDs": batch, "period": period}
        response = requests.post(url, headers=headers, json=request_body)
        
        if response.status_code == 200:
            try:
                data = response.json()
                if not data.get('error') and 'data' in data:
                    all_data.extend(data['data'])
                    print(f"Data retrieved successfully for batch {i // batch_size + 1}")
                else:
                    print(f"Error in response for batch {i // batch_size + 1}: {data.get('errorText', 'No error text')}")
            except ValueError as e:
                print(f"Failed to decode JSON for batch {i // batch_size + 1}: {e}")
        else:
            print(f"Error for batch {i // batch_size + 1}: {response.status_code}, {response.text}")
        
        time.sleep(interval)  # Respect the rate limit
    
    return all_data

# Function to insert data into ClickHouse
def insert_into_clickhouse(client, table_name, data, columns):
    client.insert(table_name, data, column_names=columns)
    print("Data inserted successfully!")

# Main function to execute the script
def main():
    # Define API endpoints
    campaign_count_url = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    campaign_details_url = 'https://advert-api.wildberries.ru/adv/v1/promotion/adverts'
    campaign_statistics_url = 'https://advert-api.wildberries.ru/adv/v2/fullstats'
    product_statistics_url = 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history'
    
    # Fetch campaign data for each project
    df_guten = fetch_campaign_data(campaign_count_url, headers_guten, 'WB-GutenTech')
    df_giper = fetch_campaign_data(campaign_count_url, headers_giper, 'WB-ГиперМаркет')
    df_kitchen = fetch_campaign_data(campaign_count_url, headers_kitchen, 'WB-KitchenAid')
    df_smart = fetch_campaign_data(campaign_count_url, headers_smart, 'WB-Smart-Market')
    
    # Fetch campaign details for each project
    campaigns_guten = fetch_campaign_details(campaign_details_url, headers_guten, 'WB-GutenTech', df_guten['advertId'].tolist())
    campaigns_giper = fetch_campaign_details(campaign_details_url, headers_giper, 'WB-ГиперМаркет', df_giper['advertId'].tolist())
    campaigns_kitchen = fetch_campaign_details(campaign_details_url, headers_kitchen, 'WB-KitchenAid', df_kitchen['advertId'].tolist())
    campaigns_smart = fetch_campaign_details(campaign_details_url, headers_smart, 'WB-Smart-Market', df_smart['advertId'].tolist())
    
    # Add the 'Project' column to each DataFrame before concatenation
    campaigns_guten['Project'] = 'WB-GutenTech'
    campaigns_giper['Project'] = 'WB-ГиперМаркет'
    campaigns_kitchen['Project'] = 'WB-KitchenAid'
    campaigns_smart['Project'] = 'WB-Smart-Market'
    
    # Combine all campaign data
    combined_campaigns = pd.concat([campaigns_guten, campaigns_giper, campaigns_kitchen, campaigns_smart], ignore_index=True)
    combined_campaigns['Marketplace'] = 'Wildberries'
    print("Columns in combined_campaigns:", combined_campaigns.columns.tolist())

    # Fetch campaign statistics for each project
    yesterday = date.today().replace(day=date.today().day - 1)
    specific_date = str(yesterday)
    
    all_campaign_data_guten = fetch_campaign_statistics(campaign_statistics_url, headers_guten, specific_date, campaigns_guten['advertId'].tolist())
    all_campaign_data_giper = fetch_campaign_statistics(campaign_statistics_url, headers_giper, specific_date, campaigns_giper['advertId'].tolist())
    all_campaign_data_kitchen = fetch_campaign_statistics(campaign_statistics_url, headers_kitchen, specific_date, campaigns_kitchen['advertId'].tolist())
    all_campaign_data_smart = fetch_campaign_statistics(campaign_statistics_url, headers_smart, specific_date, campaigns_smart['advertId'].tolist())
    
    # Flatten the campaign statistics data
    flattened_data_guten = []
    for entry in all_campaign_data_guten:
        advertId = entry["advertId"]
        for day in entry["days"]:
            for app in day["apps"]:
                for nm in app["nm"]:
                    flattened_data_guten.append({
                        "date": day["date"],
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
    
    # Repeat the same process for other projects (giper, kitchen, smart)
    
    # Combine all flattened data into a single DataFrame
    df_guten = pd.DataFrame(flattened_data_guten)
    # Repeat for other projects
    
    # Fetch product statistics for each project
    projects = {
        'WB-GutenTech': headers_guten,
        'WB-ГиперМаркет': headers_giper,
        'WB-KitchenAid': headers_kitchen,
        'WB-Smart-Market': headers_smart
    }
    
    period = {"begin": specific_date, "end": specific_date}
    dataframes = []
    
    for project_name, headers in projects.items():
        filtered_df = combined_campaigns[combined_campaigns['Project'] == project_name]
        if 'nmId' not in filtered_df.columns:
            print(f"Error: 'nmId' column not found in {project_name} data.")
            continue
        unique_nmId_values = filtered_df['nmId'].unique().tolist()
        all_data = fetch_product_statistics(product_statistics_url, headers, period, unique_nmId_values)
  
        # Flatten the nested 'history' data
        flattened_data = []
        for item in all_data:
            nmID = item['nmID']
            imtName = item['imtName']
            vendorCode = item['vendorCode']
            for history in item['history']:
                flattened_data.append({
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
                })
        
        df_copy = pd.DataFrame(flattened_data)
        dataframes.append(df_copy)
    
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Merge the final DataFrame with the combined campaign data
    merged_df_2 = pd.merge(
        combined_campaigns,
        combined_df[['nmID', 'dt', 'ordersCount', 'ordersSumRub', 'addToCartCount']],
        left_on=['nmId', 'day'],
        right_on=['nmID', 'dt'],
        how='left'
    )
    
    # Fill NaN values with 0
    merged_df_2['ordersCount'].fillna(0, inplace=True)
    merged_df_2['ordersSumRub'].fillna(0, inplace=True)
    merged_df_2['addToCartCount'].fillna(0, inplace=True)
    
    # Insert data into ClickHouse
    password = os.getenv('ClickHouse')
    client = get_client(
        host='rc1a-vk5i3icccvmfk6cm.mdb.yandexcloud.net',
        port=8443,
        username='user1',
        password=password,
        database='user1',
        secure=True,
        verify=False
    )
    
    table_name = 'campaign_data_wb'
    columns = [
        'nmId', 'day', 'name_product', 'views', 'clicks', 'sum', 'atbs', 'orders', 'shks',
        'sum_price', 'advertId', 'Project', 'Marketplace', 'endTime', 'createTime', 'startTime',
        'name_campaign', 'status', 'type', 'ordersCount', 'ordersSumRub', 'addToCartCount'
    ]
    
    data = [tuple(row) for row in merged_df_2[columns].to_numpy()]
    insert_into_clickhouse(client, table_name, data, columns)
    print(merged_df_2.head())

if __name__ == "__main__":
    main()
