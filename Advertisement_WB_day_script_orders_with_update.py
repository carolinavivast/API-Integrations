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
clickhouse_password = os.getenv('ClickHouse')

# API endpoints
url_promotion_count = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
url_promotion_adv = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
url_fullstats = "https://advert-api.wildberries.ru/adv/v2/fullstats"
url_history = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail/history"

# Headers for each project
headers = {
    'WB-GutenTech': {'Authorization': KeyGuten, 'Accept': 'application/json'},
    'WB-ГиперМаркет': {'Authorization': KeyGiper, 'Accept': 'application/json'},
    'WB-KitchenAid': {'Authorization': KeyKitchen, 'Accept': 'application/json'},
    'WB-Smart-Market': {'Authorization': KeySmart, 'Accept': 'application/json'}
}

# Function to process advert data
def process_advert_data(data):
    df = pd.json_normalize(
        data['adverts'],
        record_path='advert_list',
        meta=['type', 'status', 'count']
    )
    df['changeTime'] = pd.to_datetime(df['changeTime'])
    return df.reset_index(drop=True)

# Function to fetch campaign data
def fetch_campaign_data(df, project_headers, project_name, chunk_size=50):
    chunks = [df['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df), chunk_size)]
    all_campaign_data = []
    for idx, chunk in enumerate(chunks):
        response = requests.post(
            url_promotion_adv,
            params={"order": "create", "direction": "desc"},
            json=chunk,
            headers=project_headers
        )
        if response.status_code == 200:
            all_campaign_data.extend(response.json())
            print(f"Data retrieved successfully for {project_name} chunk {idx + 1}")
        else:
            print(f"Error for {project_name} chunk {idx + 1}: {response.status_code}, {response.text}")
        time.sleep(1)
    
    campaign_df = pd.DataFrame(all_campaign_data)
    campaign_df = campaign_df.sort_values(by='createTime', ascending=False)
    campaign_df['Project'] = project_name
    campaign_df['Marketplace'] = 'Wildberries'
    return campaign_df

# Function to fetch campaign stats for multiple dates
def fetch_campaign_stats(campaign_ids, headers, dates, project_name, chunk_size=100):
    all_data = []
    for date in dates:
        chunks = [campaign_ids['advertId'][i:i + chunk_size].tolist() for i in range(0, len(campaign_ids), chunk_size)]
        for idx, chunk in enumerate(chunks):
            payload = [{"id": campaign_id, "dates": [date]} for campaign_id in chunk]
            response = requests.post(url_fullstats, headers=headers, json=payload)
            
            if response.status_code == 200:
                all_data.extend(response.json())
                print(f"Data retrieved successfully for {date}, chunk {idx + 1} for {project_name}")
            else:
                print(f"Error for {date}, chunk {idx + 1}: {response.status_code}, {response.text}")
                if "no companies with correct intervals" in response.text:
                    print("Stopping execution due to invalid interval error")
                    break
            
            time.sleep(65)  # Respect API rate limits
    
    return all_data if all_data else [{}]

# Function to flatten campaign data
def flatten_campaign_data(campaign_data):
    flattened_data = []
    for entry in campaign_data:
        advertId = entry.get("advertId")
        for day in entry.get("days", []):
            date = day.get("date")
            for app in day.get("apps", []):
                for nm in app.get("nm", []):
                    flattened_data.append({
                        "date": date, "nmId": nm.get("nmId"), "name": nm.get("name"),
                        "views": nm.get("views"), "clicks": nm.get("clicks"), "ctr": nm.get("ctr"),
                        "cpc": nm.get("cpc"), "sum": nm.get("sum"), "atbs": nm.get("atbs"),
                        "orders": nm.get("orders"), "cr": nm.get("cr"), "shks": nm.get("shks"),
                        "sum_price": nm.get("sum_price"), "advertId": advertId,
                    })
    return pd.DataFrame(flattened_data)

# Function to group and aggregate data
def group_and_aggregate(df, project_name):
    try:
        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # Drop rows with invalid dates
        df = df.dropna(subset=['date'])
        
        # Group by date (as date only), nmId, and advertId
        grouped_df = df.groupby([df["date"].dt.date, "nmId", "advertId"], as_index=False).agg({
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
        
        grouped_df['Project'] = project_name
        grouped_df['Marketplace'] = 'Wildberries'
        return grouped_df
    
    except Exception as e:
        print(f"Error in group_and_aggregate: {str(e)}")
        print("Problematic 'date' values:")
        print(df['date'].unique()[:10])  # Show first 10 unique dates for debugging
        raise

# Function to fetch historical data
def fetch_historical_data(nm_ids, dates, headers, project_name):
    all_data = []
    batch_size = 20
    requests_per_minute = 3
    interval = 60 / requests_per_minute
    
    for date in dates:
        for i in range(0, len(nm_ids), batch_size):
            batch = nm_ids[i:i + batch_size]
            request_body = {"nmIDs": batch, "period": {"begin": date, "end": date}}
            response = requests.post(url_history, headers=headers, json=request_body)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if not data.get('error') and 'data' in data:
                        all_data.extend(data['data'])
                        print(f"Data retrieved successfully for {date}, batch {i // batch_size + 1} for {project_name}")
                except ValueError as e:
                    print(f"Failed to decode JSON for {date}, batch {i // batch_size + 1}: {e}")
            else:
                print(f"Error for {date}, batch {i // batch_size + 1}: {response.status_code}, {response.text}")
            
            time.sleep(interval)
    
    return all_data

# Function to flatten historical data
def flatten_historical_data(historical_data):
    flattened_data = []
    for item in historical_data:
        for history in item['history']:
            flattened_data.append({
                'nmID': item['nmID'], 'imtName': item['imtName'], 'vendorCode': item['vendorCode'],
                'dt': history['dt'], 'openCardCount': history['openCardCount'],
                'addToCartCount': history['addToCartCount'], 'addToCartConversion': history['addToCartConversion'],
                'ordersCount': history['ordersCount'], 'ordersSumRub': history['ordersSumRub'],
                'cartToOrderConversion': history['cartToOrderConversion'], 'buyoutsCount': history['buyoutsCount'],
                'buyoutsSumRub': history['buyoutsSumRub'], 'buyoutPercent': history['buyoutPercent']
            })
    return pd.DataFrame(flattened_data)

# Main execution
def main():
    # Get date range for last 7 days
    end_date = (datetime.now() - timedelta(days=1)).date()  # Yesterday
    start_date = end_date - timedelta(days=5)
    
    date_range = [str(start_date + timedelta(days=i)) for i in range(6)]
    print(f"Fetching data for dates: {date_range}")

    # Initialize ClickHouse client
    ch_client = get_client(
        host='rc1a-j5ou9lq30ldal602.mdb.yandexcloud.net',
        port=8443,
        username='user1',
        password=clickhouse_password,
        database='user1',
        secure=True,
        verify=False
    )

    # Process each project
    all_final_data = []
    for project_name, project_headers in headers.items():
        print(f"\nProcessing project: {project_name}")
        
        # Step 1: Get promotion count
        response = requests.get(url_promotion_count, headers=project_headers)
        if response.status_code != 200:
            print(f"Failed to retrieve data for {project_name}. Status code: {response.status_code}")
            continue
        
        # Step 2: Process advert data
        df_advert = process_advert_data(response.json())
        
        # Step 3: Fetch campaign data
        df_campaign = fetch_campaign_data(df_advert, project_headers, project_name)
        
        # Filter and prepare campaign data
        columns_to_keep = ["endTime", "createTime", "startTime", "name", "advertId", "status", "type"]
        filtered_campaign = df_campaign[columns_to_keep].copy()
        filtered_campaign['endTime'] = pd.to_datetime(filtered_campaign['endTime'], format='mixed').dt.date
        filtered_campaign['createTime'] = pd.to_datetime(filtered_campaign['createTime'], format='mixed').dt.date
        filtered_campaign['startTime'] = pd.to_datetime(filtered_campaign['startTime'], format='mixed').dt.date
        
        # Map status and type values
        status_mapping = {
            -1: "Кампания в процессе удаления", 4: "Готова к запуску", 7: "Кампания завершена",
            8: "Отказался", 9: "Идут показы", 11: "Кампания на паузе"
        }
        type_mapping = {
            4: "Кампания в каталоге", 5: "Кампания в карточке товара", 6: "Кампания в поиске",
            7: "Кампания в рекомендациях", 8: "Автоматическая кампания", 9: "Аукцион"
        }
        filtered_campaign['status'] = filtered_campaign['status'].replace(status_mapping)
        filtered_campaign['type'] = filtered_campaign['type'].replace(type_mapping)
        
        # Step 4: Fetch campaign stats for each date
        campaign_stats = fetch_campaign_stats(df_campaign, project_headers, date_range, project_name)
        df_stats = flatten_campaign_data(campaign_stats)
        
        if df_stats.empty:
            print(f"No stats data for {project_name}")
            continue
        
        # Step 5: Group and aggregate stats
        df_grouped = group_and_aggregate(df_stats, project_name)
        
        # Step 6: Fetch historical data
        unique_nm_ids = df_grouped['nmId'].unique().tolist()
        historical_data = fetch_historical_data(unique_nm_ids, date_range, project_headers, project_name)
        df_history = flatten_historical_data(historical_data)
        
        # Step 7: Merge all data
        df_grouped['date'] = pd.to_datetime(df_grouped['date']).dt.date
        df_history['dt'] = pd.to_datetime(df_history['dt']).dt.date
        
        # Rename columns in df2 to match df1 for merging
        df_history.rename(columns={'nmID': 'nmId', 'dt': 'day'}, inplace=True)
        df_grouped.rename(columns={'date': 'day'}, inplace=True)
        
        merged_df = pd.merge(
            df_history,
            df_grouped[['nmId', 'day', 'name_product', 'views', 'clicks', 'sum', 'atbs',
            'orders', 'shks', 'sum_price', 'advertId', 'Project', 'Marketplace',
            'endTime', 'createTime', 'startTime', 'name_campaign', 'status',
            'type']],
            on=['nmId', 'day'],
            how='left'
        )
        
        # Fill NaN values
        merged_df['ordersCount'].fillna(0, inplace=True)
        merged_df['ordersSumRub'].fillna(0, inplace=True)
        merged_df['addToCartCount'].fillna(0, inplace=True)
        
        # Merge with campaign info
        final_df = merged_df.merge(
            filtered_campaign,
            on='advertId',
            how='left'
        )
        
        # Rename columns
        final_df.rename(columns={
            'name_x': 'name_product',
            'name_y': 'name_campaign',
            'date': 'day'
        }, inplace=True)
        
        # Select final columns
        columns = [
            'nmId', 'day', 'name_product', 'views', 'clicks', 'sum', 'atbs', 'orders', 'shks',
            'sum_price', 'advertId', 'Project', 'Marketplace', 'endTime', 'createTime', 'startTime',
            'name_campaign', 'status', 'type', 'ordersCount', 'ordersSumRub', 'addToCartCount'
        ]
        final_df = final_df[columns]
        
        all_final_data.append(final_df)
    
    # Combine all project data
    if not all_final_data:
        print("No data to insert")
        return
    
    final_combined = pd.concat(all_final_data, ignore_index=True)
    
    # Convert date columns
    final_combined['day'] = pd.to_datetime(final_combined['day'])
    
    # Prepare data for ClickHouse
    data_to_insert = [tuple(row) for row in final_combined.to_numpy()]
    
    # Define table name and columns
    table_name = 'campaign_data_wb'
    
    # Delete existing data for these dates to avoid duplicates
    delete_query = f"ALTER TABLE {table_name} DELETE WHERE day >= '{start_date}' AND day <= '{end_date}'"
    ch_client.command(delete_query)
    print(f"Deleted existing data for dates {start_date} to {end_date}")
    
    # Insert new data
    ch_client.insert(table_name, data_to_insert, column_names=columns)
    print(f"Successfully inserted data for dates {start_date} to {end_date}")

if __name__ == "__main__":
    main()