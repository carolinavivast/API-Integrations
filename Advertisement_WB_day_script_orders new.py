import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import time
from datetime import date
from clickhouse_connect import get_client

# Load environment variables
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

# Fetch campaign statistics for each project
yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
specific_date = str(yesterday)

period = {"begin": specific_date, "end": specific_date}

# Initialize variables
yesterday_start = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
yesterday_end = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d 23:59:59')

# Function to fetch campaign data
def fetch_campaign_data(url, headers, project_name):
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
    query_params = {
        "order": "create",  # Order by the "change" field
        "direction": "desc"
    }
    for idx, chunk in enumerate(campaign_chunks):
        response = requests.post(url, params=query_params, headers=headers, json=chunk)
        if response.status_code == 200:
            data = response.json()
            all_campaign_data.extend(data)
            print(f"Data retrieved successfully for {project_name} chunk {idx + 1}")
        else:
            print(f"Error for {project_name} chunk {idx + 1}: {response.status_code}, {response.text}")
        time.sleep(1)  # Add a delay to avoid hitting API rate limits

    return pd.DataFrame(all_campaign_data)

# Function to fetch campaign statistics using get_report
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

# Function to flatten and group campaign data
def flatten_campaigns(data):
    flattened_data = []
    for entry in data:
        advertId = entry.get("advertId")  # Extract the advertId
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
                        "advertId": advertId  # Add advertId to each row
                    })
    df = pd.DataFrame(flattened_data)
    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.tz_localize(None)

    # Group by 'date' (day only) and 'nmId', summing numeric columns except 'advertId'
    df_grouped = (
        df.groupby([df["date"].dt.date, "nmId", "advertId"], as_index=False)
        .agg({
            "date": "first",  # Keep the first date (to retain the day)
            "name": "first",  # Keep the first name (or customize this logic)
            "views": "sum",
            "clicks": "sum",
            "ctr": "mean",  # Sum or average, depending on your needs
            "cpc": "mean",  # Use mean for 'cpc' (cost per click)
            "sum": "sum",
            "atbs": "sum",
            "orders": "sum",
            "cr": "mean",  # Use mean for 'cr' (conversion rate)
            "shks": "sum",
            "sum_price": "sum",
            "advertId": "first"  # Keep the first 'advertId' (no summing)
        })
    )
    # Rename the 'date' column to 'day' for clarity
    df_grouped.rename(columns={"date": "day"}, inplace=True)
    return df_grouped

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
    #client.insert(table_name, data, column_names=columns)
    print("Data inserted successfully!")

# Main function to execute the script
def main():
    # Define API endpoints
    campaign_count_url = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    campaign_details_url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts"
    campaign_statistics_url = 'https://advert-api.wildberries.ru/adv/v2/fullstats'
    product_statistics_url = 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail'

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

    # Sort each DataFrame by 'createTime' in descending order
    campaigns_guten = campaigns_guten.sort_values(by='createTime', ascending=False)
    campaigns_giper = campaigns_giper.sort_values(by='createTime', ascending=False)
    campaigns_kitchen = campaigns_kitchen.sort_values(by='createTime', ascending=False)
    campaigns_smart = campaigns_smart.sort_values(by='createTime', ascending=False)

    # Add the 'Project' column to each DataFrame before concatenation
    campaigns_guten['Project'] = 'WB-GutenTech'
    campaigns_giper['Project'] = 'WB-ГиперМаркет'
    campaigns_kitchen['Project'] = 'WB-KitchenAid'
    campaigns_smart['Project'] = 'WB-Smart-Market'

    # Combine all campaign data
    combined_campaigns = pd.concat([campaigns_guten, campaigns_giper, campaigns_kitchen, campaigns_smart], ignore_index=True)
    combined_campaigns['Marketplace'] = 'Wildberries'
    print("Columns in combined_campaigns:", combined_campaigns.columns.tolist())

    # Keep only the desired columns
    columns_to_keep = ["endTime", "createTime", "startTime", "name", "advertId", "status", "type", "Project", "Marketplace"]
    # Ensure filtered_df is a copy of the slice, not a view
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
    filtered_df

    # Fetch campaign statistics using get_report
    all_campaign_data_guten = get_report(campaign_statistics_url, headers_guten, yesterday_start, yesterday_end, 1, 'WB-GutenTech')
    all_campaign_data_giper = get_report(campaign_statistics_url, headers_giper, yesterday_start, yesterday_end, 1, 'WB-ГиперМаркет')
    all_campaign_data_kitchen = get_report(campaign_statistics_url, headers_kitchen, yesterday_start, yesterday_end, 1, 'WB-KitchenAid')
    all_campaign_data_smart = get_report(campaign_statistics_url, headers_smart, yesterday_start, yesterday_end, 1, 'WB-Smart-Market')

    # Flatten the campaign statistics data
    df_grouped_guten = flatten_campaigns(all_campaign_data_guten)
    df_grouped_giper = flatten_campaigns(all_campaign_data_giper)
    df_grouped_kitchen = flatten_campaigns(all_campaign_data_kitchen)
    df_grouped_smart = flatten_campaigns(all_campaign_data_smart)

    df_grouped_guten['Project'] = 'WB-GutenTech'
    df_grouped_giper['Project'] = 'WB-ГиперМаркет'
    df_grouped_kitchen['Project'] = 'WB-KitchenAid'
    df_grouped_smart['Project'] = 'WB-Smart-Market'

    # Concatenate the DataFrames
    df_grouped_combined_campaigns = pd.concat([df_grouped_guten, df_grouped_giper, df_grouped_kitchen, df_grouped_smart], ignore_index=True)
    df_grouped_combined_campaigns['Marketplace'] = 'Wildberries'

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
            "name_x": "name_product",  # Rename 'name_x' to 'name_product'
            "name_y": "name_campaign"  # Rename 'name_y' to 'name_campaign'
        },
        inplace=True
    )

    # Fetch product statistics for each project
    projects = {
        'WB-GutenTech': headers_guten,
        'WB-ГиперМаркет': headers_giper,
        'WB-KitchenAid': headers_kitchen,
        'WB-Smart-Market': headers_smart
    }

    dataframes = []

    for project_name, headers in projects.items():
        filtered_df = df_final[df_final['Project'] == project_name]
        unique_nmId_values = filtered_df['nmId'].unique().tolist()
        print(f"Total unique nmId values for {project_name}:", len(unique_nmId_values))
        all_data = get_report(product_statistics_url, headers, specific_date, specific_date, 1, project_name)

        # Flatten the nested 'history' data for easier analysis
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

        # Create a DataFrame for the current project's flattened data
        df_copy = pd.DataFrame(flattened_data)
        dataframes.append(df_copy)

    combined_df = pd.concat(dataframes, ignore_index=True)
    # Creating copies
    df_copy = combined_df.copy()
    df_final_copy = df_final.copy()

    # Convert 'day' and 'dt' to datetime for accurate merging
    df_final_copy['day'] = pd.to_datetime(df_final_copy['day']).dt.date
    df_copy['begin'] = pd.to_datetime(df_copy['begin']).dt.date

    # Rename columns in df2 to match df1 for merging
    df_copy.rename(columns={'nmID': 'nmId', 'begin': 'day'}, inplace=True)

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

    # Insert data into ClickHouse
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
    merged_df_2['day'] = pd.to_datetime(merged_df_2['day'])  # Convert to datetime

    # Debugging: Check the data types of the DataFrame
    print("Data types of merged_df:")
    print(merged_df_2.dtypes)

    table_name = 'campaign_data_wb'
    columns = [
        'nmId', 'day', 'name_product', 'views', 'clicks', 'sum', 'atbs', 'orders', 'shks',
        'sum_price', 'advertId', 'Project', 'Marketplace', 'endTime', 'createTime', 'startTime',
        'name_campaign', 'status', 'type', 'ordersCount', 'ordersSumRub', 'addToCartCount'
    ]
    # Reorder columns to match the expected order
    merged_df_2 = merged_df_2[columns]

    data = [tuple(row) for row in merged_df_2[columns].to_numpy()]

    # Debugging: Check the structure of the data
    insert_into_clickhouse(client, table_name, data, columns)
    print(merged_df_2.head())

if __name__ == "__main__":
    main()
