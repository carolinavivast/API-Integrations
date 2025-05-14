# wildberries_campaign_data.py

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from clickhouse_connect import get_client
from dotenv import load_dotenv
import os
import time

load_dotenv()

# --- CONFIGURATION ---
HEADERS = {
    'WB-GutenTech': {
        'Authorization': os.getenv('KeyGuten'),
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    },
    'WB-ГиперМаркет': {
        'Authorization': os.getenv('KeyGiper'),
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    },
    'WB-KitchenAid': {
        'Authorization': os.getenv('KeyKitchen'),
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    },
    'WB-Smart-Market': {
        'Authorization': os.getenv("KeySmart"),
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
}

PROJECT_NAMES = list(HEADERS.keys())

# --- UTILITY FUNCTIONS ---

def get_yesterday_date(fmt='%Y-%m-%d'):
    return (datetime.now() - timedelta(days=1)).strftime(fmt)

def get_date_interval(yesterday_str):
    return {
        "begin": f"{yesterday_str} 00:00:00",
        "end": f"{yesterday_str} 23:59:59"
    }

# --- API CALLS ---

def fetch_advert_count(headers):
    url = 'https://advert-api.wildberries.ru/adv/v1/promotion/count '
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching advert count: {response.status_code}, {response.text}")
        return None

def fetch_advert_details(chunk, headers, project_name):
    url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts "
    query_params = {"order": "create", "direction": "desc"}
    all_data = []
    for idx, c in enumerate(chunk):
        response = requests.post(url, params=query_params, json=c, headers=headers)
        if response.status_code == 200:
            all_data.extend(response.json())
            print(f"[{project_name}] Fetched chunk {idx + 1}")
        else:
            print(f"[{project_name}] Error in chunk {idx + 1}: {response.status_code}, {response.text}")
        time.sleep(1)
    return pd.DataFrame(all_data)

def fetch_full_stats(campaigns_df, headers, date_interval, project_name):
    url = "https://advert-api.wildberries.ru/adv/v2/fullstats "
    chunk_size = 100
    chunks = [campaigns_df['advertId'][i:i + chunk_size].tolist() for i in range(0, len(campaigns_df), chunk_size)]
    all_data = []

    for idx, chunk in enumerate(chunks):
        payload = [{"id": cid, "interval": date_interval} for cid in chunk]
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            all_data.extend(response.json())
            print(f"[{project_name}] Full stats chunk {idx + 1} fetched")
        else:
            print(f"[{project_name}] Error in full stats chunk {idx + 1}: {response.status_code}, {response.text}")
        time.sleep(65)
    return all_data

def fetch_nm_report(headers, begin, end, project_name):
    url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail "
    page = 1
    all_data = []

    while True:
        request_body = {
            "period": {"begin": begin, "end": end},
            "orderBy": {"field": "ordersSumRub", "mode": "desc"},
            "page": page,
            "timezone": "Europe/Moscow",
            "brandNames": [],
            "objectIDs": [],
            "nmIDs": []
        }
        response = requests.post(url, headers=headers, json=request_body)
        if response.status_code != 200:
            print(f"[{project_name}] NM report request failed: {response.status_code}, {response.text}")
            break

        data = response.json()
        cards = data.get('data', {}).get('cards', [])
        if not cards:
            print(f"[{project_name}] No more NM data")
            break

        all_data.extend(cards)
        print(f"[{project_name}] Page {page} fetched ({len(cards)} items)")
        if not data.get('data', {}).get('isNextPage', False):
            break
        page += 1
        time.sleep(5)
    return all_data

# --- DATA PROCESSING ---

def process_advert_data(data_guten, data_giper, data_kitchen, data_smart):
    dfs = {}
    for project, data in zip(PROJECT_NAMES, [data_guten, data_giper, data_kitchen, data_smart]):
        if not data:
            continue
        df = pd.json_normalize(
            data['adverts'],
            record_path='advert_list',
            meta=['type', 'status', 'count']
        )
        df['changeTime'] = pd.to_datetime(df['changeTime'])
        df = df.reset_index(drop=True)
        dfs[project] = df
    return dfs

def fetch_all_advert_details(advert_dfs, headers_dict):
    campaigns = {}
    for project, df in advert_dfs.items():
        chunk_size = 50
        chunks = [df['advertId'][i:i + chunk_size].tolist() for i in range(0, len(df), chunk_size)]
        campaigns[project] = fetch_advert_details(chunks, headers_dict[project], project)
    return campaigns

def flatten_fullstats_data(fullstats_data):
    flattened = []
    for entry in fullstats_data:
        advertId = entry["advertId"]
        for day in entry["days"]:
            date = day["date"]
            for app in day["apps"]:
                for nm in app["nm"]:
                    flattened.append({
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
    return pd.DataFrame(flattened)

def merge_and_process_data(campaign_data, filtered_df):
    # Merge with campaign metadata
    df_final = campaign_data.merge(
        filtered_df[["advertId", "endTime", "createTime", "startTime", "name", "status", "type"]],
        on="advertId",
        how="left"
    )
    df_final = df_final.drop(columns=["ctr", "cpc", "cr"])
    df_final.rename(columns={"name_x": "name_product", "name_y": "name_campaign"}, inplace=True)
    return df_final

def process_nm_data(nm_data):
    flattened = []
    for card in nm_data:
        sp = card["statistics"]["selectedPeriod"]
        conv = sp["conversions"]
        stocks = card["stocks"]
        flattened.append({
            "nmID": card["nmID"],
            "vendorCode": card["vendorCode"],
            "brandName": card["brandName"],
            "objectID": card["object"]["id"],
            "objectName": card["object"]["name"],
            "begin": sp["begin"],
            "end": sp["end"],
            "openCardCount": sp["openCardCount"],
            "addToCartCount": sp["addToCartCount"],
            "ordersCount": sp["ordersCount"],
            "ordersSumRub": sp["ordersSumRub"],
            "buyoutsCount": sp["buyoutsCount"],
            "buyoutsSumRub": sp["buyoutsSumRub"],
            "cancelCount": sp["cancelCount"],
            "cancelSumRub": sp["cancelSumRub"],
            "avgOrdersCountPerDay": sp["avgOrdersCountPerDay"],
            "avgPriceRub": sp["avgPriceRub"],
            "addToCartPercent": conv["addToCartPercent"],
            "cartToOrderPercent": conv["cartToOrderPercent"],
            "buyoutsPercent": conv["buyoutsPercent"],
            "stocksMp": stocks["stocksMp"],
            "stocksWb": stocks["stocksWb"]
        })
    return pd.DataFrame(flattened)

# --- MAIN FUNCTION ---

def main():
    yesterday = get_yesterday_date()
    date_interval = get_date_interval(yesterday)

    # Step 1: Fetch advert counts
    advert_data = {}
    for project, header in HEADERS.items():
        raw = fetch_advert_count(header)
        if raw:
            advert_data[project] = raw

    # Step 2: Process advert data
    advert_dfs = process_advert_data(*[advert_data.get(p) for p in PROJECT_NAMES])

    # Step 3: Fetch detailed advert info
    campaign_headers = {p: HEADERS[p] for p in advert_dfs}
    campaign_dfs = fetch_all_advert_details(advert_dfs, campaign_headers)

    # Step 4: Fetch full stats
    fullstats_data = {}
    for project, df in campaign_dfs.items():
        data = fetch_full_stats(df, HEADERS[project], date_interval, project)
        fullstats_data[project] = data

    # Step 5: Flatten full stats
    flattened_dfs = {}
    for project, data in fullstats_data.items():
        if data:
            df = flatten_fullstats_data(data)
            df["Project"] = project
            df["Marketplace"] = "Wildberries"
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df.rename(columns={"date": "day"}, inplace=True)
            flattened_dfs[project] = df

    # Step 6: Combine all campaign data
    combined_campaigns = pd.concat(flattened_dfs.values(), ignore_index=True)

    # Step 7: Add readable status/type
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

    # Apply mappings
    combined_campaigns['status'] = combined_campaigns['status'].replace(status_mapping)
    combined_campaigns['type'] = combined_campaigns['type'].replace(type_mapping)

    # Step 8: Fetch NM report
    nm_data = {}
    for project, header in HEADERS.items():
        data = fetch_nm_report(header, date_interval['begin'], date_interval['end'], project)
        nm_data[project] = data

    # Step 9: Flatten NM data
    nm_dfs = {}
    for project, data in nm_data.items():
        if data:
            df = process_nm_data(data)
            df["Project"] = project
            df["Marketplace"] = "Wildberries"
            df["begin"] = pd.to_datetime(df["begin"]).dt.date
            df.rename(columns={'begin': 'day', 'nmID': 'nmId'}, inplace=True)
            nm_dfs[project] = df

    # Step 10: Merge with NM data
    merged_df = pd.merge(
        combined_campaigns,
        pd.concat(nm_dfs.values(), ignore_index=True)[['nmId', 'day', 'ordersCount', 'ordersSumRub', 'addToCartCount']],
        on=['nmId', 'day'],
        how='left'
    )

    # Fill NaN values
    merged_df[['ordersCount', 'ordersSumRub', 'addToCartCount']] = merged_df[['ordersCount', 'ordersSumRub', 'addToCartCount']].fillna(0)

    # Step 11: Insert into ClickHouse
    password = os.getenv('ClickHouse')
    client = get_client(
        host='rc1a-j5ou9lq30ldal602.mdb.yandexcloud.net',
        port=8443,
        username='user1',
        password=password,
        database='user1',
        secure=True,
        verify=False
    )

    columns = [
        'nmId', 'day', 'name_product', 'views', 'clicks', 'sum', 'atbs', 'orders', 'shks',
        'sum_price', 'advertId', 'Project', 'Marketplace', 'endTime', 'createTime', 'startTime',
        'name_campaign', 'status', 'type', 'ordersCount', 'ordersSumRub', 'addToCartCount'
    ]

    merged_df = merged_df[columns]
    data = [tuple(row) for row in merged_df.to_numpy()]
    table_name = 'campaign_data_wb'

    try:
        client.insert(table_name, data, column_names=columns)
        print("✅ Data inserted successfully!")
    except Exception as e:
        print(f"❌ Failed to insert data: {e}")

if __name__ == "__main__":
    main()