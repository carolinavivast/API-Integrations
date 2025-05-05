import requests
import pandas as pd
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import os
import time
from clickhouse_connect import get_client

# Load environment variables
load_dotenv()

class WildberriesDataPipeline:
    def __init__(self):
        # Initialize API keys and headers
        self.projects = {
            'WB-GutenTech': os.getenv('KeyGuten'),
            'WB-ГиперМаркет': os.getenv('KeyGiper'),
            'WB-KitchenAid': os.getenv('KeyKitchen'),
            'WB-Smart-Market': os.getenv('KeySmart')
        }
        self.password = os.getenv('ClickHouse')
        
        # API endpoints
        self.endpoints = {
            'campaign_count': 'https://advert-api.wildberries.ru/adv/v1/promotion/count',
            'campaign_details': 'https://advert-api.wildberries.ru/adv/v1/promotion/adverts',
            'campaign_stats': 'https://advert-api.wildberries.ru/adv/v2/fullstats',
            'product_stats': 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail'
        }
        
        # Date setup
        self.yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.period = {"begin": self.yesterday, "end": self.yesterday}
        self.yesterday_start = f"{self.yesterday} 00:00:00"
        self.yesterday_end = f"{self.yesterday} 23:59:59"
        
        # Mappings
        self.status_mapping = {
            -1: "Кампания в процессе удаления",
            4: "Готова к запуску",
            7: "Кампания завершена",
            8: "Отказался",
            9: "Идут показы",
            11: "Кампания на паузе"
        }
        
        self.type_mapping = {
            4: "Кампания в каталоге",
            5: "Кампания в карточке товара",
            6: "Кампания в поиске",
            7: "Кампания в рекомендациях",
            8: "Автоматическая кампания",
            9: "Аукцион"
        }

    def get_headers(self, api_key):
        return {
            'Authorization': api_key,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

    def fetch_data(self, url, headers, method='get', params=None, json_data=None, retries=3):
        for attempt in range(retries):
            try:
                if method == 'get':
                    response = requests.get(url, headers=headers, params=params)
                else:
                    response = requests.post(url, headers=headers, json=json_data)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    wait = int(response.headers.get('Retry-After', 30))
                    print(f"Rate limited. Waiting {wait} seconds...")
                    time.sleep(wait)
                    continue
                else:
                    print(f"Error {response.status_code}: {response.text}")
            except Exception as e:
                print(f"Request failed: {str(e)}")
            
            if attempt < retries - 1:
                time.sleep(5)
        
        return None

    def get_campaign_data(self, project_name, api_key):
        # Get campaign list
        data = self.fetch_data(
            self.endpoints['campaign_count'],
            self.get_headers(api_key)
        )
        
        if not data:
            return pd.DataFrame()
            
        df = pd.json_normalize(data['adverts'], record_path='advert_list', meta=['type', 'status', 'count'])
        df['changeTime'] = pd.to_datetime(df['changeTime'])
        return df.reset_index(drop=True)

    def get_campaign_details(self, project_name, api_key, campaign_ids):
        chunks = [campaign_ids[i:i+50] for i in range(0, len(campaign_ids), 50)]
        all_data = []
        
        for chunk in chunks:
            data = self.fetch_data(
                self.endpoints['campaign_details'],
                self.get_headers(api_key),
                method='post',
                params={"order": "create", "direction": "desc"},
                json_data=chunk
            )
            if data:
                all_data.extend(data)
            time.sleep(1)
        
        df = pd.DataFrame(all_data)
        df['Project'] = project_name
        return df.sort_values('createTime', ascending=False)

    def get_campaign_stats(self, project_name, api_key, campaign_ids):
        stats = []
        for campaign_id in campaign_ids:
            data = self.fetch_data(
                self.endpoints['campaign_stats'],
                self.get_headers(api_key),
                method='post',
                json_data=[{
                    "id": campaign_id,
                    "dates": [self.yesterday]
                }]
            )
            if data:
                stats.extend(data)
            time.sleep(65)  # Rate limit
        
        return self.process_campaign_stats(stats)

    def process_campaign_stats(self, stats_data):
        flattened = []
        for entry in stats_data:
            advert_id = entry.get("advertId")
            for day in entry.get("days", []):
                date = day.get("date")
                for app in day.get("apps", []):
                    for nm in app.get("nm", []):
                        flattened.append({
                            "date": date,
                            "nmId": nm.get("nmId"),
                            "name": nm.get("name"),
                            "views": nm.get("views"),
                            "clicks": nm.get("clicks"),
                            "sum": nm.get("sum"),
                            "atbs": nm.get("atbs"),
                            "orders": nm.get("orders"),
                            "shks": nm.get("shks"),
                            "sum_price": nm.get("sum_price"),
                            "advertId": advert_id
                        })
        
        df = pd.DataFrame(flattened)
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
        return df

    def get_product_stats(self, project_name, api_key, nm_ids):
        all_data = []
        for nm_id in nm_ids:
            data = self.fetch_data(
                self.endpoints['product_stats'],
                self.get_headers(api_key),
                method='post',
                json_data={
                    "nmIDs": [nm_id],
                    "period": self.period
                }
            )
            if data and not data.get('error'):
                all_data.extend(data.get('data', []))
            time.sleep(20)  # Rate limit
        
        return self.process_product_stats(all_data)

    def process_product_stats(self, product_data):
        flattened = []
        for item in product_data:
            for history in item.get('history', []):
                flattened.append({
                    'nmID': item['nmID'],
                    'dt': history['dt'],
                    'ordersCount': history['ordersCount'],
                    'ordersSumRub': history['ordersSumRub'],
                    'addToCartCount': history['addToCartCount']
                })
        return pd.DataFrame(flattened)

    def run_pipeline(self):
        all_campaigns = []
        all_stats = []
        
        # Process each project
        for project_name, api_key in self.projects.items():
            print(f"Processing {project_name}...")
            
            # Get campaign data
            campaigns = self.get_campaign_data(project_name, api_key)
            if campaigns.empty:
                continue
                
            # Get campaign details
            details = self.get_campaign_details(project_name, api_key, campaigns['advertId'].tolist())
            
            # Get campaign statistics
            stats = self.get_campaign_stats(project_name, api_key, details['advertId'].tolist())
            stats['Project'] = project_name
            
            # Merge campaign data
            merged = stats.merge(
                details[["advertId", "endTime", "createTime", "startTime", "name", "status", "type"]],
                on="advertId",
                how="left"
            )
            merged.rename(columns={
                "name": "name_campaign",
                "date": "day"
            }, inplace=True)
            
            all_campaigns.append(merged)
            
            # Get product statistics
            nm_ids = merged['nmId'].unique().tolist()
            if nm_ids:
                product_stats = self.get_product_stats(project_name, api_key, nm_ids)
                if not product_stats.empty:
                    product_stats['day'] = pd.to_datetime(product_stats['dt']).dt.date
                    merged = merged.merge(
                        product_stats[['nmID', 'day', 'ordersCount', 'ordersSumRub', 'addToCartCount']],
                        left_on=['nmId', 'day'],
                        right_on=['nmID', 'day'],
                        how='left'
                    )
                    merged.drop(columns=['nmID'], inplace=True)
                    merged.fillna(0, inplace=True)
            
            all_stats.append(merged)
        
        # Combine all data
        final_df = pd.concat(all_stats, ignore_index=True)
        final_df['Marketplace'] = 'Wildberries'
        
        # Clean and transform data
        final_df['status'] = final_df['status'].replace(self.status_mapping)
        final_df['type'] = final_df['type'].replace(self.type_mapping)
        for col in ['endTime', 'createTime', 'startTime']:
            final_df[col] = pd.to_datetime(final_df[col]).dt.date
        
        # Insert into ClickHouse
        self.insert_to_clickhouse(final_df)

    def insert_to_clickhouse(self, df):
        client = get_client(
            host='rc1a-j5ou9lq30ldal602.mdb.yandexcloud.net',
            port=8443,
            username='user1',
            password=self.password,
            database='user1',
            secure=True,
            verify=False
        )
        
        columns = [
            'nmId', 'day', 'name', 'views', 'clicks', 'sum', 'atbs', 'orders', 'shks',
            'sum_price', 'advertId', 'Project', 'Marketplace', 'endTime', 'createTime', 
            'startTime', 'name_campaign', 'status', 'type', 'ordersCount', 
            'ordersSumRub', 'addToCartCount'
        ]
        
        data = [tuple(row) for row in df[columns].to_numpy()]
        #client.insert('campaign_data_wb', data, column_names=columns)
        print("Data successfully inserted into ClickHouse")

if __name__ == "__main__":
    pipeline = WildberriesDataPipeline()
    pipeline.run_pipeline()