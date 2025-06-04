##WB Stock FBO with warehouses
# Отчёт об остатках на складах

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from clickhouse_connect import get_client
import time


# Load environment variables from .env file
load_dotenv()

# Retrieve API keys from environment variables
KeyGuten = os.getenv('KeyGuten')
KeyGiper = os.getenv('KeyGiper')
KeyKitchen = os.getenv('KeyKitchen')
KeySmart = os.getenv("KeySmart")

#^-------------------------Here we get the ReportID----------------------------------------------------

# API endpoint
url = 'https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains'

# Query parameters
params = {
    'locale': 'ru',  # Default: "ru"
    'groupByBrand': 'true',  # Разбивка по брендам
    'groupBySubject': 'true',  # Разбивка по предметам
    'groupBySa': 'true',  # Разбивка по артикулам продавца
}

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
# Make the GET request to the API
response_guten = requests.get(url, headers=headers_guten, params=params)
response_giper = requests.get(url, headers=headers_giper, params=params)
response_kitchen = requests.get(url, headers=headers_kitchen, params=params)
response_smart = requests.get(url, headers=headers_smart, params=params)


# Check if the requests were successful
if response_guten.status_code == 200:
    ReportID_guten = response_guten.json()
    print("Warehouse Remains Data (Guten)")
else:
    print(f"Failed to retrieve data (Guten). Status code: {response_guten.status_code}")
    print(f"Response: {response_guten.text}")

if response_giper.status_code == 200:
    ReportID_giper = response_giper.json()
    print("Warehouse Remains Data (Giper)")
else:
    print(f"Failed to retrieve data (Giper). Status code: {response_giper.status_code}")
    print(f"Response: {response_giper.text}")

if response_kitchen.status_code == 200:
    ReportID_kitchen = response_kitchen.json()
    print("Warehouse Remains Data (Kitchen)")
else:
    print(f"Failed to retrieve data (Kitchen). Status code: {response_kitchen.status_code}")
    print(f"Response: {response_kitchen.text}")
if response_smart.status_code == 200:
    # Parse the JSON response
    ReportID_smart = response_smart.json()
    print("Warehouse Remains Data:")
else:
    print(f"Failed to retrieve data. Status code: {response_smart.status_code}")
    print(f"Response: {response_smart.text}")

# Replace with the actual task ID you want to download
TASK_ID_guten = ReportID_guten['data']['taskId']
TASK_ID_giper = ReportID_giper['data']['taskId']
TASK_ID_kitchen = ReportID_kitchen['data']['taskId']
TASK_ID_smart = ReportID_smart['data']['taskId']


# Wait for 10 seconds before making the second request
time.sleep(30)

#^---------------------------Here we get the report------------------------------------------------------------------

# API endpoint with the task ID
url_guten = f'https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{TASK_ID_guten}/download'
url_giper = f'https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{TASK_ID_giper}/download'
url_kitchen = f'https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{TASK_ID_kitchen}/download'
url_smart = f'https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{TASK_ID_smart}/download'

# Make the GET request to the API to download the reports
response_guten_report = requests.get(url_guten, headers=headers_guten)
response_giper_report = requests.get(url_giper, headers=headers_giper)
response_kitchen_report = requests.get(url_kitchen, headers=headers_kitchen)
response_smart_report = requests.get(url_smart, headers=headers_smart)

# Check if the requests were successful
if response_guten_report.status_code == 200 and response_giper_report.status_code == 200 and response_kitchen_report.status_code == 200 and response_smart_report.status_code == 200:
    data_guten = response_guten_report.json()
    df_guten = pd.DataFrame(data_guten)
    print("Task Data Loaded into DataFrame (Guten):")
    
    data_giper = response_giper_report.json()
    df_giper = pd.DataFrame(data_giper)
    print("Task Data Loaded into DataFrame (Giper):")
    
    data_kitchen = response_kitchen_report.json()
    df_kitchen = pd.DataFrame(data_kitchen)
    print("Task Data Loaded into DataFrame (Kitchen):")
    
    data_smart = response_smart_report.json()
    df_smart = pd.DataFrame(data_smart)
    print("Task Data Loaded into DataFrame (Smart):")

    # Normalize the JSON data for each project
    df_guten = pd.json_normalize(data_guten, record_path=['warehouses'], meta=['brand', 'subjectName', 'vendorCode', 'inWayToClient', 'inWayFromClient', 'quantityWarehousesFull'], errors='ignore')
    df_guten['Project'] = 'WB-GutenTech'

    df_giper = pd.json_normalize(data_giper, record_path=['warehouses'], meta=['brand', 'subjectName', 'vendorCode', 'inWayToClient', 'inWayFromClient', 'quantityWarehousesFull'], errors='ignore')
    df_giper['Project'] = 'WB-ГиперМаркет'

    df_kitchen = pd.json_normalize(data_kitchen, record_path=['warehouses'], meta=['brand', 'subjectName', 'vendorCode', 'inWayToClient', 'inWayFromClient', 'quantityWarehousesFull'], errors='ignore')
    df_kitchen['Project'] = 'WB-KitchenAid'
    
    df_smart = pd.json_normalize(data_smart, record_path=['warehouses'], meta=['brand', 'subjectName', 'vendorCode', 'inWayToClient', 'inWayFromClient', 'quantityWarehousesFull'],errors='ignore')
    df_smart['Project'] = 'WB-Smart-Market'
    
    # Combine the dataframes
    combined_df = pd.concat([df_guten, df_giper, df_kitchen,df_smart], axis=0, ignore_index=True)

    # Add the current date and marketplace information
    #today = datetime.now() - timedelta(days=6)
    today = datetime.now()
    combined_df['Date'] = today
    combined_df['Marketplace'] = 'Wildberries'
    combined_df['brand'] = combined_df['brand'].str.upper()
    combined_df['brand'] = combined_df['brand'].fillna('').astype(str)

    # Save the combined data to an Excel file
    #output_file = 'warehouse_data.xlsx'  # Name of the output file
    #existing_df = pd.read_excel(output_file)
    #updated_df = pd.concat([existing_df, combined_df], axis=0, ignore_index=True)
    #saving the data to excel
    #updated_df.to_excel(output_file, index=False)  # Set index=False to avoid saving row numbers
    #print(f"Data saved to {output_file}")
    
    #^------------------------------------Here we sent it to the database---------------------------------------------------
        
    # Define connection parameters for ClickHouse
    password = os.getenv('ClickHouse')
    # Define connection parameters

    client = get_client(
        host='rc1a-j5ou9lq30ldal602.mdb.yandexcloud.net',  # Your Yandex Cloud ClickHouse host
        port=8443,                                          # Yandex Cloud uses port 8443 for HTTPS
        username='user1',                                      # Your ClickHouse username
        password= password,                           # Your ClickHouse password
        database='user1',                            # Your database name
        secure=True,                                        # Use HTTPS
        verify=False                                        # Disable SSL certificate verification 
        # Define the data to insert
    )

    # Insert data into ClickHouse
    table_name = 'warehouse_data_wb'
    data = [tuple(row) for row in combined_df.to_numpy()]
    column_names = [
        'warehouseName', 'quantity', 'brand', 'subjectName', 'vendorCode',
        'inWayToClient', 'inWayFromClient', 'quantityWarehousesFull',
        'Project', 'Date', 'Marketplace'
    ]

    # Use the insert method for bulk insertion
    client.insert(table_name, data, column_names=column_names)
    print("Data inserted successfully into ClickHouse!")
    
else:
    print(f"Failed to download task data:Guten, Giper and Kitchen Status code: {response_guten_report.status_code, response_giper_report.status_code,response_kitchen_report.status_code,response_smart_report.status_code}")
    print(f"Response: {response_guten_report.text,response_giper_report.text,response_kitchen_report.text,response_smart_report.text}")
