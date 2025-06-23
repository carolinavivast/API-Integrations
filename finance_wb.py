import requests
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, date
import pandas as pd
from clickhouse_connect import get_client
import time
from typing import List, Dict
import logging
import numpy as np

# Load environment variables from .env file
load_dotenv()
KeyGuten = os.getenv("KeyGuten")
KeyGiper = os.getenv("KeyGiper")
KeyKitchen = os.getenv("KeyKitchen")
KeySmart = os.getenv("KeySmart")
password = os.getenv('ClickHouse')

class WildberriesAPI:
    def __init__(self, api_key: str, project_name: str):
        self.api_key = api_key
        self.project_name = project_name
        self.base_url = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"
        self.last_request = 0

    def _wait_if_needed(self):
        """Wait if less than 60 seconds since last request"""
        elapsed = time.time() - self.last_request
        if elapsed < 60:
            time.sleep(60 - elapsed)

    def get_report(self, date_from: str, date_to: str, limit: int = 100000) -> List[Dict]:
        """Fetch report with automatic pagination"""
        results = []
        rrdid = 0

        while True:
            self._wait_if_needed()

            params = {
                "dateFrom": date_from,
                "dateTo": date_to,
                "limit": min(limit, 100000),
                "rrdid": rrdid
            }

            response = requests.get(
                self.base_url,
                headers={"Authorization": self.api_key},
                params=params
            )

            self.last_request = time.time()

            if not response.ok:
                print(f"Error for {self.project_name}: {response.status_code}")
                break

            data = response.json()
            if not data:
                break

            results.extend(data)
            rrdid = data[-1].get("rrd_id", 0)

            if len(data) < limit:
                break

        return results

def process_project(api_key: str, project_name: str, date_from: str, date_to: str) -> pd.DataFrame:
    """Process data for a single project"""
    api = WildberriesAPI(api_key=api_key, project_name=project_name)
    report = api.get_report(date_from=date_from, date_to=date_to)
    
    print(f"Got {len(report)} records for {project_name}")
    if report:
        print(f"First record for {project_name}:", report[0])

    df = pd.DataFrame(report)
    
    # Basic data cleaning
    df = df.applymap(lambda x: None if x == '' else x)
    
    # Add project identifier
    df['project'] = project_name
    df['load_dt'] = pd.Timestamp.now()
    df['source'] = "WB-Realization-API"
    
    return df

def main():
    today = date.today()
    previous_monday = date(2024, 12, 1)
    previous_sunday = date(2025, 1, 4)

    # Process all projects
    projects = [
        ("WB-GutenTech-72684", KeyGuten),
        ("WB-ГиперМаркет-249999596", KeyGiper),
        ("WB-KitchenTrade-189728", KeyKitchen),
        ("WB-Smart Market-4002353", KeySmart)
    ]

    dfs = []
    for project_name, api_key in projects:
        try:
            df = process_project(api_key, project_name, previous_monday, previous_sunday)
            dfs.append(df)
        except Exception as e:
            print(f"Error processing {project_name}: {str(e)}")
            continue

    if not dfs:
        print("No data was retrieved for any project")
        return

    # Combine all DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)

    # Ensure the DataFrame has the correct columns
    columns = [
        'realizationreport_id', 'date_from', 'date_to', 'create_dt', 'currency_name',
        'suppliercontract_code', 'rrd_id', 'gi_id', 'dlv_prc', 'fix_tariff_date_from',
        'fix_tariff_date_to', 'subject_name', 'nm_id', 'brand_name', 'sa_name', 'ts_name',
        'barcode', 'doc_type_name', 'quantity', 'retail_price', 'retail_amount',
        'sale_percent', 'commission_percent', 'office_name', 'supplier_oper_name',
        'order_dt', 'sale_dt', 'rr_dt', 'shk_id', 'retail_price_withdisc_rub',
        'delivery_amount', 'return_amount', 'delivery_rub', 'gi_box_type_name',
        'product_discount_for_report', 'supplier_promo', 'rid', 'ppvz_spp_prc',
        'ppvz_kvw_prc_base', 'ppvz_kvw_prc', 'sup_rating_prc_up', 'is_kgvp_v2',
        'ppvz_sales_commission', 'ppvz_for_pay', 'ppvz_reward', 'acquiring_fee',
        'acquiring_percent', 'payment_processing', 'acquiring_bank', 'ppvz_vw',
        'ppvz_vw_nds', 'ppvz_office_name', 'ppvz_office_id', 'ppvz_supplier_id',
        'ppvz_supplier_name', 'ppvz_inn', 'declaration_number', 'bonus_type_name',
        'sticker_id', 'site_country', 'srv_dbs', 'penalty', 'additional_payment',
        'rebill_logistic_cost', 'storage_fee', 'deduction', 'acceptance', 'assembly_id',
        'srid', 'report_type', 'is_legal_entity', 'trbx_id', 'rebill_logistic_org',
        'load_dt', 'source', 'project'
    ]

    # Convert date columns to datetime and handle NaT values
    date_cols = ['date_from', 'date_to', 'create_dt', 'fix_tariff_date_from',
                 'fix_tariff_date_to', 'order_dt', 'sale_dt', 'rr_dt', 'load_dt']
    for col in date_cols:
        if col in combined_df.columns:
            combined_df[col] = pd.to_datetime(combined_df[col], errors='coerce').dt.tz_localize(None)
            combined_df[col] = combined_df[col].where(pd.notnull(combined_df[col]), None)

    # Convert boolean columns to integers
    if 'srv_dbs' in combined_df.columns:
        combined_df['srv_dbs'] = combined_df['srv_dbs'].astype(int)
    if 'is_legal_entity' in combined_df.columns:
        combined_df['is_legal_entity'] = combined_df['is_legal_entity'].astype(int)

    # Handle nullable string columns
    nullable_cols = [
        'project','suppliercontract_code', 'fix_tariff_date_from', 'fix_tariff_date_to',
        'subject_name', 'brand_name', 'sa_name', 'ts_name', 'barcode', 'doc_type_name',
        'office_name', 'gi_box_type_name', 'payment_processing', 'acquiring_bank',
        'ppvz_office_name', 'ppvz_supplier_name', 'ppvz_inn', 'declaration_number',
        'bonus_type_name', 'site_country', 'srid', 'trbx_id', 'rebill_logistic_org'
    ]

    for col in nullable_cols:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].replace({np.nan: None, '': None})

    # Reorder columns to match the expected order (add missing columns with None)
    for col in columns:
        if col not in combined_df.columns:
            combined_df[col] = None

    data_organized = combined_df[columns]   
    # Convert DataFrame to a list of tuples for bulk insertion
    data = [tuple(row) for row in data_organized.to_numpy()]

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

    # Insert data
    table_name = 'wb_finance'
    client.insert(table_name, data, column_names=columns)
    logging.info("Data inserted successfully!")
    print(f"Total records inserted: {len(data)}")

if __name__ == "__main__":
    main()