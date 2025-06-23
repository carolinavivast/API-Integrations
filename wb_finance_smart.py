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
KeySmart = os.getenv("KeySmart")
password = os.getenv('ClickHouse')

class WildberriesAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
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
                print(f"Error: {response.status_code}")
                break

            data = response.json()
            if not data:
                break

            results.extend(data)
            rrdid = data[-1].get("rrd_id", 0)

            if len(data) < limit:
                break

        return results

def main():
    # Initialize the API client
    api = WildberriesAPI(api_key=KeySmart)
    today = date.today()
    previous_monday = today - timedelta(days=today.weekday() + 7)
    # Calculate the Sunday of the previous week
    previous_sunday = previous_monday + timedelta(days=6)

    # Fetch the report
    report = api.get_report(
        date_from= previous_monday,
        date_to=previous_sunday
    )

    print(f"Got {len(report)} records")
    
    if report:
        print("First record:", report[0])

    # Convert to pandas DataFrame
    df = pd.DataFrame(report)

    # Basic data cleaning
    df = df.applymap(lambda x: None if x == '' else x)  # Replace empty strings with None

    # Add missing columns with default values
    if 'load_dt' not in df.columns:
        df['load_dt'] = pd.Timestamp.now()
    if 'source' not in df.columns:
        df['source'] = "WB-Realization-API"
    
    print(df)

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
        'load_dt', 'source'
    ]

    # Convert date columns to datetime and handle NaT values
    date_cols = ['date_from', 'date_to', 'create_dt', 'fix_tariff_date_from',
                 'fix_tariff_date_to', 'order_dt', 'sale_dt', 'rr_dt', 'load_dt']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.tz_localize(None)
            df[col] = df[col].where(pd.notnull(df[col]), None)  # Replace NaT with None

    # Convert boolean columns to integers
    df['srv_dbs'] = df['srv_dbs'].astype(int)
    df['is_legal_entity'] = df['is_legal_entity'].astype(int)

    # Handle nullable string columns
    nullable_cols = [
        'suppliercontract_code', 'fix_tariff_date_from', 'fix_tariff_date_to',
        'subject_name', 'brand_name', 'sa_name', 'ts_name', 'barcode', 'doc_type_name',
        'office_name', 'gi_box_type_name', 'payment_processing', 'acquiring_bank',
        'ppvz_office_name', 'ppvz_supplier_name', 'ppvz_inn', 'declaration_number',
        'bonus_type_name', 'site_country', 'srid', 'trbx_id', 'rebill_logistic_org'
    ]

    for col in nullable_cols:
        df[col] = df[col].replace({np.nan: None, '': None})

    # Reorder columns to match the expected order
    data_organized = df[columns]

    # Convert DataFrame to a list of tuples for bulk insertion
    data = [tuple(row) for row in data_organized.to_numpy()]

    # Define the table name
    table_name = 'wb_realization_reports'

    # Define connection parameters
    client = get_client(
        host='rc1a-j5ou9lq30ldal602.mdb.yandexcloud.net',  # Your Yandex Cloud ClickHouse host
        port=8443,                                          # Yandex Cloud uses port 8443 for HTTPS
        username='user1',                                   # Your ClickHouse username
        password=password,                                  # Your ClickHouse password
        database='user1',                                   # Your database name
        secure=True,                                        # Use HTTPS
        verify=False                                        # Disable SSL certificate verification
    )

    # Use the insert method for bulk insertion
    client.insert(table_name, data, column_names=columns)
    logging.info("Data inserted successfully!")

if __name__ == "__main__":
    main()