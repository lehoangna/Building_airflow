import datetime
import tempfile
import pandas as pd
import json
from google.cloud import storage
import pymongo

from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time()
)

graphic_cards_schema = [
    {"mode": "REQUIRED", "name": "index", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "title", "type": "STRING"},
    {"mode": "NULLABLE", "name": "brand", "type": "STRING"},
    {"mode": "NULLABLE", "name": "rating", "type": "STRING"},
    {"mode": "NULLABLE", "name": "price", "type": "STRING"},
    {"mode": "NULLABLE", "name": "shipping", "type": "STRING"},
    {"mode": "NULLABLE", "name": "imgUrl", "type": "STRING"},
    {"mode": "NULLABLE", "name": "rating_num", "type": "STRING"},
]

vnstock_schema = [
    {"mode": "NULLABLE", "name": "Open", "type": "FLOAT"},
    {"mode": "NULLABLE", "name": "High", "type": "FLOAT"},
    {"mode": "NULLABLE", "name": "Low", "type": "FLOAT"},
    {"mode": "NULLABLE", "name": "Close", "type": "FLOAT"},
    {"mode": "NULLABLE", "name": "Volume", "type": "FLOAT"},
    {"mode": "NULLABLE", "name": "TradingDate", "type": "TIMESTAMP"},
    {"mode": "NULLABLE", "name": "Ticker", "type": "STRING"},
]

tiki_schema = [
    {"mode": "NULLABLE", "name": "id", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "master_id", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "sku", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "name", "type": "STRING"},
    {"mode": "NULLABLE", "name": "url_key", "type": "STRING"},
    {"mode": "NULLABLE", "name": "url_path", "type": "STRING"},
    {"mode": "NULLABLE", "name": "short_url", "type": "STRING"},
    {"mode": "NULLABLE", "name": "type", "type": "STRING"},
    {"mode": "NULLABLE", "name": "book_cover", "type": "STRING"},
    {"mode": "NULLABLE", "name": "short_description", "type": "STRING"},
    {"mode": "NULLABLE", "name": "price", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "list_price", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "original_price", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "badges", "type": "STRING"},
    {"mode": "NULLABLE", "name": "badges_news", "type": "STRING"},
    {"mode": "NULLABLE", "name": "discount", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "discount_rate", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "rating_average", "type": "FLOAT"},
    {"mode": "NULLABLE", "name": "review_count", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "review_text", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "favourite_count", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "thumbnail_url", "type": "STRING"},
    {"mode": "NULLABLE", "name": "has_ebook", "type": "BOOLEAN"},
    {"mode": "NULLABLE", "name": "inventory_status", "type": "STRING"},
    {"mode": "NULLABLE", "name": "inventory_type", "type": "STRING"},
    {"mode": "NULLABLE", "name": "productset_group_name", "type": "STRING"},
    {"mode": "NULLABLE", "name": "is_fresh", "type": "BOOLEAN"},
    {"mode": "NULLABLE", "name": "seller", "type": "STRING"},
    {"mode": "NULLABLE", "name": "is_flower", "type": "BOOLEAN"},
    {"mode": "NULLABLE", "name": "has_buynow", "type": "BOOLEAN"},
    {"mode": "NULLABLE", "name": "is_gift_card", "type": "BOOLEAN"},
    {"mode": "NULLABLE", "name": "salable_type", "type": "STRING"},
    {"mode": "NULLABLE", "name": "data_version", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "day_ago_created", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "all_time_quantity_sold", "type": "INTEGER"},
    {"mode": "NULLABLE", "name": "meta_title", "type": "STRING"},
    {"mode": "NULLABLE", "name": "meta_description", "type": "STRING"},
    {"mode": "NULLABLE", "name": "meta_keywords", "type": "STRING"},
    {"mode": "NULLABLE", "name": "is_baby_milk", "type": "BOOLEAN"},
    {"mode": "NULLABLE", "name": "is_acoholic_drink", "type": "BOOLEAN"},
    {"mode": "NULLABLE", "name": "images", "type": "STRING"},
    {"mode": "NULLABLE", "name": "warranty_policy", "type": "STRING"},
    {"mode": "NULLABLE", "name": "brand", "type": "STRING"},
    {"mode": "NULLABLE", "name": "current_seller", "type": "STRING"},
    {"mode": "NULLABLE", "name": "other_sellers", "type": "STRING"},
    {"mode": "NULLABLE", "name": "seller_specifications", "type": "STRING"},
    {"mode": "NULLABLE", "name": "specifications", "type": "STRING"},
    {"mode": "NULLABLE", "name": "products_links", "type": "STRING"},
    {"mode": "NULLABLE", "name": "services_and_promotions", "type": "STRING"},
    {"mode": "NULLABLE", "name": "promitions", "type": "STRING"},
    {"mode": "NULLABLE", "name": "stock_item", "type": "STRING"},
    {"mode": "NULLABLE", "name": "add_on_title", "type": "STRING"},
    {"mode": "NULLABLE", "name": "add_on", "type": "STRING"},
    {"mode": "NULLABLE", "name": "quantity_sold", "type": "STRING"},
    {"mode": "NULLABLE", "name": "categories", "type": "STRING"},
    {"mode": "NULLABLE", "name": "breadcrumbs", "type": "STRING"},
    {"mode": "NULLABLE", "name": "installment_info", "type": "STRING"},
    {"mode": "NULLABLE", "name": "installment_info_v2", "type": "STRING"},
    {"mode": "NULLABLE", "name": "is_seller_in_chat_whitelist", "type": "BOOLEAN"},
    {"mode": "NULLABLE", "name": "inventory", "type": "STRING"},
    {"mode": "NULLABLE", "name": "warranty_info", "type": "STRING"},
    {"mode": "NULLABLE", "name": "return_and_exchange_policy", "type": "STRING"},
    {"mode": "NULLABLE", "name": "is_tier_pricing_available", "type": "BOOLEAN"},
    {"mode": "NULLABLE", "name": "is_tier_pricing_eligible", "type": "BOOLEAN"},
    {"mode": "NULLABLE", "name": "asa_cashback_widget", "type": "STRING"},
    {"mode": "NULLABLE", "name": "benefits", "type": "STRING"},
    {"mode": "NULLABLE", "name": "video_url", "type": "STRING"},
    {"mode": "NULLABLE", "name": "best_price_guaranteed", "type": "BOOLEANss"},
    {"mode": "NULLABLE", "name": "price_comparison", "type": "STRING"},
    {"mode": "NULLABLE", "name": "deal_specs", "type": "STRING"},
]

default_args = {
    'start_date': yesterday,
    'email': ['lehoang2kna@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

with models.DAG(
    'extract_data',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    def from_mongodb_to_gcs():
        my_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = my_client["tiki_products"]
        data_products = list(db.products.find({}, {'_id': 0, 'description': 0}))
        data_products = pd.DataFrame(data_products)
        client = storage.Client.from_service_account_json('/home/lehoang/Downloads/buildingairflow-743dcc3a66cc.json')
        bucket = client.get_bucket('lehoang2kna-pro6-data')
        bucket.blob('tiki_products.csv').upload_from_string(data_products.to_csv(index=False), 'text/csv')

    mongodb_to_gcs = PythonOperator(
        task_id='mongodb_to_gcs',
        python_callable=from_mongodb_to_gcs,
        execution_timeout=datetime.timedelta(minutes=15)
    )

    # mysql_to_gcs = MySQLToGCSOperator(
    #     task_id='mysql_to_gcs',
    #     mysql_conn_id='my_mysql_conn',
    #     sql='SELECT title, brand, rating, price, shipping, imgUrl, rating_num FROM graphic_card.information',
    #     bucket='lehoang2kna-pro6-data',
    #     filename='graphic_cards.csv',
    #     schema=graphic_cards_schema,
    #     export_format='csv',
    #     gcp_conn_id='my_gcs_conn',
    # )
    #
    # local_file_to_gcs = FileToGoogleCloudStorageOperator(
    #     task_id='local_file_to_gcs',
    #     src='/home/lehoang/Downloads/upload_vnstock_data.csv',
    #     dst='upload_vnstock_data.csv',
    #     bucket='lehoang2kna-pro6-data',
    #     gcp_conn_id="my_gcs_conn",
    #     mime_type='text/csv'
    # )

    create_tiki_table = BigQueryCreateEmptyTableOperator(
        task_id='create_tiki_table',
        dataset_id='data',
        table_id='tiki_products',
        schema_fields=tiki_schema,
        gcp_conn_id='my_gcs_conn',
    )

    move_tiki = GCSToBigQueryOperator(
        task_id='move_tiki',
        bucket='lehoang2kna-pro6-data',
        source_objects=['tiki_products.csv'],
        destination_project_dataset_table='buildingairflow.data.tiki_products',
        schema_fields=tiki_schema,
        gcp_conn_id='my_gcs_conn',
        allow_quoted_newlines=True,
        write_disposition='WRITE_TRUNCATE',
    )

    # mysql_to_gcs >> mongodb_to_gcs >> local_file_to_gcs
    mongodb_to_gcs >> create_tiki_table >> move_tiki