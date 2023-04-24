import datetime
import tempfile
import pandas as pd
import json
from google.cloud import storage
from google.cloud import bigquery
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
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.email import send_email

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

def send_email_on_failure(context):
    title = f"DAG Failed: {context.get('dag_run').dag_id}"
    body = f"Task: {context.get('task_instance').task_id}\n\nLog:\n{context.get('task_instance').log_url}"
    send_email(to=['lehoang2kna@gmail.com'], subject=title, html_content=body)

default_args = {
    'start_date': yesterday,
    'email': ['lehoang2kna@gmail.com'],
    'email_on_failure': True,
    'email_subject': 'My DAG Failed!',
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'on_failure_callback': send_email_on_failure
}

with models.DAG(
    'extract_data',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:
    mysql_to_gcs = MySQLToGCSOperator(
        task_id='mysql_to_gcs',
        mysql_conn_id='my_mysql_conn',
        sql='SELECT title, brand, rating, price, shipping, imgUrl, rating_num FROM graphic_card.information',
        bucket='lehoang2kna-pro6-data',
        filename='graphic_cards.csv',
        schema=graphic_cards_schema,
        export_format='csv',
        gcp_conn_id='my_gcs_conn',
    )

    def from_mongodb_to_gcs():
        my_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = my_client["tiki_products"]
        with open("tiki_products.json", "w", encoding='utf-8') as f:
            for document in db.products.find({}, {'_id': 0}):
                json.dump(document, f, ensure_ascii=False)
                f.write("\n")
        client = storage.Client.from_service_account_json('/home/lehoang/Downloads/buildingairflow-743dcc3a66cc.json')
        bucket = client.get_bucket('lehoang2kna-pro6-data')
        bucket.blob('tiki_products.json').upload_from_filename('tiki_products.json', 'application/json')

    mongodb_to_gcs = PythonOperator(
        task_id='mongodb_to_gcs',
        python_callable=from_mongodb_to_gcs,
        execution_timeout=datetime.timedelta(minutes=15)
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id='data',
        gcp_conn_id='my_gcs_conn',
    )

    create_graphic_card_table = BigQueryCreateEmptyTableOperator(
        task_id='create_graphic_card_table',
        dataset_id='data',
        table_id='graphic_cards',
        schema_fields=graphic_cards_schema,
        gcp_conn_id='my_gcs_conn',
    )

    move_graphic_card = GCSToBigQueryOperator(
        task_id='move_graphic_card',
        bucket='lehoang2kna-pro6-data',
        source_objects=['graphic_cards.csv'],
        destination_project_dataset_table='buildingairflow.data.graphic_cards',
        schema_fields=graphic_cards_schema,
        gcp_conn_id='my_gcs_conn',
        allow_quoted_newlines=True,
        write_disposition='WRITE_TRUNCATE',
    )
    def create_table_and_load_data_tiki():
        bq_client = bigquery.Client.from_service_account_json('/home/lehoang/Downloads/buildingairflow-743dcc3a66cc.json')
        dataset_ref = bq_client.dataset('data')

        # Create table in BigQuery
        table_ref = dataset_ref.table('tiki_products')
        table = bigquery.Table(table_ref)
        bq_client.create_table(table)

        # Load data from JSON file in GCS to BigQuery table with autodetected schema
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.autodetect = True
        job_config.max_bad_records = 100
        job_config.ignore_unknown_values = True
        uri = 'gs://lehoang2kna-pro6-data/tiki_products.json'
        load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()


    create_table_and_load_tiki = PythonOperator(
        task_id='create_table_and_load_tiki',
        python_callable=create_table_and_load_data_tiki,
    )

    mysql_to_gcs >> mongodb_to_gcs >> create_dataset >> create_graphic_card_table >> move_graphic_card >> create_table_and_load_tiki