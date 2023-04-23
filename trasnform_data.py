import datetime
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time()
)

graphic_cards_schema = [
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

default_args = {
    'start_date': yesterday,
    'email': ['lehoang2kna@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
}

with models.DAG(
    'transform_data',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    # create_dataset = BigQueryCreateEmptyDatasetOperator(
    #     task_id='create_dataset',
    #     dataset_id='data',
    #     gcp_conn_id='my_gcs_conn',
    # )
    #
    # create_graphic_card_table = BigQueryCreateEmptyTableOperator(
    #     task_id='create_graphic_card_table',
    #     dataset_id='data',
    #     table_id='graphic_cards',
    #     schema_fields=graphic_cards_schema,
    #     gcp_conn_id='my_gcs_conn',
    # )

    create_tiki_table = BigQueryCreateEmptyTableOperator(
        task_id='create_tiki_table',
        dataset_id='data',
        table_id='test_nhe',
        schema_fields=schema,
        gcp_conn_id='my_gcs_conn',
    )

    # create_vnstock_table = BigQueryCreateEmptyTableOperator(
    #     task_id='create_vnstock_table',
    #     dataset_id='data',
    #     table_id='vnstock',
    #     schema_fields=vnstock_schema,
    #     gcp_conn_id='my_gcs_conn',
    # )

    # move_graphic_card = GCSToBigQueryOperator(
    #     task_id='move_graphic_card',
    #     bucket='lehoang2kna-pro6-data',
    #     source_objects=['graphic_cards.csv'],
    #     destination_project_dataset_table='buildingairflow.data.graphic_cards',
    #     schema_fields=graphic_cards_schema,
    #     gcp_conn_id='my_gcs_conn',
    #     allow_quoted_newlines=True,
    #     write_disposition='WRITE_TRUNCATE',
    # )

    move_tiki = GCSToBigQueryOperator(
        task_id='move_tiki',
        bucket='lehoang2kna-pro6-data',
        source_objects=['test.csv'],
        destination_project_dataset_table='buildingairflow.data.test_nhe',
        schema_fields=None,
        autodetect=True,
        gcp_conn_id='my_gcs_conn',
        allow_quoted_newlines=True,
        write_disposition='WRITE_TRUNCATE',
    )

    # move_vnstock = GCSToBigQueryOperator(
    #     task_id='move_vnstock',
    #     bucket='lehoang2kna-pro6-data',
    #     source_objects=['upload_vnstock_data.csv'],
    #     destination_project_dataset_table='buildingairflow.data.vnstock',
    #     schema_fields=vnstock_schema,
    #     gcp_conn_id='my_gcs_conn',
    #     allow_quoted_newlines=True,
    #     write_disposition='WRITE_TRUNCATE',
    # )
    # create_dataset >> create_graphic_card_table >> move_graphic_card
    create_tiki_table >> move_tiki