from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator
)

from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from dotenv import load_dotenv
load_dotenv()


default_args = {
    'owner': 'Ritayan Patra',
    'start_date': days_ago(1),
    'retries': 1
}


with DAG(
    'walmart_sales_etl_gcs',
    default_args = default_args,
    description = 'Extract-Transform-Load pipeline for Walmart Merchant Sales Data from GCS to BigQuery',
    schedule_interval = '@daily',
    catcup=False
) as dag:
    
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_datawarehouse',
        dataset_id='walmart_dwh',
        location='US'
    )

    create_merchants_table = BigQueryCreateEmptyTableOperator(
        task_id = 'create_merchants_table',
        dataset_id = 'walmart_dwh',
        table_id = 'merchants_data',
        schema_fields = [
            {"name": "merchant_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "merchant_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "merchant_category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "merchant_country", "type" : "STRING", "mode": "NULLABLE"},
            {"name": "last_update", "type":"TIMESTAMP", "mode": "NULLABLE"}
        ]
    )

    create_sales_table = BigQueryCreateEmptyTableOperator(
        task_id = 'create_sales_table',
        dataset_id = 'walmart_dwh',
        table_id = 'sales_data',
        schema_fields = [
            {"name":"sale_id", "type": "STRING", "mode": "REQUIRED"},
            {"name":"sale_date", "type":"DATE", "mode":"NULLABLE"},
            {"name":"product_id", "type":"STRING", "mode":"NULLABLE"},
            {"name":"quantity_sold", "type":"INT64", "mode":"NULLABLE"},
            {"name":"total_sale_amount", "type":"FLOAT64", "mode":"NULLABLE"},
            {"name":"merchant_id", "type":"STRING", "mode":"NULLABLE"},
            {"name":"last_update", "type":"TIMESTAMP", "mode":"NULLABLE"}
        ]
    )

    create_target_table = BigQueryCreateEmptyTableOperator(
        task_id = "create_target_table",
        dataset_id = "walmart_dwh",
        table_id = "target_data",
        schema_fields = [
            {"name": "sale_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "sale_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "quantity_sold", "type": "INT64", "mode": "NULLABLE"},
            {"name": "total_sale_amount", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "merchant_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "merchant_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "merchant_category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "merchant_country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
    )

    with TaskGroup("load_data_from_gcs_to_bq") as load_data:

        load_merchants_data = GCSToBigQueryOperator(
            task_id = "gcs_to_bq_merchants",
            bucket = 'bigquery_ritayan',
            source_objects = ['walmart/merchants/merchants_*.json'],
            destination_project_dataset_table = 'walmart_dwh.merchants_data',
            source_format = 'NEWLINE_DELIMITED_JSON',       # Each JSON object is on a separate line
            write_disposition = 'WRITE_TRUNCATE'    # Overwrite table if it exists
        )

        load_sales_data = GCSToBigQueryOperator(
            task_id = "gcs_to_bq_sales",
            bucket = "bigquery_ritayan",
            source_objects = ['walmart/sales/sales_*.json'],
            destination_project_dataset_table = 'walmart_dwh.sales_data',
            source_format = 'NEWLINE_DELIMITED_JSON',       # Each JSON object is on a separate line
            write_disposition = 'WRITE_TRUNCATE'    # Overwrite table if it exists
        )


    
    merge_data = BigQueryExecuteQueryOperator(
        task_id='merge_merchant_and_sales_data',
        sql="""

        MERGE `computer-systems-ritayan-patra.walmart_dwh.target_data` T
        USING (
            SELECT
                s.sale_id,
                s.sale_data,
                s.product_id,
                s.quantity_sold,
                s.total_sale_amount,
                s.merchant_id,
                m.merchant_name,
                m.merchant_category,
                m.merchant_country,
                CURRENT_TIMESTAMP() as last_update,
            FROM
                `computer-systems-ritayan-patra.walmart_dwh.sales_data` AS s
            LEFT JOIN
                `computer-systems-ritayan-patra.walmart_dwh.merchants_data` AS m
            ON
                s.merchant_id = m.merchant_id
            ) S
        ON T.sale_id = S.sale_id
        WHEN MATCHED THEN
            UPDATE SET
                T.sale_date = S.sale_date,
                T.product_id = S.product_id,
                T.quantity_sold = S.quantity_sold,
                T.total_sale_amount = S.total_sale_amount,
                T.merchant_id = S.merchant_id,
                T.merchant_name = S.merchant_name,
                T.merchant_category = S.merchant_category,
                T.merchant_country = S.merchant_country,
                T.last_update = S.last_update
        WHEN NOT MATCHED THEN
            INSERT(
                sale_id,
                sale_data,
                product_id,
                quantity_sold,
                total_sale_amount,
                merchant_id,
                merchant_name,
                merchant_category,
                merchant_country,
                last_update
            )
            VALUES (
                S.sale_id,
                S.sale_date,
                S.product_id,
                S.quantity_sold,
                S.total_sale_amount,
                S.merchant_id,
                S.merchant_name,
                S.merchant_category,
                S.merchant_country,
                S.last_update
            );
        """,
        use_legacy_sql=False,
    )


    create_dataset >> [create_merchants_table, create_sales_table, create_target_table] >> load_data
    load_data >> merge_data


    