import datetime as dt

from airflow import DAG
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from http_gcs_operator import HttpToGcsOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator
)
from airflow.utils.trigger_rule import TriggerRule
from godatadriven.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

PROJECT_ID = 'gdd-990fd90d0db6efbabdc6b70f1c'

BUCKET = 'airflow-training-knab-asv'

dag = DAG(
    dag_id="FirstScript",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow training",
        "start_date": dt.datetime(2018, 8, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    dag=dag,
    sql="select * from land_registry_price_paid_uk where transfer_date='{{ds}}'",
    bucket="airflow-training-knab-asv",
    filename="land_registry_price_paid_uk/{{ds}}/proerties_{}.json",
    postgres_conn_id="airflow-training-postgres"
)

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id=PROJECT_ID,
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

land_registry_prices_to_bigquery = DataFlowPythonOperator(
    task_id="land_registry_prices_to_bigquery",
    dataflow_default_options={
        "project": "gdd-990fd90d0db6efbabdc6b70f1c",
        "region": "europe-west1",
    },
    py_file="gs://airflow-training-knab-asv/dataflow_job.py",
    dag=dag,
)

for currency in {'EUR', 'USD'}:
    currency_task = HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint="airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=" + currency,
        http_conn_id="airflow-training-currency-http",
        gcs_conn_id="airflow-training-storage-bucket",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag,
    )
    currency_task >> dataproc_create_cluster

compute_aggregates = DataProcPySparkOperator(
    task_id='compute_aggregates',
    main='gs://airflow-training-knab-asv/build_statistics.py',
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=["{{ ds }}"],
    dag=dag,
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    dag=dag,
    project_id=PROJECT_ID,
    trigger_rule=TriggerRule.ALL_DONE,
)

pgsl_to_gcs >> dataproc_create_cluster

dataproc_create_cluster >> compute_aggregates

dataproc_create_cluster >> land_registry_prices_to_bigquery

compute_aggregates >> dataproc_delete_cluster


write_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id="write_to_bq",
    bucket=BUCKET,
    source_objects=["average_prices/transfer_date={{ ds }}/*"],
    destination_project_dataset_table="gdd-990fd90d0db6efbabdc6b70f1c:prices.land_registry_prices${{ ds_nodash }}",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

compute_aggregates >> write_to_bq