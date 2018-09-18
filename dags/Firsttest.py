import datetime as dt

from airflow import DAG
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from http_gcs_operator import HttpToGcsOperator

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

for currency in {'EUR', 'USD'}:
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        method="GET",
        endpoint="airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=" + currency,
        http_conn_id="airflow-training-currency-http",
        gcs_conn_id="airflow-training-storage-bucket",
        gcs_path="currency/{{ ds }}-" + currency + ".json",
        dag=dag,
    )

