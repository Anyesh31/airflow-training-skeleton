import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

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


pgsl_to_gcs= PostgresToGoogleCloudStorageOperator
(
    task_id="postgres_to_gcs",
    dag=dag,
    sql="select * from land_registry_price_paid_uk where trnasfer_date='{{ds}}'",
    bucket="airflow-training-knab-asv",
    filename="land_registry_price_paid_uk/{{ds}}/properties_{}.json",
    postgre_conn_id="airflow-training-postgres"
    )
)
