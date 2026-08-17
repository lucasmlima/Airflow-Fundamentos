import pendulum
from airflow import DAG
from big_data_operator import BigDataOperator

with DAG(
    dag_id="bigdata",
    description="bigdata",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["bigdata"]
) as dag:
    big_data = BigDataOperator(
        task_id="bigdata",
        path_to_csv_file="/opt/airflow/data/Churn.csv",
        path_to_save_file="/opt/airflow/data/Churn.parquet",
        file_type="parquet",
    )

    big_data
