import pendulum
from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd

mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

with DAG(
    dag_id="consumer",
    description="consumer",
    schedule=[mydataset],
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["consumer"]
) as dag:

    def create_dataset_file():

        dataset = pd.read_csv("/opt/airflow/data/Churn_new.csv", sep=";")
        dataset.to_csv("/opt/airflow/data/Churn_new2.csv", sep=";", index=False)

    t1 = PythonOperator(
        task_id="t1",
        python_callable=create_dataset_file,
    )

    t1