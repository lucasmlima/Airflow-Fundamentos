import pendulum
from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd

mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

with DAG(
    dag_id="producer",
    description="producer",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["dataset"]
) as dag:

    def create_dataset_file():

        dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")
        dataset.to_csv("/opt/airflow/data/Churn_new.csv", sep=";", index=False)

    t1 = PythonOperator(
        task_id="t1",
        python_callable=create_dataset_file,
        outlets=[mydataset],
    )

    t1