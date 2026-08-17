import pendulum
from airflow import DAG
from airflow.sdk import task

with DAG(
    dag_id="exemplo_xcom_1",
    description="Primeiro exemplo de xcom",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["xcom"]
) as dag:
    
    @task
    def task_write():
        return {"valorxcom1":10000}
    
    @task 
    def task_read(payload : dict):

        print(f"Valor de retorno xcom: {payload['valorxcom1']}")

    task_read(task_write())
