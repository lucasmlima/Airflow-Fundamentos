import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="dagrundag1",
    description="Primeira dag do dag run",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["dagrun"]
) as dag:
    
    task1 = BashOperator(task_id = 'tsk1', bash_command='sleep 5')
    task2 = TriggerDagRunOperator(
        task_id='tsk2',
        trigger_dag_id='dagrundag2',
        conf={"Chave":"Airflow is cool"},
        wait_for_completion=True,
        poke_interval=5
    )

    task1 >> task2
