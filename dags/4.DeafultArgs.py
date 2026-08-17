import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

default_args = {
    "depends_on_past":False,
    "email":"teste@gmail.com",
    "email_on_failure":False,
    "email_on_retry":False,
    "retries":1,
    "retry_delay":timedelta(seconds=10),
}

with DAG(
    dag_id="dafaultargs",
    description="Dag de teste default args",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["args","default"],
    default_args=default_args,
) as dag:
    
    task1=BashOperator(task_id="task1",bash_command="exit 1", retries=3)
    task2=BashOperator(task_id="task2",bash_command="sleep 5")
    task3=BashOperator(task_id="task3",bash_command="sleep 5")

    task1 >> task2 >> task3