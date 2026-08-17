import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="primeira_dag",
    description="Nossa primeira dag",
    schedule=None,
    start_date=pendulum.datetime(2025,6,10,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Curso Exemplo"]
)as dag:
    
    task1=BashOperator(task_id="tsk1",bash_command="sleep 5")
    task2=BashOperator(task_id="tsk2",bash_command="sleep 5")
    task3=BashOperator(task_id="tsk3",bash_command="sleep 5")

    task1 >> task2 >> task3