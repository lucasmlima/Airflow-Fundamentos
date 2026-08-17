import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="segunda_dag",
    description="Minha segunda dag, com dependencia",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["Segunda DAG"]
) as dag:
    
    task1=BashOperator(task_id="task1",bash_command="sleep 5")
    task2=BashOperator(task_id="task2",bash_command="sleep 5")
    task3=BashOperator(task_id="task3",bash_command="sleep 5")

    task1 >> [task2, task3]