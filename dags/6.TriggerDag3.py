import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="triggerdag3",
    description="Trigger DAG 3",
    schedule=None,
    start_date=pendulum.datetime(2025,6,10,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Curso Exemplo"]
)as dag:
    
    task1=BashOperator(task_id="tsk1",bash_command="exit 1")
    task2=BashOperator(task_id="tsk2",bash_command="exit 1")
    task3=BashOperator(task_id="tsk3",bash_command="sleep 5", trigger_rule=TriggerRule.ALL_FAILED)

    [task1 , task2 ] >> task3