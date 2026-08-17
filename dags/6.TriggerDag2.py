import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta

default_args= {
    'retries':1,
    'retry_delay':timedelta(seconds=10),
}

with DAG(
    dag_id="triggerdag2",
    description="Trigger DAG 2",
    schedule=None,
    start_date=pendulum.datetime(2025,6,10,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["Curso Exemplo"],
    default_args=default_args
)as dag:
    
    task1=BashOperator(task_id="tsk1",bash_command="exit 1")
    task2=BashOperator(task_id="tsk2",bash_command="sleep 5")
    task3=BashOperator(task_id="tsk3",bash_command="sleep 5", trigger_rule=TriggerRule.ONE_FAILED)

    [task1 , task2 ] >> task3