import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="dagrundag2",
    description="Segunda dag do dag run",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["dagrun"]
) as dag:
    
    task1 = BashOperator(task_id = 'tsk1', bash_command='echo "{{ dag_run.conf["Chave"]}}"',)
    # Utiliza Função Jinja, que é uma função de marcação de template, para pegar o retorno da chamda
    # da dag
    task2 = BashOperator(task_id = 'tsk2', bash_command='sleep 5')

    task1 >> task2
