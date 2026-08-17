import pendulum
import random
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, get_current_context
from airflow.providers.standard.operators.python import BranchPythonOperator


with DAG(
    dag_id="branchtest",
    description="teste de branch",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["branch"]
) as dag:

    def gera_numero_aleatorio():
        return random.randint(1,6)

    gera_numero_aleatorio_task = PythonOperator(
        task_id='gera_numero_aleatorio_task',
        python_callable=gera_numero_aleatorio
    )

    def avalia_numero_aleatorio():
        ctx = get_current_context()
        numero = ctx['ti'].xcom_pull(task_ids='gera_numero_aleatorio_task')
        return 'par_task' if numero % 2 == 0 else 'impar_task'

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=avalia_numero_aleatorio
    )

    par_task = BashOperator(
        task_id='par_task',
        bash_command='echo "Número Par"'
    )
    impar_task = BashOperator(
        task_id='impar_task',
        bash_command='echo "Número impar"'
    )

    gera_numero_aleatorio_task >> branch_task >> [par_task, impar_task]