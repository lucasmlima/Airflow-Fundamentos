import pendulum
import random
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, get_current_context
from airflow.providers.standard.operators.python import ShortCircuitOperator


with DAG(
    dag_id="shortcircuit",
    description="teste de shortcircuit",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["shortcircuit"]
) as dag:

    def gera_qualidade() -> int:
        return random.randint(1,100)

    gera_qualidade = PythonOperator(
        task_id="gera_qualidade",
        python_callable=gera_qualidade,
    )

    def qualidade_suficiente() -> bool:
        ctx = get_current_context()
        ti = ctx['ti']
        qualidade = ti.xcom_pull(task_ids='gera_qualidade')
        return int(qualidade) >= 70

    shortcircuit = ShortCircuitOperator(
        task_id='shortcircuit',
        python_callable=qualidade_suficiente
    )

    processa = BashOperator(
        task_id='processa',
        bash_command='echo "Processador por que qualidade boa"'
    )

    finaliza = BashOperator(
        task_id='finaliza',
        bash_command='echo "Finalizado porque qualidade boa"'
    )

    gera_qualidade >> shortcircuit >> processa >> finaliza