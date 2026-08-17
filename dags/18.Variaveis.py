import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable


with DAG(
    dag_id="Variaveis",
    description="Importar Variáveis",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["variavel"]
) as dag:

    def print_varible():
        minhar_var = Variable.get('minhavar')
        print(f'O valor da variável é: {minhar_var}')
    
    task1 = PythonOperator(task_id = 'task1', python_callable=print_varible)