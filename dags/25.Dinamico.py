import pendulum
from airflow import DAG
from airflow.decorators import task

ITENS=['sp','rj','bh','rs']

with DAG(
    dag_id="dinamico",
    description="dinamico",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["dinamico"]
) as dag:

    @task
    def baixar(nome : str) -> str:
        print(f'Baixando: {nome}...')
        return nome

    @task 
    def processar(nome : str) -> str:
        print(f"Processando: {nome}...")
        return f'ok: {nome}'

    @task
    def consolidar(resultados : list[str]) -> str:
        print('Consolidado', resultados)
        return ''


    baixados = baixar.expand(nome=ITENS)
    processados = processar.expand(nome=baixados)
    consolidar(processados)
