import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="pools",
    description="teste de pools",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["pools"]
) as dag:

    task_leve = BashOperator(
        task_id="task_leve",
        bash_command="sleep 5",
        pool='meupool',
        priority_weight=1,
        weight_rule="absolute"
    )

    task_media = BashOperator(
            task_id="task_media",
            bash_command="sleep 5",
            pool='meupool',
            priority_weight=5,
            weight_rule="absolute"
        )

    task_pesada = BashOperator(
            task_id="task_pesada",
            bash_command="sleep 5",
            pool='meupool',
            priority_weight=10,
            pool_slots=2,
            weight_rule="absolute"
        )