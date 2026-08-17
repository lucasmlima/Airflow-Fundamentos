import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import get_current_context
from airflow.providers.http.operators.http import HttpOperator

with DAG(
    dag_id="sensor",
    description="sensor",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    tags=["sensor"]
) as dag:

    ready = HttpSensor(
        task_id="ready",
        http_conn_id="openmeteo",
        endpoint="v1/forecast?latitude=23.55&longitude=-46.63&hourly=temperature_2m",
        poke_interval=5,
        timeout=300,
        deferrable=True,
    )

    fetch = HttpOperator(
        task_id="fetch",
        http_conn_id="openmeteo",
        endpoint="v1/forecast?latitude=23.55&longitude=-46.63&hourly=temperature_2m",
        method="GET",
        response_filter=lambda r: r.json(),
        log_response=True,
        deferrable=True,
    )

    def process_weather():

        ti = get_current_context()["ti"]
        data = ti.xcom_pull(task_ids='fetch')
        hourly=data.get('hourly',{})
        times=hourly.get("time",[])[:5]
        temps=hourly.get("temperature_2m",[])[:5]

        print("Tempraturas:")
        for t,v in zip(times,temps):
            print(f"{t}:{v}")

    process = PythonOperator(
        task_id="process",
        python_callable=process_weather,
    )

    ready >> fetch >> process