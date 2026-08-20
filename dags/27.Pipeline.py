import json
from pathlib import Path
from datetime import datetime
import pendulum
import pandas as pd
from airflow import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator, SQLColumnCheckOperator

POSTGRES_CONN_ID = 'pg_concessionaria'
EXPORT_DIR = Path("/opt/airflow/data")
WATERMARK_FILE = EXPORT_DIR / "_watermark_por_estado.json"

NORMALIZACAO_SQL_FILTRADO = '''
        WITH vendas_norm AS (
            SELECT
                v.id_vendas               AS id_venda,
                ve.nome                   AS veiculo,
                v.valor_pago              AS valor_pago,
                ve.valor                  AS valor_veiculo,
                (ve.valor - v.valor_pago) AS desconto,
                ROUND(
                    CASE WHEN ve.valor IS NULL OR ve.valor = 0 THEN 0
                        ELSE ((ve.valor - v.valor_pago) / ve.valor) * 100.0
                    END, 2
                )                         AS desconto_percentual,
                ven.nome                  AS vendedor,
                cons.concessionaria       AS concessionaria,
                v.data_venda              AS data_venda,
                est.sigla                 AS estado_sigla
            FROM vendas v
            JOIN veiculos ve           ON ve.id_veiculos = v.id_veiculos
            JOIN vendedores ven        ON ven.id_vendedores = v.id_vendedores
            JOIN concessionarias cons  ON cons.id_concessionarias = v.id_concessionarias
            JOIN cidades cid           ON cid.id_cidades = cons.id_cidades
            JOIN estados est           ON est.id_estados = cid.id_estados
            WHERE est.sigla = %s
            AND (%s IS NULL OR v.data_venda > %s)
        )
        SELECT *
        FROM vendas_norm
        ORDER BY data_venda, id_venda;
        '''

def _load_watermark() -> dict[str,str]:
    if WATERMARK_FILE.exists():
        return json.loads(WATERMARK_FILE.read_text())
    return {}

def _save_watermark(d: dict[str,str]) -> None:
    WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
    WATERMARK_FILE.write_text(json.dumps(d))


with DAG(
    dag_id="pipeline",
    description="pipeline de dados",
    schedule=None,
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["curso","exemplo"]
) as dag:

    chk_vendas_tem_dados = SQLTableCheckOperator(
        task_id='chk_vendas_tem_dados',
        conn_id = POSTGRES_CONN_ID,
        table='vendas',
        checks={"row_count_nonzero": {"check_statement": "COUNT(*)>0"}},
    )

    chk_colunas_vendas = SQLColumnCheckOperator(
        task_id='chk_colunas_vendas',
        conn_id = POSTGRES_CONN_ID,
        table='vendas',
        column_mapping={
            "id_vendas": {"null_check": {"equal_to": 0}},
            "valor_pago": {"null_check": {"equal_to": 0} , "min": {"geq_to":0}},
            "data_venda": {"null_check": {"equal_to": 0}},                        
        },
    )

    chk_integridade_por_join = SQLTableCheckOperator(
        task_id='chk_integridade_por_join',
        conn_id = POSTGRES_CONN_ID,
        table='vendas',
        checks={
            "vendas_com_join_completo":
            { "check_statement" : """
                EXISTS(
                    SELECT 1
                    FROM vendas v
                    JOIN veiculos ve           ON ve.id_veiculos = v.id_veiculos
                    JOIN vendedores ven        ON ven.id_vendedores = v.id_vendedores
                    JOIN concessionarias cons  ON cons.id_concessionarias = v.id_concessionarias
                    JOIN cidades cid           ON cid.id_cidades = cons.id_cidades
                    JOIN estados est           ON est.id_estados = cid.id_estados                
                ) 
            """
            },
        }
    )

    @task(task_id="exportar_incremental_por_estado")
    def exportar_incremental_por_estado() -> None:
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        (EXPORT_DIR / "vendas").mkdir(parents=True, exist_ok=True)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        ufs = [r[0] for r in hook.get_records("SELECT DISTINCT sigla FROM estados ORDER BY sigla")]

        watermark = _load_watermark()

        for uf in ufs:
            last_iso = watermark.get(uf)
            params = (uf, last_iso, last_iso)

            df = hook.get_pandas_df(NORMALIZACAO_SQL_FILTRADO, parameters = params)
            if df.empty:
                continue

            df["data_venda"] = pd.to_datetime(df["data_venda"],errors="coerce")
            df = df.dropna(subset=["data_venda"])
            df = df.sort_values(["data_venda","id_venda"]).drop_duplicates( subset=["id_venda"], keep="last")

            for (ano, mes), part in df.groupby([df["data_venda"].dt.year,df["data_venda"].dt.month]):
                dest_dir = (
                    EXPORT_DIR / 'vendas' / f"estado={uf.lower()}" / f"ano={ano:04d}" / f"mes={mes:02d}"
                )
                dest_dir.mkdir(parents=True, exist_ok=True)

                stamp =datetime.utcnow().strftime("%Y%m%d%H%M%S")
                fpath = dest_dir / f"lote_{stamp}.parquet"

                part = part.drop(columns=["estado_sigla"], errors="ignore")
                part.to_parquet(fpath, index=False, engine='pyarrow')
            
            watermark[uf] = df["data_venda"].max().isoformat()

        _save_watermark(watermark)

    [chk_vendas_tem_dados,chk_colunas_vendas, chk_integridade_por_join] >> exportar_incremental_por_estado()


