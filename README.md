# 🚀 Apache Airflow - Fundamentos & Prática de Engenharia de Dados

Repositório contendo implementações práticas, conceitos fundamentais e pipelines de dados desenvolvidos com o **Apache Airflow**. O projeto cobre desde conceitos básicos de orquestração (DAGs, Tasks, Operadores) até arquiteturas orientadas a eventos (Datasets / Data-aware Scheduling), plugins customizados, particionamento e processamento em lote com arquivos Parquet e CSV.

---

## 📁 Estrutura do Repositório

```plaintext
Airflow-Fundamentos/
├── config/
│   └── airflow.cfg               # Arquivo de configuração customizado do Airflow
├── dags/
│   ├── 1.PrimeiraDag.py          # Introdução à criação de DAGs e Tasks
│   ├── 2.SegundaDag.py           # Encadeamento básico de operadores
│   ├── 3.TerceiraDag.py          # Estruturas de dependências
│   ├── 4.DeafultArgs.py          # Definição e boas práticas de default_args
│   ├── 5.TriggerDag1.py          # Triggers entre DAGs (TriggerDagRunOperator)
│   ├── 6.TriggerDag2.py          # Alvo de disparo por trigger
│   ├── 6.TriggerDag3.py          # Variações e condições de trigger
│   ├── 8.DagComplexa.py          # Topologia com múltiplos caminhos paralelos
│   ├── 9.DagGroup.py             # Agrupamento e organização visual com TaskGroup
│   ├── 10.XCom.py                # Compartilhamento básico de estado via XCom
│   ├── 11.XCom2.py               # Manipulação avançada de XComs
│   ├── 12.Dagrundag1.py          # Encadeamento e orquestração DAG-to-DAG
│   ├── 13.Dagrundag2.py          # Continuidade de execução de pipeline externo
│   ├── 17.EmptyOperator.py       # Uso de EmptyOperator para controle de fluxo
│   ├── 18.Variaveis.py           # Gestão e leitura de Airflow Variables
│   ├── 19.Pools.py               # Gerenciamento e controle de concorrência com Pools
│   ├── 20.Branch.py              # Desvios condicionais com BranchPythonOperator
│   ├── 21.ShortCircuit.py        # Interrupção antecipada com ShortCircuitOperator
│   ├── 22.Producer.py            # Produtor em arquitetura baseada em Datasets
│   ├── 23.Consumer.py            # Consumidor disparado por atualização de Dataset
│   ├── 24.Sensor.py              # Monitoramento e espera ativa via Sensors
│   ├── 25.Dinamico.py            # DAGs dinâmicas (Dynamic Task Mapping / geração dinâmica)
│   ├── 26.BigDataPlugin.py       # DAG integrando plugins customizados
│   └── 27.Pipeline.py            # Pipeline ETL de ponta a ponta (limpeza, transformação e carga)
├── data/
│   ├── Churn.csv                 # Dataset de exemplo de churn de clientes
│   ├── Churn.parquet             # Dados convertidos/armazenados em formato colunar
│   ├── Churn_new.csv             # Arquivos de ingestão incremental
│   ├── Churn_new2.csv
│   ├── _watermark_por_estado.json# Controle incremental/watermark por partição
│   └── vendas/                   # Lakehouse local particionado por Estado, Ano e Mês
│       ├── estado=ac/ano=2026/mes=08/
│       ├── estado=al/ano=2026/mes=08/
│       ├── ...
│       └── estado=to/ano=2026/mes=08/
├── plugins/
│   ├── big_data_operator.py      # Operador customizado para processamento em larga escala
│   └── big_data_plugin.py        # Registro e empacotamento do plugin no Airflow
├── docker-compose.yaml           # Orquestração de containers do Apache Airflow
└── .gitignore                    # Arquivos ignorados pelo Git (.env, logs, venv)
