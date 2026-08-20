# 🌪️ Apache Airflow — Orquestração de Pipelines & Engenharia de Dados[cite: 2]

Repositório estruturado para demonstrar o domínio prático e arquitetural do **Apache Airflow** aplicado a cenários reais de **Engenharia de Dados**[cite: 2]. O projeto engloba desde agendamento básico, controle de concorrência e dependências complexas até comunicação via XComs, orquestração *event-driven* (Datasets Producer/Consumer), sensores e desenvolvimento de **Plugins/Operadores Customizados**[cite: 2].

---

## 📌 Visão Geral & Recursos de Engenharia

- **Ambiente Containerizado:** Setup configurado via `docker-compose.yaml` com Webserver, Scheduler e PostgreSQL para banco de metadados[cite: 2].
- **Controle de Fluxo Avançado:** Implementação de `TriggerRules`, `Branching`, `ShortCircuit`, `TaskGroups` e geração dinâmica de DAGs[cite: 2].
- **Gerenciamento de Recursos:** Uso de `Pools` para controle de concorrência e `Variables` para parametrização global desacoplada[cite: 2].
- **Extensibilidade & Plugins:** Criação de operadores customizados (`BigDataOperator`) para manipulação de arquivos estruturados (conversão de CSV para Parquet)[cite: 2].

---

## 🛠️ Tecnologias & Ferramentas

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)

- **Orquestrador:** Apache Airflow 2.x[cite: 2]
- **Infraestrutura:** Docker & Docker Compose[cite: 2]
- **Linguagem:** Python[cite: 2]
- **Formatos Manipulados:** CSV, Parquet[cite: 2]

---

## 📂 Estrutura das DAGs e Casos de Uso

O diretório `dags/` está estruturado em uma trilha de complexidade progressiva[cite: 2]:

| Arquivo / DAG | Conceito & Implementação |
|:---|:---|
| `1.PrimeiraDag.py` a `3.TerceiraDag.py`[cite: 2] | Definição base de DAGs, execução via `BashOperator` / `PythonOperator` e encadeamento (`>>`, `<<`). |
| `4.DeafultArgs.py`[cite: 2] | Padronização de políticas de retry, intervalos e tolerância a falhas via `default_args`. |
| `5.TriggerDag1.py` a `6.TriggerDag3.py`[cite: 2] | Gerenciamento de execução condicional com `TriggerRule` (`all_success`, `one_failed`, `all_done`). |
| `8.DagComplexa.py`[cite: 2] | Orquestração de grafos multidirecionais com tarefas paralelas e pontos de convergência. |
| `9.DagGroup.py`[cite: 2] | Organização visual e modularização de pipelines com `TaskGroup`. |
| `10.XCom.py` / `11.XCom2.py`[cite: 2] | Comunicação e transferência de pequenos estados/metadados entre tarefas via `xcom_push` e `xcom_pull`. |
| `12.Dagrundag1.py` / `13.Dagrundag2.py`[cite: 2] | Orquestração cross-DAG acionando pipelines dependentes via `TriggerDagRunOperator`. |
| `17.EmptyOperator.py`[cite: 2] | Criação de nós de sincronização e marcos no pipeline com `EmptyOperator`. |
| `18.Variaveis.py`[cite: 2] | Leitura e consumo de variáveis globais gerenciadas pela interface/metadados do Airflow. |
| `19.Pools.py`[cite: 2] | Isolamento de recursos e limitação de slots de concorrência com `Pools`. |
| `20.Branch.py`[cite: 2] | Desvio dinâmico de fluxo em runtime utilizando `BranchPythonOperator`. |
| `21.ShortCircuit.py`[cite: 2] | Interrupção inteligente de etapas downstream usando `ShortCircuitOperator`. |
| `22.Producer.py` / `23.Consumer.py`[cite: 2] | Arquitetura reativa (*Event-Driven*) com **Airflow Datasets** (disparo automático baseado na atualização de dados). |
| `24.Sensor.py`[cite: 2] | Bloqueio e detecção assíncrona de arquivos com `FileSensor`. |
| `25.Dinamico.py`[cite: 2] | **Dynamic DAGs:** Geração dinâmica de tasks em runtime a partir de iterações e parâmetros. |
| `26.BigDataPlugin.py`[cite: 2] | Execução de rotina de transformação consumindo operadores customizados da pasta `plugins/`[cite: 2]. |

---

## 🧩 Plugins Customizados

O diretório `plugins/` demonstra a extensibilidade nativa da ferramenta[cite: 2]:
- **`big_data_operator.py`:** Operador customizado para leitura de dados brutos (`data/Churn.csv`) e transformação/persistência em formato colunar otimizado (`data/Churn.parquet`)[cite: 2].
- **`big_data_plugin.py`:** Registro e exportação da classe via `AirflowPlugin`[cite: 2].

---

## 🚀 Como Executar Localmente

### Pré-requisitos
- Docker & Docker Compose instalados.

### Passo a Passo

1. **Clone o repositório:**
```bash
git clone [https://github.com/lucasmlima/Airflow-Fundamentos.git](https://github.com/lucasmlima/Airflow-Fundamentos.git)
cd Airflow-Fundamentos
