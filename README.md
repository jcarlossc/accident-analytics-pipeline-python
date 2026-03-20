# Accident Data Pipeline (Python + Poetry)

Pipeline completo para coleta, padronização, limpeza e validação de dados de acidentes, desenvolvido com foco em boas práticas de engenharia 
de dados, arquitetura modular e configuração externa via YAML.

---

## 📌 Visão Geral

Este projeto implementa um pipeline robusto para processamento de dados, incluindo:

* 🔄 Coleta de dados via API
* 🧹 Limpeza e tratamento
* 🧱 Padronização de colunas e registros
* ✅ Validação de dados
* 📊 Geração de saídas estruturadas
* 📝 Logging detalhado

Tudo isso com:

* Arquitetura modular
* Tratamento de erros (try/except)
* Configuração desacoplada via YAML
* Pronto para produção

---

## 📌 Arquitetura do Projeto
```
accident-analytics-pipeline-python/
|
├── config/
│     ├── config.yaml
│     ├── logging.yaml
│     └── paths.yaml
├── data/
│     ├── processed/
│     └── raw/
│          └── acidentes_recife_2019.csv
├── logs/
│     └── app.log
├── src/
|    ├── accident_analytics_pipeline_python/
│    |     ├── cleaning/
|    |     |      └── data_clean.py
│    |     ├── ingestion/
|    |     |      └── data_ingest.py
│    |     ├── standardization/
|    |     |      └── data_standardization.py
│    |     ├── utils/
│    |     |    ├── helpers/
|    |     |    |     └── helper.py
│    |     |    ├── load_config/
|    |     |    |     └── load_config.py
│    |     |    └── logger/
|    |     |          └── logger.py
│    |     └── validation/
|    |          └── data_validation.py
├── tests/
|    ├── test_data_clean.py
|    ├── test_data_ingest.py
|    ├── test_data_standardization.py
|    ├── test_data_validation.py
|    ├── test_helper.py
|    ├── test_load_config.py
|    ├── test_logger.py
|    └── test_main.py
├── .gitignore
├── LICENSE
├── poetry.lock
├── pyproject.toml
└── README
```

## 📌 Tecnologias Utilizadas
| Tecnologia | Descrição |
| ---------- | --------- |
| Python 3.11+ | Linguagem de programação |
| Poetry | Gerenciamento de dependências |
| Pandas | Biblioteca para Análise de Dados |
| PyYAML | Formato de serialização de dados |
| Logging | Sistema de log nativo |
| pytest | Teste unitários |
| mypy | Biblioteca para tipagem estática |
| Black | Biblioteca para formatação do código |

---







