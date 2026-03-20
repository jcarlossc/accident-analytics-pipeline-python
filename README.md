# Accident Data Pipeline (Python + Poetry)

Pipeline completo para coleta, padronização, limpeza e validação de dados de acidentes, desenvolvido com foco em boas práticas de engenharia 
de dados, arquitetura modular e configuração externa via YAML.

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
* Arquitetura pronta para produção

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
|    └── accident_analytics_pipeline_python/
│         ├── cleaning/
|         |      └── data_clean.py
│         ├── ingestion/
|         |      └── data_ingest.py
│         ├── standardization/
|         |      └── data_standardization.py
│         ├── utils/
│         |    ├── helpers/
|         |    |     └── helper.py
│         |    ├── load_config/
|         |    |     └── load_config.py
│         |    └── logger/
|         |          └── logger.py
│         └── validation/
|              └── data_validation.py
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

## 📌 Pipeline
O pipeline segue um fluxo típico de ETL (Extract → Transform → Load).
```
           +------------------+
           |  Dados Brutos    |
           |  acidentes_2019  |
           +--------+---------+
                    v
           +------------------+
           |  Ingestão        |
           |  data_ingest.py  |
           +--------+---------+
                    v
           +---------------------------+
           | Padronização              |
           | data_standardization.py   |
           +--------+------------------+
                    v
           +------------------+
           | Limpeza          |
           | data_clean.py    |
           +--------+---------+
                    v
           +------------------+
           | Validação        |
           | data_validate.py |
           +--------+---------+
                    v
           +------------------+
           | Dados Processados|
           +------------------+
```

## 📌 Fluxo do Pipeline
### 1. Ingestão
* Coleta via CSV
* Logging de sucessos e falhas
### 2. Padronização
* Padronização de colunas
* Tipagem de colunas
### 3. Limpeza
* Tratamento de valores nulos
* Verificação de consistência
### 4. Validação
* Validação dos dados padronizados e limpos
### 5. Saída
* Exportação para CSV
* Dados prontos para análise

## 📌 Configuração do Projeto (YAML)
Arquivos:
```
config/config.yaml
config/logging.yaml
config/paths.yaml
```

## 📌 Documentação das Colunas (alguns exemplos)
| Coluna |	Tipo | Descrição |
|--------|------|-----------|
| data |	Date | Data do acidente |
| hora |	Time | Hora do acidente |
| bairro |	Character | Bairro da ocorrência |
| auto |	Integer |	Número de automóveis |
| moto |	Integer |	Número de motocicletas |
| ciclista | Integer | Número de ciclistas |
| pedestre | Integer | Número de pedestres |
| vitimas | Integer |	Total de vítimas |
| vitimasfatais | Integer | Número de vítimas fatais |

## 📌 Métricas de Qualidade de Dados
Durante a validação, o pipeline verifica métricas importantes de qualidade.

| Métrica | Descrição |
| ------- | --------- |
| Completeness | Percentual de valores não nulos |
| Consistency | Consistência entre colunas |
| Validity	| Valores dentro do domínio permitido |
| Timeliness | Datas válidas |
| Timeliness | Horas válidas |

Exemplo de validações:
* datas válidas
* horas válidas
* valores numéricos ≥ 0
* colunas categóricas não nulas

## 📌 Sistema de Logging
O pipeline utiliza logger para registrar eventos.

Alguns exemplo:
```
2026-03-20 17:19:30,847 - INFO - accident_analytics_pipeline_python.main - ### Iniciando pipeline de acidentes. ###
2026-03-20 17:19:30,847 - INFO - accident_analytics_pipeline_python.ingestion.data_ingestion - Iniciando coleta de dados: data\raw\acidentes_recife_2019.csv
2026-03-20 17:19:31,613 - INFO - accident_analytics_pipeline_python.ingestion.data_ingestion - Colunas processadas: 42
2026-03-20 17:19:31,613 - INFO - accident_analytics_pipeline_python.ingestion.data_ingestion - Registros processados: 12062
2026-03-20 17:19:31,613 - INFO - accident_analytics_pipeline_python.ingestion.data_ingestion - Coleta concluída com sucesso.
2026-03-20 17:19:31,613 - INFO - accident_analytics_pipeline_python.standardization.data_standardization - Início da padronização.
2026-03-20 17:19:32,276 - INFO - accident_analytics_pipeline_python.standardization.data_standardization - Colunas processadas: 42
2026-03-20 17:19:32,276 - INFO - accident_analytics_pipeline_python.standardization.data_standardization - Registros processados: 12062
2026-03-20 17:19:32,276 - INFO - accident_analytics_pipeline_python.standardization.data_standardization - Padronização concluída com sucesso.

```
Logs armazenados em:
```
logs/app.log
```

## 📌 Modo de Utilizar


## 📌 Licença
Este projeto está licenciado sob MIT License.

## 📌 Contato
* Recife, PE - Brasil<br>
* Telefone: +55 81 99712 9140<br>
* Telegram: @jcarlossc<br>
* Pypi: https://pypi.org/user/jcarlossc/<br>
* Blogger linguagem R: [https://informaticus77-r.blogspot.com/](https://informaticus77-r.blogspot.com/)<br>
* Blogger linguagem Python: [https://informaticus77-python.blogspot.com/](https://informaticus77-python.blogspot.com/)<br>
* Email: jcarlossc1977@gmail.com<br>
* LinkedIn: https://www.linkedin.com/in/carlos-da-costa-669252149/<br>
* GitHub: https://github.com/jcarlossc<br>
* Kaggle: https://www.kaggle.com/jcarlossc/  
* Twitter/X: https://x.com/jcarlossc1977



