import logging
from pathlib import Path

from accident_analytics_pipeline_python.utils.loggers.logger import setup_logger
from accident_analytics_pipeline_python.utils.load_config.load_config import load_all_configs
from accident_analytics_pipeline_python.ingestion.data_ingestion import ingest_data
from accident_analytics_pipeline_python.standardization.data_standardization import standardization_data
from accident_analytics_pipeline_python.cleaning.data_clean import clean_data
from accident_analytics_pipeline_python.validation.data_validation import validation_data
from accident_analytics_pipeline_python.utils.helpers.helper import save_csv

def main() -> None:
    """
    Função principal responsável por orquestrar a execução
    do pipeline de processamento de dados de acidentes.

    O pipeline executa as seguintes etapas:

    1. Carregamento das configurações do projeto
    2. Configuração do sistema de logging
    3. Ingestão de dados brutos
    4. Padronização do dataset
    5. Limpeza dos dados
    6. Validação de integridade e qualidade
    7. Persistência do dataset processado

    Obs: no CMD, na raiz do sistema, "poetry run acidentes", executa o pipeline. 
    """

    # Diretório contendo os arquivos de configuração
    config_path = Path("config")

    # Carrega todos os arquivos de configuração do projeto
    config = load_all_configs(config_path)

    # Configuração do sistema de logging da aplicação
    setup_logger(
        config["logging"],
        config["paths"]["logs"]["file"]
    )

    # Instância do logger para este módulo
    logger = logging.getLogger(__name__)

    try:

        logger.info("### Iniciando pipeline de acidentes. ###")

        # ------------------------------------------------------------------
        # 1. Ingestão dos dados brutos
        # ------------------------------------------------------------------
        df = ingest_data(config["paths"]["data"]["raw"])

        # ------------------------------------------------------------------
        # 2. Padronização estrutural do dataset
        # ------------------------------------------------------------------
        df = standardization_data(df)

        # ------------------------------------------------------------------
        # 3. Limpeza e tratamento de inconsistências
        # ------------------------------------------------------------------
        df = clean_data(df)

        # ------------------------------------------------------------------
        # 4. Validação de qualidade e integridade dos dados
        # ------------------------------------------------------------------
        validation_data(df)

        # ------------------------------------------------------------------
        # 5. Salvamento do dataset processado
        # ------------------------------------------------------------------
        save_csv(df, config["paths"]["data"]["processed"])

        logger.info("### Término do pipeline de acidentes. ###")

    except Exception as e:
        logger.critical("Erro crítico no pipeline.", exc_info=True)

        # Caso configurado, interrompe a execução do pipeline
        if config["config"]["pipeline"]["fail_on_error"]:
            raise

if __name__ == "__main__":
    main()