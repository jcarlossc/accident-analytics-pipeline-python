import logging
from pathlib import Path
from accident_analytics_pipeline_python.utils.logger.logger import setup_logger
from accident_analytics_pipeline_python.utils.load_config.load_config import load_all_configs
from accident_analytics_pipeline_python.ingestion.data_ingestion import ingest_data
from accident_analytics_pipeline_python.standardization.data_standardization import standardization_data
from accident_analytics_pipeline_python.cleaning.data_clean import clean_data




def main() -> None:


    config_path = Path("config")

    config = load_all_configs(config_path)

    setup_logger(
        config["logging"],
        config["paths"]["logs"]["file"]
    )
    logger = logging.getLogger(__name__)


    try:

        logger.info("Iniciando pipeline de acidentes.")

        # Coleta
        df = ingest_data(config["paths"]["data"]["raw"])
        print(df)
        df = standardization_data(df)
        print(df)

        df = clean_data(df)
        print(df)

        logger.info("Término do pipeline de acidentes.")

    except Exception as e:
        logger.critical("Erro crítico no pipeline.", exc_info=True)
        if config["config"]["pipeline"]["fail_on_error"]:
            raise

if __name__ == "__main__":
    main()