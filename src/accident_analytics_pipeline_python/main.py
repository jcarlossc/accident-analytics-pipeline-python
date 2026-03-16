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
    config_path = Path("config")

    config = load_all_configs(config_path)

    setup_logger(
        config["logging"],
        config["paths"]["logs"]["file"]
    )
    logger = logging.getLogger(__name__)

    try:
        logger.info("Iniciando pipeline de acidentes.")

        df = ingest_data(config["paths"]["data"]["raw"])

        df = standardization_data(df)

        df = clean_data(df)

        validation_data(df)

        save_csv(df, config["paths"]["data"]["processed"])

        logger.info("Término do pipeline de acidentes.")

    except Exception as e:
        logger.critical("Erro crítico no pipeline.", exc_info=True)
        if config["config"]["pipeline"]["fail_on_error"]:
            raise

if __name__ == "__main__":
    main()