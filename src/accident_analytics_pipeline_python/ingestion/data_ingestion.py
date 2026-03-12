import pandas as pd
import logging

logger = logging.getLogger(__name__)

def ingest_data(path: str) -> pd.DataFrame:
    try:
        logger.info("Coleta de dados iniciada.")
        df = pd.read_csv(path)
        logger.info("Coleta concluída com sucesso.")
        return df

    except Exception as e:
        logger.error("Erro no coleta de dados.", exc_info=True)
        raise    