import logging
import pandas as pd
from pathlib import Path

def ingest_data(path: str | Path) -> pd.DataFrame:
    
    logger = logging.getLogger(__name__)

    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    if path.stat().st_size == 0:
        raise ValueError("Arquivo CSV está vazio.")

    try:
        logger.info("Iniciando coleta de dados: %s", path)

        df = pd.read_csv(path)

        if df.empty:
            raise ValueError("DataFrame vazio após leitura.")

        logger.info("Coleta concluída com sucesso.")

        return df

    except Exception as e:
        logger.error("Erro ao coletar de dados.", exc_info=True)
        raise    