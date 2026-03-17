from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def save_csv(df: pd.DataFrame, path: str | Path) -> None:
    try:
        logger.info("Iniciando salvamento do arquivo CSV.")

        path = Path(path)

        if df is None:
            raise ValueError("DataFrame é None.")

        if df.empty:
            raise ValueError("DataFrame vazio. Nada para salvar.")

        path.parent.mkdir(parents=True, exist_ok=True)

        df.to_csv(
            path,
            index=False,
            encoding="utf-8"
        )

        logger.info("Arquivo CSV salvo com sucesso em: %s", path)

    except Exception:
        logger.error("Erro ao salvar arquivo CSV.", exc_info=True)
        raise