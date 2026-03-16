import pandas as pd
import logging

logger = logging.getLogger(__name__)

def standardization_data(df: pd.DataFrame) -> pd.DataFrame:
    
    logger.info("Início da padronização.")

    if df is None:
        raise ValueError("Objeto None")

    if not isinstance(df, pd.DataFrame):
        raise TypeError("Objeto não é um pandas DataFrame")

    if df.empty:
        raise ValueError("DataFrame vazio")
    
    try:    
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.lstrip("_")
        )

        if "data" in df.columns:
            df["data"] = (
                pd.to_datetime(
                    df["data"],
                    errors="coerce"
                )
                .dt.date
            )

        if "hora" in df.columns:
            df["hora"] = pd.to_datetime(
                df["hora"],
                format="%H:%M:%S",
                errors="coerce"
            ).dt.time

        capitalize_columns = ["bairro", "endereco", "detalhe_endereco_acidente",
            "endereco_cruzamento", "bairro_cruzamento"]

        for col in capitalize_columns:
            if col in df.columns:
                df[col] = df[col].str.title()

        columns_to_capitalize = ["natureza_acidente", "situacao", "complemento",
            "referencia_cruzamento", "sentido_via",
            "tipo", "descricao"]

        for col in columns_to_capitalize:
            if col in df.columns:
                df[col] = df[col].str.strip().str.capitalize()  

        logger.info("Colunas processadas: %d", len(df.columns))
        logger.info("Registros processados: %d", len(df))              
    
        logger.info("Padronização concluída com sucesso.")

        return df

    except Exception as e:
        logger.error("Erro na padronização de dados.", exc_info=True)
        raise