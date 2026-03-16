import pandas as pd
import logging
from datetime import date, time

logger = logging.getLogger(__name__)

def validation_data(df: pd.DataFrame) -> None:
    try:

        logger.info("Iniciando validação dos dados")

        if df["data"].map(lambda x: isinstance(x, date) or pd.isna(x)).all():
            logger.info("A coluna data está no formato date")
        else:
            logger.info("Existem valores inválidos")

        if df["hora"].map(lambda x: isinstance(x, time) or pd.isna(x)).all():
            logger.info("A coluna hora está no formato time")
        else:
            logger.info("Existem valores inválidos")

        numeric_columns = [
            "auto",
            "moto",
            "ciclom",
            "ciclista",
            "pedestre",
            "onibus",
            "caminhao",
            "viatura",
            "outros",
            "vitimas",
            "vitimasfatais"
        ]

        for col in numeric_columns:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    raise ValueError(f"Coluna '{col}' não é numérica.")

        text_columns = [
            "natureza_acidente",
            "situacao",
            "bairro",
            "endereco",
            "numero",
            "detalhe_endereco_acidente",
            "complemento",
            "endereco_cruzamento",
            "numero_cruzamento",
            "referencia_cruzamento",
            "bairro_cruzamento",
            "num_semaforo",
            "sentido_via",
            "tipo",
            "descricao",
            "acidente_verificado",
            "tempo_clima",
            "situacao_semaforo",
            "sinalizacao",
            "condicao_via",
            "conservacao_via",
            "ponto_controle",
            "situacao_placa",
            "velocidade_max_via",
            "mao_direcao",
            "divisao_via1",
            "divisao_via2",
            "divisao_via3"
        ]

        for col in text_columns:
            if col in df.columns:
                if df[col].isna().any():
                    raise ValueError(
                        f"Coluna '{col}' contém valores NA."
                    )

        invalid_dates = (~df["data"].map(lambda x: isinstance(x, date) or pd.isna(x))).sum()
        logger.info("Valores inválidos na coluna data: %d", invalid_dates)          

        logger.info("Colunas processadas: %d", len(df.columns))
        logger.info("Registros processados: %d", len(df))         

        logger.info("Validação concluída com sucesso.")

    except Exception as e:
        logger.error("Erro na validação dos dados.", exc_info=True)
        raise