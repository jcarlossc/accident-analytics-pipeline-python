"""
Limpeza de dados de acidentes.
"""

import pandas as pd
import logging


logger = logging.getLogger(__name__)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:

    try:
        logger.info("Início da limpeza dos dados.")

        # -----------------------------------
        # Verificar se objeto é None
        # (equivalente ao NULL no R)
        # -----------------------------------
        if df is None:
            raise ValueError("Objeto None")

        # -----------------------------------
        # Verificar se é DataFrame
        # -----------------------------------
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Objeto não é um pandas DataFrame")

        # -----------------------------------
        # Verificar se DataFrame está vazio
        # -----------------------------------
        if df.empty:
            raise ValueError("DataFrame vazio")


        columns = [
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

        df[columns] = (
            df[columns]
            .replace(["", " "], pd.NA)          # valores em branco -> NA
            .apply(pd.to_numeric, errors="coerce")  # converter para número
            .fillna(0)                          # NA -> 0
        )
             

        categorical_columns = [
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

        df[categorical_columns] = (
            df[categorical_columns]
            .replace(["", " ", "-", "NA", "null"], pd.NA)
            .fillna("Não informado")
        )

        logger.info("Término da limpeza dos dados")

        return df

    except Exception as e:
        logger.error("Erro na limpeza dos dados.", exc_info=True)
        raise


 