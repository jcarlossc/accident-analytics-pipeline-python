import pandas as pd
import logging
from datetime import date, time

# Instância de logger associada ao módulo atual.
# Utilizar __name__ permite que o sistema de logging organize os logs
# de acordo com a hierarquia dos módulos da aplicação.
logger = logging.getLogger(__name__)

def validation_data(df: pd.DataFrame) -> None:
    """
    Realiza validações de integridade e consistência em um DataFrame.

    Esta função verifica se determinadas colunas possuem os tipos
    de dados esperados e se não contêm valores inválidos ou ausentes.
    Essas validações são importantes para garantir a qualidade dos
    dados antes de etapas posteriores do pipeline, como análises,
    agregações ou persistência em banco de dados.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame contendo os dados que serão validados.

    Retorno
    -------
    None
        A função não retorna valores. Caso alguma inconsistência seja
        encontrada, uma exceção é lançada.

    Observações
    -----------
    Esta etapa normalmente ocorre após processos de ingestão,
    padronização e limpeza de dados em pipelines ETL/ELT.
    """

    # Registro do início do processo de validação.
    # Esse log é útil para rastrear o fluxo de execução do pipeline.
    logger.info("Iniciando validação dos dados")

    # Verifica se todos os valores da coluna "data" são do tipo
    # datetime.date ou valores ausentes (NaN/NaT).
    # Essa validação garante que a coluna foi corretamente convertida
    # durante a etapa de padronização.
    if "data" in df.columns:
        if df["data"].map(lambda x: isinstance(x, date) or pd.isna(x)).all():
            logger.info("A coluna data está no formato date")
        else:
            logger.info("Existem valores inválidos na coluna data")

    # Verifica se todos os valores da coluna "hora" são do tipo
    # datetime.time ou valores ausentes.
    # Isso garante consistência para análises temporais.
    if "hora" in df.columns:
        if df["hora"].map(lambda x: isinstance(x, time) or pd.isna(x)).all():
            logger.info("A coluna hora está no formato time")
        else:
            logger.info("Existem valores inválidos na coluna hora")
    
    try:
        # Lista de colunas que representam contagens de veículos
        # ou vítimas. Essas colunas devem obrigatoriamente possuir
        # tipo numérico para permitir cálculos e agregações.
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

        # Verifica se cada coluna possui tipo numérico.
        # Caso contrário, interrompe a execução com erro.
        for col in numeric_columns:
            if col in df.columns:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    raise ValueError(f"Coluna '{col}' não é numérica.")

        # Lista de colunas categóricas ou textuais que não devem conter
        # valores ausentes após o processo de limpeza.
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

        # Verifica se as colunas textuais possuem valores ausentes.
        # Caso existam valores NA, uma exceção é lançada para evitar
        # que dados incompletos sejam utilizados em análises.
        for col in text_columns:
            if col in df.columns:
                if df[col].isna().any():
                    raise ValueError(
                        f"Coluna '{col}' contém valores NA."
                    )
        
        # Registra no log a quantidade de colunas e de registros.
        logger.info("Colunas processadas: %d", len(df.columns))
        logger.info("Registros processados: %d", len(df))         

        # Registra no log de término do processo de validação.
        logger.info("Validação concluída com sucesso.")

    except Exception as e:
        # Registro detalhado do erro com stack trace completo.
        # Isso facilita a investigação de falhas durante a execução do pipeline.
        logger.error("Erro na validação dos dados.", exc_info=True)

        # Propaga a exceção para que camadas superiores possam tratá-la.
        raise