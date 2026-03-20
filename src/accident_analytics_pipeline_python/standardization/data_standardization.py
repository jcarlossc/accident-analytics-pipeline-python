import pandas as pd
import logging

# Instância de logger para este módulo.
# O nome do logger utiliza __name__ para seguir a hierarquia de logs do projeto.
# Isso permite configurar logs centralmente na aplicação.
logger = logging.getLogger(__name__)


def standardization_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza a padronização estrutural e textual de um DataFrame.

    Esta função aplica transformações comuns em pipelines de preparação
    de dados, incluindo normalização de nomes de colunas, conversão de
    tipos de dados e padronização de campos textuais.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame contendo os dados brutos a serem padronizados.

    Retorno
    -------
    pd.DataFrame
        DataFrame com estrutura e valores padronizados para uso
        em etapas posteriores do pipeline de dados.

    Exceções
    --------
    ValueError
        Lançada quando o DataFrame é None ou está vazio.

    TypeError
        Lançada quando o objeto recebido não é um pandas DataFrame.

    Observações
    -----------
    Esta etapa normalmente ocorre após a ingestão de dados em pipelines
    ETL/ELT, garantindo consistência de schema e padronização de valores
    antes da etapa de análise ou persistência.
    """

    # Registro do início do processo de padronização.
    logger.info("Início da padronização.")

    # Validação 1: verifica se o objeto recebido é None.
    # Essa verificação evita falhas posteriores ao acessar atributos do objeto
    if df is None:
        raise ValueError("Objeto None")

    # Validação 2: garante que o objeto recebido é um DataFrame.
    # Isso evita erros caso outro tipo de objeto seja passado para a função.
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Objeto não é um pandas DataFrame")

    # Validação 3: verifica se o DataFrame possui registros.
    # DataFrames vazios podem indicar falha na etapa de ingestão.
    if df.empty:
        raise ValueError("DataFrame vazio")

    try:
        # Padronização dos nomes das colunas.
        # As seguintes transformações são aplicadas:
        # - remoção de espaços extras
        # - conversão para minúsculas
        # - substituição de espaços por underline
        # - remoção de underline no início da string
        # Essa normalização facilita o acesso às colunas no código.
        df.columns = (
            df.columns.str.strip().str.lower().str.replace(" ", "_").str.lstrip("_")
        )

        # Conversão da coluna "data" para tipo datetime.
        # O parâmetro errors="coerce" converte valores inválidos para NaT,
        # evitando falhas durante a transformação.
        if "data" in df.columns:
            df["data"] = pd.to_datetime(df["data"], errors="coerce").dt.date

        # Conversão da coluna "hora" para tipo time.
        # A formatação explícita melhora a confiabilidade da conversão.
        if "hora" in df.columns:
            df["hora"] = pd.to_datetime(
                df["hora"], format="%H:%M:%S", errors="coerce"
            ).dt.time

        # Lista de colunas que devem ter cada palavra iniciada com letra maiúscula.
        # Método title() é usado para padronizar nomes próprios e endereços.
        capitalize_columns = [
            "bairro",
            "endereco",
            "detalhe_endereco_acidente",
            "endereco_cruzamento",
            "bairro_cruzamento",
        ]

        # Aplica capitalização do tipo Title Case nas colunas selecionadas.
        for col in capitalize_columns:
            if col in df.columns:
                df[col] = df[col].str.title()

        # Lista de colunas que devem ter apenas a primeira letra maiúscula.
        # O método capitalize() é adequado para descrições ou categorias.
        columns_to_capitalize = [
            "natureza_acidente",
            "situacao",
            "complemento",
            "referencia_cruzamento",
            "sentido_via",
            "tipo",
            "descricao",
        ]

        # Aplica limpeza de espaços e capitalização da primeira letra.
        for col in columns_to_capitalize:
            if col in df.columns:
                df[col] = df[col].str.strip().str.capitalize()

        # Registra no log a quantidade de colunas e de registros.
        logger.info("Colunas processadas: %d", len(df.columns))
        logger.info("Registros processados: %d", len(df))

        # Registro indicando que a padronização foi concluída com sucesso.
        logger.info("Padronização concluída com sucesso.")

        # Retorna o DataFrame transformado.
        return df

    except Exception as e:
        # Registro de erro detalhado com stack trace completo.
        # Isso facilita a identificação de problemas no pipeline.
        logger.error("Erro na padronização de dados.", exc_info=True)

        # Propaga a exceção para que a camada superior trate o erro.
        raise
