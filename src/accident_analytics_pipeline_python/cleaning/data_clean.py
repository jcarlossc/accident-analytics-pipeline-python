import pandas as pd
import logging

# Instância de logger associada a este módulo.
# Utilizar __name__ permite integrar o logger à hierarquia de logs da aplicação,
# facilitando a configuração centralizada e o controle de níveis de log.
logger = logging.getLogger(__name__)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza a limpeza e normalização dos dados de um DataFrame.

    Esta função aplica regras de limpeza em colunas numéricas e categóricas,
    tratando valores ausentes, corrigindo inconsistências comuns em dados
    brutos e garantindo que o dataset esteja adequado para etapas
    posteriores do pipeline de processamento.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame contendo os dados a serem limpos.

    Retorno
    -------
    pd.DataFrame
        DataFrame com valores tratados e padronizados.

    Exceções
    --------
    ValueError
        Lançada quando o objeto recebido é None ou quando o DataFrame está vazio.

    TypeError
        Lançada quando o objeto recebido não é um pandas DataFrame.

    Observações
    -----------
    Esta função geralmente é executada após as etapas de ingestão e
    padronização de schema em pipelines ETL/ELT.
    """

    # Registro do início do processo de limpeza.
    # Esse log é útil para rastrear o fluxo de execução do pipeline.
    logger.info("Início da limpeza dos dados.")

    # Validação 1: verifica se o objeto recebido é None.
    # Essa verificação evita erros de acesso a atributos do objeto.
    if df is None:
            raise ValueError("Objeto None")

    # Validação 2: garante que o objeto recebido é um DataFrame.
    # Isso protege a função contra tipos de dados incorretos.
    if not isinstance(df, pd.DataFrame):
            raise TypeError("Objeto não é um pandas DataFrame")

    # Validação 3: verifica se o DataFrame contém registros.
    # DataFrames vazios podem indicar falhas na ingestão ou extração.
    if df.empty:
            raise ValueError("DataFrame vazio")
    
    try:
        # Lista de colunas que representam contagens de veículos ou vítimas.
        # Essas colunas devem ser tratadas como valores numéricos.
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

        # Limpeza e conversão das colunas numéricas.
        # Etapas aplicadas:
        # 1. Substituição de valores vazios por NA
        # 2. Conversão para tipo numérico
        # 3. Substituição de valores ausentes por zero
        df[columns] = (
            df[columns]
            .replace(["", " "], pd.NA)         
            .apply(pd.to_numeric, errors="coerce") 
            .fillna(0)                          
        )
             
        # Lista de colunas categóricas que representam atributos textuais
        # do acidente, local ou condições da via.     
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

        # Tratamento das colunas categóricas.
        # Valores inconsistentes comuns em dados brutos são convertidos para NA,
        # e posteriormente substituídos por um valor padrão.
        df[categorical_columns] = (
            df[categorical_columns]
            .replace(["", " ", "-", "NA", "null"], pd.NA)
            .fillna("Não informado")
        )

        # Registra no log a quantidade de colunas e de registros.
        logger.info("Colunas processadas: %d", len(df.columns))
        logger.info("Registros processados: %d", len(df))

        # Registra no log de término do processo de limpeza.
        logger.info("Término da limpeza dos dados")

        # Retorna o DataFrame tratado para uso nas etapas seguintes do pipeline.
        return df

    except Exception as e:
        # Registro detalhado do erro com stack trace completo.
        # Isso facilita a investigação de falhas durante a execução do pipeline.
        logger.error("Erro na limpeza dos dados.", exc_info=True)

        # Propaga a exceção para que camadas superiores possam tratá-la.
        raise


 