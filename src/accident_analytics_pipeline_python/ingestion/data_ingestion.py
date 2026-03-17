import logging
import pandas as pd
from pathlib import Path

# Instância de logger para este módulo.
# O nome do logger utiliza __name__ para seguir a hierarquia de logs do projeto.
# Isso permite configurar logs centralmente na aplicação.
logger = logging.getLogger(__name__)

def ingest_data(path: str | Path) -> pd.DataFrame:
    """
    Realiza a ingestão de dados a partir de um arquivo CSV.

    Esta função é responsável por carregar um dataset bruto (raw data)
    para um DataFrame do pandas, realizando validações básicas de
    integridade antes e após a leitura do arquivo.

    Parâmetros
    ----------
    path : str | Path
        Caminho para o arquivo CSV contendo os dados de entrada.

    Retorno
    -------
    pd.DataFrame
        DataFrame contendo os dados carregados do arquivo CSV.

    Exceções
    --------
    FileNotFoundError
        Lançada quando o arquivo especificado não existe.

    ValueError
        Lançada quando o arquivo está vazio ou quando o DataFrame
        resultante da leitura não contém registros.

    Observações
    -----------
    Esta função é tipicamente utilizada na etapa de **ingestão de dados**
    de pipelines ETL/ELT, garantindo que apenas arquivos válidos sejam
    processados nas etapas seguintes do pipeline.
    """

    # Converte o caminho recebido para um objeto Path.
    # Isso permite utilizar métodos seguros e portáveis para manipulação
    # de arquivos no sistema operacional.
    path = Path(path)

    # Validação 1: verifica se o arquivo existe no sistema de arquivos.
    # Caso não exista, interrompe o processo imediatamente.
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    # Validação 2: verifica se o arquivo possui tamanho maior que zero.
    # Arquivos vazios indicam falha na coleta ou exportação de dados.
    if path.stat().st_size == 0:
        raise ValueError("Arquivo CSV está vazio.")

    try:
        # Registra no log o início da ingestão de dados.
        # Essa informação é útil para monitoramento e auditoria do pipeline.
        logger.info("Iniciando coleta de dados: %s", path)

        # Realiza a leitura do arquivo CSV utilizando pandas.
        # O resultado é um DataFrame contendo os dados estruturados.
        df = pd.read_csv(path)

        # Validação 3: verifica se o DataFrame resultante contém registros.
        # Mesmo que o arquivo exista, ele pode conter apenas cabeçalho.
        if df.empty:
            raise ValueError("DataFrame vazio após leitura.")

        # Registra no log a quantidade de colunas e a quantidade de registros.
        logger.info("Colunas processadas: %d", len(df.columns))
        logger.info("Registros processados: %d", len(df))

        # Log indicando sucesso na ingestão dos dados.
        logger.info("Coleta concluída com sucesso.")

        # Retorna o DataFrame para uso nas próximas etapas do pipeline.
        return df

    except Exception as e:
        # Registra erro detalhado no log.
        # O parâmetro exc_info=True inclui o stack trace completo,
        # facilitando a investigação de falhas.
        logger.error("Erro ao coletar de dados.", exc_info=True)
        raise    