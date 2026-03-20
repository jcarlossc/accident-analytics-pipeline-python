from pathlib import Path
import pandas as pd
import logging

# Instância de logger associada ao módulo atual.
# Utilizar __name__ permite que o sistema de logging organize os logs
# de acordo com a hierarquia dos módulos da aplicação.
logger = logging.getLogger(__name__)


def save_csv(df: pd.DataFrame, path: str | Path) -> None:
    """
    Persiste um DataFrame em um arquivo CSV no sistema de arquivos.

    Esta função é responsável por salvar o dataset processado
    em formato CSV, garantindo que o diretório de destino exista
    e que o DataFrame contenha dados válidos antes da gravação.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame contendo os dados que serão persistidos.

    path : str | Path
        Caminho completo para o arquivo CSV de destino.

    Retorno
    -------
    None
        A função não retorna valor. Caso ocorra erro durante o
        processo de salvamento, uma exceção é lançada.

    Observações
    -----------
    Esta função normalmente é utilizada na etapa final de pipelines
    de dados, após processos de ingestão, padronização, limpeza
    e validação do dataset.
    """

    # Registro do início do processo de salvamento.
    # Esse log permite rastrear quando o pipeline inicia
    # a etapa de persistência dos dados.
    logger.info("Iniciando salvamento do arquivo CSV.")

    # Converte o caminho recebido para objeto Path.
    # Isso garante manipulação segura e multiplataforma
    # de caminhos de arquivos.
    path = Path(path)

    # Validação 1: verifica se o DataFrame recebido é None.
    # Essa validação evita tentativas de salvar um objeto inexistente.
    if df is None:
        raise ValueError("DataFrame é None.")

    # Validação 2: verifica se o DataFrame possui registros.
    # Evita a criação de arquivos vazios que podem indicar
    # falhas em etapas anteriores do pipeline.
    if df.empty:
        raise ValueError("DataFrame vazio. Nada para salvar.")

    try:
        # Garante que o diretório de destino exista.
        # Caso o diretório não exista, ele será criado automaticamente.
        # O parâmetro parents=True permite criar toda a hierarquia
        # de diretórios necessária.
        path.parent.mkdir(parents=True, exist_ok=True)

        # Persistência do DataFrame em formato CSV.
        # - index=False evita salvar o índice do pandas no arquivo
        # - encoding="utf-8" garante compatibilidade com sistemas
        #   que utilizam codificação UTF-8.
        df.to_csv(path, index=False, encoding="utf-8")

        # Registro indicando que o arquivo foi salvo com sucesso.
        # O caminho do arquivo é incluído no log para facilitar
        # rastreabilidade e auditoria.
        logger.info("Arquivo CSV salvo com sucesso em: %s", path)

    except Exception:
        # Registro indicando que o arquivo foi salvo com sucesso.
        # O caminho do arquivo é incluído no log para facilitar
        # rastreabilidade e auditoria.
        logger.error("Erro ao salvar arquivo CSV.", exc_info=True)

        # Propaga a exceção para que camadas superiores da aplicação
        raise
