import pandas as pd
import pytest
from pathlib import Path

from accident_analytics_pipeline_python.ingestion.data_ingestion import ingest_data


def test_ingest_data_success(tmp_path: Path):
    """
    Testa coleta bem-sucedida de um CSV válido.
    """

    # Cria um CSV temporário
    file = tmp_path / "data.csv"
    df_expected = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    df_expected.to_csv(file, index=False)

    # Executa função
    df_result = ingest_data(file)

    # Valida resultado
    assert not df_result.empty
    assert list(df_result.columns) == ["col1", "col2"]
    assert len(df_result) == 2


def test_ingest_data_file_not_found():
    """
    Deve lançar FileNotFoundError quando o arquivo não existe.
    """

    with pytest.raises(FileNotFoundError):
        ingest_data("arquivo_inexistente.csv")


def test_ingest_data_empty_file(tmp_path: Path):
    """
    Deve lançar erro quando o arquivo está vazio (0 bytes).
    """

    file = tmp_path / "empty.csv"
    file.touch()  # cria arquivo vazio

    with pytest.raises(ValueError, match="Arquivo CSV está vazio"):
        ingest_data(file)


def test_ingest_data_only_header(tmp_path: Path):
    """
    Deve lançar erro quando o CSV possui apenas cabeçalho.
    """

    file = tmp_path / "header_only.csv"

    # CSV com apenas header
    file.write_text("col1,col2\n")

    with pytest.raises(ValueError, match="DataFrame vazio"):
        ingest_data(file)


def test_ingest_data_invalid_format(tmp_path: Path):
    """
    Testa comportamento com arquivo inválido (não CSV).
    """

    file = tmp_path / "invalid.csv"

    # Conteúdo inválido
    file.write_text(",,,,,,,")

    # Pode gerar erro do pandas ou passar dependendo do parser
    # Aqui validamos que não retorna vazio silenciosamente
    with pytest.raises(Exception):
        ingest_data(file)


def test_ingest_data_logs(tmp_path: Path, caplog):
    """
    Testa se logs são emitidos corretamente.
    """

    file = tmp_path / "data.csv"

    df_expected = pd.DataFrame({"col1": [1], "col2": ["a"]})
    df_expected.to_csv(file, index=False)

    with caplog.at_level("INFO"):
        ingest_data(file)

    # Verifica mensagens de log
    assert "Iniciando coleta de dados" in caplog.text
    assert "Colunas processadas" in caplog.text
    assert "Registros processados" in caplog.text
    assert "Coleta concluída com sucesso" in caplog.text
