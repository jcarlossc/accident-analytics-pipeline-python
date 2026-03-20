import pandas as pd
import pytest

from accident_analytics_pipeline_python.standardization.data_standardization import (
    standardization_data,
)


def test_standardization_none():
    """Deve lançar erro quando df é None"""
    with pytest.raises(ValueError, match="Objeto None"):
        standardization_data(None)


def test_standardization_not_dataframe():
    """Deve lançar erro quando entrada não é DataFrame"""
    with pytest.raises(TypeError, match="não é um pandas DataFrame"):
        standardization_data("isso não é df")


def test_standardization_empty_dataframe():
    """Deve lançar erro para DataFrame vazio"""
    df = pd.DataFrame()

    with pytest.raises(ValueError, match="DataFrame vazio"):
        standardization_data(df)


def test_standardization_success():
    """Testa padronização completa de dados"""

    df = pd.DataFrame(
        {
            " Data ": ["2024-01-01"],
            "Hora": ["12:30:00"],
            "Bairro": ["boa viagem"],
            "Natureza Acidente": ["colisão"],
        }
    )

    result = standardization_data(df)

    # Colunas padronizadas
    assert "data" in result.columns
    assert "hora" in result.columns
    assert "bairro" in result.columns
    assert "natureza_acidente" in result.columns

    # Conversão de data
    assert str(result["data"].iloc[0]) == "2024-01-01"

    # Conversão de hora
    assert result["hora"].iloc[0].strftime("%H:%M:%S") == "12:30:00"

    # Title case
    assert result["bairro"].iloc[0] == "Boa Viagem"

    # Capitalize
    assert result["natureza_acidente"].iloc[0] == "Colisão"


def test_standardization_missing_columns():
    """Não deve falhar se colunas opcionais não existirem"""

    df = pd.DataFrame({"outra_coluna": ["valor"]})

    result = standardization_data(df)

    assert "outra_coluna" in result.columns
    assert result["outra_coluna"].iloc[0] == "valor"


def test_standardization_invalid_date_and_time():
    """Valores inválidos devem virar NaT/NaN"""

    df = pd.DataFrame({"data": ["data_invalida"], "hora": ["99:99:99"]})

    result = standardization_data(df)

    assert pd.isna(result["data"].iloc[0])
    assert pd.isna(result["hora"].iloc[0])


def test_standardization_logs(caplog):
    """Verifica se logs são emitidos"""

    df = pd.DataFrame({"data": ["2024-01-01"]})

    with caplog.at_level("INFO"):
        standardization_data(df)

    assert "Início da padronização" in caplog.text
    assert "Padronização concluída com sucesso" in caplog.text
