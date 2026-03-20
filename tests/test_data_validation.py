import pandas as pd
import pytest
from datetime import date, time

from accident_analytics_pipeline_python.validation.data_validation import (
    validation_data,
)


def test_validation_success():
    """Deve passar quando os dados estão válidos"""

    df = pd.DataFrame(
        {
            "data": [date(2024, 1, 1)],
            "hora": [time(12, 0, 0)],
            "auto": [1],
            "natureza_acidente": ["colisão"],
        }
    )

    # Não deve lançar erro
    validation_data(df)


def test_validation_invalid_date(caplog):
    """Valores inválidos em 'data' devem ser logados"""

    df = pd.DataFrame(
        {"data": ["2024-01-01"], "hora": [time(12, 0, 0)]}  # string inválida
    )

    with caplog.at_level("INFO"):
        validation_data(df)

    assert "Existem valores inválidos" in caplog.text


def test_validation_invalid_time(caplog):
    """Valores inválidos em 'hora' devem ser logados"""

    df = pd.DataFrame(
        {"data": [date(2024, 1, 1)], "hora": ["12:00:00"]}  # string inválida
    )

    with caplog.at_level("INFO"):
        validation_data(df)

    assert "Existem valores inválidos" in caplog.text


def test_validation_non_numeric_column():
    """Deve lançar erro quando coluna numérica não é numérica"""

    df = pd.DataFrame(
        {"auto": ["a", "b"], "natureza_acidente": ["teste", "teste"]}  # inválido
    )

    with pytest.raises(ValueError, match="não é numérica"):
        validation_data(df)


def test_validation_null_in_text_column():
    """Deve lançar erro se coluna textual tiver NA"""

    df = pd.DataFrame({"natureza_acidente": [None]})

    with pytest.raises(ValueError, match="contém valores NA"):
        validation_data(df)


def test_validation_missing_columns():
    """Não deve falhar se colunas opcionais não existirem"""

    df = pd.DataFrame({"coluna_qualquer": [1]})

    # Não deve lançar erro
    validation_data(df)


def test_validation_logs(caplog):
    """Verifica logs de execução"""

    df = pd.DataFrame(
        {
            "data": [date(2024, 1, 1)],
            "hora": [time(12, 0, 0)],
            "auto": [1],
            "natureza_acidente": ["teste"],
        }
    )

    with caplog.at_level("INFO"):
        validation_data(df)

    assert "Iniciando validação dos dados" in caplog.text
    assert "Validação concluída com sucesso" in caplog.text
