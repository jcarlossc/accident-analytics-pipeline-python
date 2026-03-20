import pandas as pd
import pytest

from accident_analytics_pipeline_python.cleaning.data_clean import clean_data


def test_clean_data_none():
    """Deve lançar erro quando df é None"""
    with pytest.raises(ValueError, match="Objeto None"):
        clean_data(None)


def test_clean_data_not_dataframe():
    """Deve lançar erro quando entrada não é DataFrame"""
    with pytest.raises(TypeError):
        clean_data("não é dataframe")


def test_clean_data_empty_dataframe():
    """Deve lançar erro quando DataFrame está vazio"""
    df = pd.DataFrame()

    with pytest.raises(ValueError, match="DataFrame vazio"):
        clean_data(df)


def test_clean_data_numeric_only():
    """
    Deve limpar corretamente apenas colunas numéricas existentes
    sem quebrar se outras não existirem.
    """

    df = pd.DataFrame({"auto": ["1", "", " "], "moto": ["2", None, "3"]})

    result = clean_data(df)

    assert result["auto"].tolist() == [1, 0, 0]
    assert result["moto"].tolist() == [2, 0, 3]


def test_clean_data_categorical_only():
    """
    Deve limpar corretamente apenas colunas categóricas existentes
    sem depender de colunas numéricas.
    """

    df = pd.DataFrame(
        {"natureza_acidente": ["", "NA", "colisão"], "bairro": [" ", "-", None]}
    )

    result = clean_data(df)

    assert result["natureza_acidente"].tolist() == [
        "Não informado",
        "Não informado",
        "colisão",
    ]

    assert result["bairro"].tolist() == [
        "Não informado",
        "Não informado",
        "Não informado",
    ]


def test_clean_data_mixed_columns():
    """
    Deve processar corretamente quando existem colunas
    numéricas e categóricas juntas.
    """

    df = pd.DataFrame(
        {
            "auto": ["1", "", "x"],
            "moto": ["2", None, "3"],
            "natureza_acidente": ["", "NA", "colisão"],
            "bairro": ["boa viagem", None, "recife"],
        }
    )

    result = clean_data(df)

    # Numéricas
    assert result["auto"].tolist() == [1, 0, 0]
    assert result["moto"].tolist() == [2, 0, 3]

    # Categóricas
    assert result["natureza_acidente"].tolist() == [
        "Não informado",
        "Não informado",
        "colisão",
    ]

    assert result["bairro"].tolist() == ["boa viagem", "Não informado", "recife"]


def test_clean_data_no_expected_columns():
    """
    Não deve quebrar se nenhuma coluna esperada existir.
    """

    df = pd.DataFrame({"outra_coluna": ["valor"]})

    result = clean_data(df)

    assert "outra_coluna" in result.columns
    assert result["outra_coluna"].iloc[0] == "valor"


def test_clean_data_invalid_numeric_values():
    """
    Valores inválidos devem virar 0.
    """

    df = pd.DataFrame({"auto": ["abc", None, ""]})

    result = clean_data(df)

    assert result["auto"].tolist() == [0, 0, 0]


def test_clean_data_logs(caplog):
    """Verifica se logs são emitidos corretamente"""

    df = pd.DataFrame({"auto": ["1"]})

    with caplog.at_level("INFO"):
        clean_data(df)

    assert "Início da limpeza dos dados" in caplog.text
    assert "Término da limpeza dos dados" in caplog.text
