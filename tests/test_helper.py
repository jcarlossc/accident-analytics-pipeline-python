import pandas as pd
from accident_analytics_pipeline_python.utils.helpers.helper import save_csv

def test_save_csv(tmp_path):
    df = pd.DataFrame({"a": [1, 2]})
    file = tmp_path / "out.csv"

    save_csv(df, file)

    assert file.exists()