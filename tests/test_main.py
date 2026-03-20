from unittest.mock import patch
import pandas as pd

def test_main_pipeline():
    with patch("accident_analytics_pipeline_python.main.ingest_data") as mock_ingest, \
         patch("accident_analytics_pipeline_python.main.standardization_data") as mock_std, \
         patch("accident_analytics_pipeline_python.main.clean_data") as mock_clean, \
         patch("accident_analytics_pipeline_python.main.validation_data") as mock_val, \
         patch("accident_analytics_pipeline_python.main.save_csv") as mock_save, \
         patch("accident_analytics_pipeline_python.main.load_all_configs") as mock_config:

        # Config fake
        mock_config.return_value = {
            "paths": {
                "data": {"raw": "input.csv", "processed": "output.csv"},
                "logs": {"file": "log.log"}
            },
            "logging": {"logging": {"level": "INFO", "format": "%(message)s"}},
            "config": {"pipeline": {"fail_on_error": True}}
        }

        df = pd.DataFrame({"a": [1]})

        mock_ingest.return_value = df
        mock_std.return_value = df
        mock_clean.return_value = df

        from accident_analytics_pipeline_python.main import main

        main()

        # Verifica execução do pipeline
        mock_ingest.assert_called_once()
        mock_std.assert_called_once()
        mock_clean.assert_called_once()
        mock_val.assert_called_once()
        mock_save.assert_called_once()