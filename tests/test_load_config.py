from accident_analytics_pipeline_python.utils.load_config.load_config import load_all_configs

def test_load_all_configs(tmp_path):
    file = tmp_path / "config.yaml"
    file.write_text("test: 123")

    configs = load_all_configs(tmp_path)

    assert "config" in configs
    assert configs["config"]["test"] == 123