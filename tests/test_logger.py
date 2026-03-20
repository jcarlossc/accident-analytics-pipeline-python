from accident_analytics_pipeline_python.utils.loggers.logger import setup_logger

def test_logger_setup(tmp_path):
    config = {
        "logging": {
            "level": "INFO",
            "format": "%(message)s"
        }
    }

    log_file = tmp_path / "log.log"

    setup_logger(config, log_file)

    assert log_file.parent.exists()