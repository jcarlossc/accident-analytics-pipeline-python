import logging
from typing import Dict, Any

def setup_logger(logging_config: Dict[str, Any], log_file: str) -> None:
    level = getattr(logging, logging_config["logging"]["level"], logging.INFO)

    logging.basicConfig(
        level = level,
        format = logging_config["logging"]["format"],
        handlers = [
            logging.FileHandler(log_file, encoding = "utf-8"),
            logging.StreamHandler()
        ]
    ) 