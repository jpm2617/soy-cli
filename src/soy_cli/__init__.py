import os

import soy_cli.logging
from soy_cli.config.env import env

soy_cli.logging.configure_logging(
    use_json=os.getenv("LOG_TO_JSON", env.LOG_TO_JSON),
    log_level=os.getenv("LOG_LEVEL", env.LOG_LEVEL)
)