import soy_cli.logging
from soy_cli.config.env import env

soy_cli.logging.configure_logging(
    use_json=env.LOG_TO_JSON,
    log_level=env.LOG_LEVEL,
)
