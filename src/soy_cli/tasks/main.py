from soy_cli import logging
from soy_cli.common.asset import BaseAsset

logger = logging.getLogger(__name__)


class MyFirstTransform(BaseAsset):

    def transform(self) -> None:
        """Perform the transformation logic."""
        logger.info(
            "My First Transform: Starting transformation process"
        )

        return None
