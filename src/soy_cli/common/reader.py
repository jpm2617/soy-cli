
from typing import Any

from soy_cli import logging
from soy_cli.common.models import InputModel
from soy_cli.common.strategies.pandas import pandas_reader
from soy_cli.common.strategies.spark import spark_reader

logger = logging.getLogger(__name__)


class ReaderStrategy:
    """Base class for reader strategies."""

    def read(self, input_config: InputModel, columns: list[str] | None = None, **kwargs) -> Any:
        """Read data using the specific strategy implementation.

        :return: Can return any data format (Spark DataFrame, pandas DataFrame, etc.)
        """
        raise NotImplementedError("Subclasses should implement this method")


class SparkReaderStrategy(ReaderStrategy):
    """Strategy for reading data using Spark."""

    def read(self, input_config: InputModel, columns: list[str] | None = None, **kwargs) -> Any:
        """Read data using Spark reader."""
        from soy_cli.databricks.session import get_databricks_session
        spark_session = get_databricks_session()
        return spark_reader(spark_session, input_config, columns or [], **kwargs)


class PandasReaderStrategy(ReaderStrategy):
    """Strategy for reading data using pandas and converting to Spark DataFrame."""

    def read(self, input_config: InputModel, columns: list[str] | None = None, **kwargs) -> Any:
        """Read data using pandas reader."""
        return pandas_reader(input_config, columns, **kwargs)


class InputManager:
    """Manager class to handle input access and automatic data loading with different strategies."""

    def __init__(self, inputs_config: dict):
        self._inputs = inputs_config
        self._strategies = {
            'spark': SparkReaderStrategy(),
            'pandas': PandasReaderStrategy(),
            # Add more strategies here as needed
            # 'api': APIReaderStrategy(),
            # 'custom': CustomReaderStrategy(),
        }

    def __getitem__(self, key: str):
        """Get an input by key and automatically load data if not already loaded."""
        if key not in self._inputs:
            raise KeyError(f"Input '{key}' not found")

        input_config = InputModel.model_validate(self._inputs[key])

        strategy = input_config.strategy
        if not strategy:
            raise ValueError(
                f"Input configuration for '{key}' does not specify a strategy. Available strategies: {list(self._strategies.keys())}"
            )

        # If data is not already loaded, load it using the appropriate strategy
        if input_config._data is None:
            try:
                # Extract parameters for the reader
                if input_config.api:
                    self._load_data(key, input_config, strategy)
                else:
                    logger.warning(
                        "No API specified for input '{key}', cannot load data", key=key)

            except Exception as e:
                logger.exception(
                    "Failed to load data for input '{key}': {error}", key=key, error=str(e))
                raise

        return input_config

    def _load_data(self, key: str, input_config: InputModel, strategy: str = 'spark', **kwargs) -> None:
        """Load data using the specified strategy."""
        if strategy not in self._strategies:
            raise ValueError(
                f"Unknown reader strategy: {strategy}. Available strategies: {list(self._strategies.keys())}")

        reader_strategy = self._strategies[strategy]

        try:
            logger.debug(
                "Loading data for input '{key}' using {strategy} strategy", key=key, strategy=strategy)
            data = reader_strategy.read(
                input_config, input_config.columns, **kwargs)
            # Update the input config with loaded data
            input_config._data = data
            logger.info("Successfully loaded data for input '{key}'", key=key)
        except Exception as e:
            logger.exception(
                "Failed to load data for input '{key}': {error}", key=key, error=str(e))
            raise

    def read(self, key: str, columns: list[str] | None = None, **kwargs) -> Any:
        """Read data using the specified strategy.

        :param key: The input configuration key
        :param columns: Optional list of columns to select after reading
        :param kwargs: Additional keyword arguments for the reader
        :return: The loaded data (format depends on strategy - Spark DataFrame, pandas DataFrame, etc.)
        """
        if key not in self._inputs:
            raise KeyError(f"Input '{key}' not found")

        input_config = InputModel.model_validate(self._inputs[key])

        if not input_config.strategy:
            raise ValueError(
                f"Input configuration for '{key}' does not specify a strategy. Available strategies: {list(self._strategies.keys())}"
            )

        if input_config.strategy not in self._strategies:
            raise ValueError(
                f"Unknown reader strategy: {input_config.strategy}. Available strategies: {list(self._strategies.keys())}"
            )

        reader_strategy: ReaderStrategy = self._strategies[input_config.strategy]

        try:
            logger.debug(
                "Reading data for input '{key}' using {strategy} strategy", key=key, strategy=input_config.strategy)
            data = reader_strategy.read(input_config, columns, **kwargs)
        except Exception as e:
            logger.exception(
                "Failed to read data for input '{key}': {error}", key=key, error=str(e))
            raise
        else:
            logger.info("Successfully read data for input '{key}'", key=key)
            return data

    def add_strategy(self, name: str, strategy: ReaderStrategy) -> None:
        """Add a custom reader strategy."""
        self._strategies[name] = strategy
        logger.info("Added reader strategy: {name}", name=name)

    def keys(self):
        """Return the keys of all inputs."""
        return self._inputs.keys()

    def items(self):
        """Return items of all inputs."""
        return self._inputs.items()

    def values(self):
        """Return values of all inputs."""
        return self._inputs.values()

    def get_data(self, key: str):
        """Get the loaded data for a specific input key."""
        input_config = self[key]  # This will trigger loading if not already loaded
        return input_config._data
