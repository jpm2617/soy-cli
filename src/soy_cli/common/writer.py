from typing import Any

from soy_cli import logging
from soy_cli.common.models import OutputModel
from soy_cli.common.strategies.pandas import pandas_writer
from soy_cli.common.strategies.spark import spark_writer

IO_FILE = "io.yaml"
SCHEMA_FILE = "schema.yaml"
TRANSFORM_FILE = "main.py"

logger = logging.getLogger(__name__)


class WriterStrategy:
    """Base class for writer strategies."""

    def write(self, output_config: OutputModel, data: Any, columns: list[str] | None = None, **kwargs) -> None:
        """Write data using the specific strategy implementation.

        :param data: Can accept any data format (Spark DataFrame, pandas DataFrame, etc.)
        """
        raise NotImplementedError("Subclasses should implement this method")


class SparkWriterStrategy(WriterStrategy):
    """Strategy for writing data using Spark."""

    def write(self, output_config: OutputModel, data: Any, columns: list[str] | None = None, **kwargs) -> None:
        """Write data using Spark writer."""
        return spark_writer(output_config, data, columns, **kwargs)


class PandasWriterStrategy(WriterStrategy):
    """Strategy for writing data using pandas."""

    def write(self, output_config: OutputModel, data: Any, columns: list[str] | None = None, **kwargs) -> None:
        """Write data using pandas writer."""
        return pandas_writer(output_config, data, columns, **kwargs)


class OutputManager:
    """Manager class to handle output writing with different strategies."""

    def __init__(self, outputs_config: dict):
        self._outputs = outputs_config
        self._strategies = {
            'spark': SparkWriterStrategy(),
            'pandas': PandasWriterStrategy(),
            # Add more strategies here as needed
            # 'custom': CustomWriterStrategy(),
        }

    def __getitem__(self, key: str):
        """Get an output configuration by key."""
        if key not in self._outputs:
            raise KeyError(f"Output '{key}' not found")
        return OutputModel.model_validate(self._outputs[key])

    def write(self, key: str, data: Any, columns: list[str] | None = None, **kwargs) -> None:
        """Write data using the specified strategy.

        :param key: The output configuration key
        :param data: The data to write (format depends on strategy - Spark DataFrame, pandas DataFrame, etc.)
        :param columns: Optional list of columns to select before writing
        :param strategy: The writer strategy to use (default: 'spark')
        :param kwargs: Additional keyword arguments for the writer
        """
        output_config = OutputModel.model_validate(self[key])
        if output_config.strategy not in self._strategies:
            raise ValueError(
                f"Unknown writer strategy: {output_config.strategy}. Available strategies: {list(self._strategies.keys())}")

        if not output_config.strategy:
            raise ValueError(
                f"Output configuration for '{key}' does not specify a strategy. Available strategies: {list(self._strategies.keys())}"
            )
        writer_strategy: WriterStrategy = self._strategies[output_config.strategy]

        try:
            logger.debug(
                "Writing data for output '{key}' using {strategy} strategy", key=key, strategy=output_config.strategy)
            writer_strategy.write(output_config, data, columns, **kwargs)
            logger.info("Successfully wrote data for output '{key}'", key=key, strategy=output_config.strategy,
                        api=output_config.api, args=output_config.args, columns=output_config.columns if output_config.columns else "'*'")
        except Exception as e:
            logger.exception(
                "Failed to write data for output '{key}': {error}", key=key, error=str(e))
            raise

    def add_strategy(self, name: str, strategy: WriterStrategy) -> None:
        """Add a custom writer strategy."""
        self._strategies[name] = strategy
        logger.info("Added writer strategy: {name}", name=name)

    def keys(self):
        """Return the keys of all outputs."""
        return self._outputs.keys()

    def items(self):
        """Return items of all outputs."""
        return self._outputs.items()

    def values(self):
        """Return values of all outputs."""
        return self._outputs.values()
