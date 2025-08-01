import os
from typing import Any

from soy_cli import logging
from soy_cli.common.models import InputModel, OutputModel
from soy_cli.common.reader import InputManager
from soy_cli.common.writer import OutputManager
from soy_cli.config.env import env
from soy_cli.utils.loaders import (
    load_class_from_module,
    load_module_from_path,
    render_configs_with_jinja,
    safe_load_yaml,
)
from pydantic import BaseModel, Field

IO_FILE = "io.yaml"
SCHEMA_FILE = "schema.yaml"
TRANSFORM_FILE = "main.py"

logger = logging.getLogger(__name__)


class BaseAssetConfig(BaseModel):
    name: str = None
    inputs: list[InputModel] = Field(default_factory=list)
    outputs: list[OutputModel] = Field(default_factory=list)
    context: dict = Field(default_factory=dict)

    def get_inputs_dict(self) -> dict:
        """Get inputs as a dictionary for easier access by name."""
        return {_input.name: _input.model_dump() for _input in self.inputs}

    def get_outputs_dict(self) -> dict:
        """Get outputs as a dictionary for easier access by name."""
        return {_output.name: _output.model_dump() for _output in self.outputs}

    @classmethod
    def from_file(cls, file_path: str):
        """Create an instance of the asset configuration from a file."""
        configs = safe_load_yaml(file_path)
        rendered_configs = render_configs_with_jinja(
            configs=configs, vars_to_render=env.model_dump())
        return cls(**rendered_configs)


class BaseAsset:
    def __init__(self, config: BaseAssetConfig | None = None):
        """Initialize the base asset with empty inputs and outputs."""
        self._config = config
        self.name = self._config.name
        self.context = self._config.context or {}
        self._inputs = config.get_inputs_dict()
        self._outputs = config.get_outputs_dict()
        self._input_manager = InputManager(self._inputs)
        self._output_manager = OutputManager(self._outputs)
        logger.info("Initialized asset: {name}", name=self.name)

    @property
    def inputs(self):
        """Return the input manager that handles automatic data loading."""
        return self._input_manager

    @property
    def outputs(self):
        """Return the output manager that handles data writing with different strategies."""
        return self._output_manager

    @inputs.setter
    def inputs(self, value):
        """Set the inputs of the asset."""
        if isinstance(value, str):
            value = [value]
        self._inputs = dict(enumerate(value))
        self._input_manager = InputManager(self._inputs)

    def write_output(self, key: str, data: Any, columns: list[str] | None = None, **kwargs) -> None:
        """Write data to an output.

        :param key: The output configuration key
        :param data: The data to write (format depends on strategy - Spark DataFrame, pandas DataFrame, etc.)
        :param columns: Optional list of columns to select before writing
        :param strategy: The writer strategy to use (default: 'spark')
        :param kwargs: Additional keyword arguments for the writer
        """
        self._output_manager.write(key, data, columns, **kwargs)

    def read_input(self, key: str, columns: list[str] | None = None, **kwargs) -> Any:
        """Read data from an input using the specified strategy.

        :param key: The input configuration key
        :param columns: Optional list of columns to select after reading
        :param kwargs: Additional keyword arguments for the reader
        :return: The loaded data (format depends on strategy - Spark DataFrame, pandas DataFrame, etc.)
        """
        return self._input_manager.read(key, columns, **kwargs)

    @classmethod
    def from_file(cls, file_path):
        """Create an instance of the asset from a file."""
        config = BaseAssetConfig.from_file(file_path)
        logger.debug("Loaded asset configuration config: {config}",
                     config={config.model_dump_json()})
        return cls(config=config)

    @classmethod
    def activate(cls, context: dict[str, Any] | None = None) -> "BaseAsset":
        """Create an instance of the asset from the current directory.

        This method automatically determines the directory where the calling class
        is defined and loads the asset configuration from io.yaml file in that directory.
        """
        import inspect

        # Get the directory where the actual subclass is defined (not where start() is called)
        asset_dir = os.path.dirname(os.path.abspath(inspect.getfile(cls)))

        logger.debug(
            "Loading asset from directory: {asset_dir}", asset_dir=asset_dir)

        # Check for required files
        io_file = os.path.join(asset_dir, IO_FILE)

        if not os.path.exists(io_file):
            raise FileNotFoundError(
                f"IO configuration file not found: {io_file}")

        # Load configuration from io.yaml
        config = BaseAssetConfig.from_file(io_file)
        config.context = {**config.context, **(context or {})}

        logger.debug("Loaded asset configuration from root: {config}",
                     config=config.model_dump_json())
        return cls(config=config)

    def transform(self):
        """Transform the data. This method should be implemented by subclasses."""
        raise NotImplementedError("Subclasses should implement this method")


def load_asset(
    path: str,
    context: dict[str, Any] | None = None
) -> BaseAsset:
    """Load an asset from a module folder.

    :param str path: The path to the module folder.
    :param context: Optional context dictionary to pass to the asset.
    :return BaseAsset: The loaded asset class object.
    """
    main_file = os.path.join(path, TRANSFORM_FILE)
    if not os.path.exists(main_file):
        raise FileNotFoundError(
            f"Main transform file '{TRANSFORM_FILE}' not found in path: {path}"
        )

    module_name = path.split("/")[-1]
    asset_module = load_module_from_path(
        module_name=module_name,
        module_file_path=main_file
    )
    logger.debug("Loaded module: {module_name} from {main_file}",
                 module_name=module_name, main_file=main_file)
    class_obj = load_class_from_module(asset_module)
    logger.debug("Loaded class: {class_name} from module: {module_name}",
                 class_name=class_obj.__name__, module_name=module_name)
    class_obj: BaseAsset = class_obj.activate(context=context)
    return class_obj
