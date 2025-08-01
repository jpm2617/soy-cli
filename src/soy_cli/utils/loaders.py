import importlib.util
import inspect
import os
import sys
from typing import Any

import jinja2
import yaml


def safe_load_yaml(file_path: str) -> Any:
    """Load a YAML file safely.

    :param str file_path: The path to the YAML file.
    :return Any: The loaded YAML data.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path) as f:
        content = yaml.safe_load(f)
    return content


def render_configs_with_jinja(
    configs: str | dict[str, str], vars_to_render: dict[str, Any]
) -> str | Any:
    """Render a dictionary of configurations with Jinja2.

    :param Union[str, dict[str, any]] configs: The configurations to render.
    :param dict[str, Any] vars_to_render: The variables to render the configurations with.
    :return dict[str, Any]: The rendered configurations.
    :raises ValueError: If a variable in the template is not defined in vars_to_render.
    """
    # Convert the template YAML data to a string
    template_str = yaml.dump(configs) if isinstance(configs, dict) else configs

    # Create a Jinja2 environment that raises errors for undefined variables
    env = jinja2.Environment(
        loader=jinja2.BaseLoader(),
        autoescape=True,
        undefined=jinja2.StrictUndefined
    )

    # Load the template string into the environment
    template = env.from_string(template_str)

    # Render the template with the data YAML data
    try:
        rendered_template_str = template.render(vars_to_render)
    except jinja2.UndefinedError as e:
        raise ValueError(f"Missing variable in template: {e}") from e

    # Convert the rendered YAML string back to a Python dictionary
    if isinstance(configs, dict):
        rendered_template_yaml = yaml.safe_load(rendered_template_str)
        rendered_template_str = rendered_template_yaml

    return rendered_template_str


def load_module_from_path(
    module_name: str,
    module_file_path: str
):
    """Load a module from a file path.

    :param str module_name: The name to be assigned to the taken on the module.
    :param str module_file_path: The file path to the module.
    """
    try:
        spec = importlib.util.spec_from_file_location(
            module_name,
            module_file_path
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    except Exception as e:
        raise Exception(
            f"Failed to load module {module_name} from {module_file_path}: {e}"
        ) from e
    return module


def load_class_from_module(module_name: str):
    """Load the class object from the module."""
    obj_list = [m[0] for m in inspect.getmembers(
        module_name, inspect.isclass) if m[1].__module__ == module_name.__name__]
    if len(obj_list) > 1:
        raise Exception(
            """
            Asset main.py file contains more than one class asset. \n 
            Please break it into multiple assets.
            """
        )

    class_name = obj_list[0]
    class_obj = getattr(module_name, class_name)
    return class_obj
