import importlib.util
import os


def get_package_name() -> str:
    """Dynamically determine the package name of the current module."""
    spec = importlib.util.find_spec(__package__ or __name__.split('.')[0])
    if spec and spec.name:
        package_name = spec.name
    else:
        # Fallback to path-based method if importlib fails
        current_file = os.path.abspath(__file__)
        package_dir = os.path.dirname(os.path.dirname(current_file))
        package_name = os.path.basename(package_dir)
    return package_name


def convert_snake_case_to_camel_case(snake_str: str) -> str:
    """Convert a snake_case string to CamelCase."""
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])
