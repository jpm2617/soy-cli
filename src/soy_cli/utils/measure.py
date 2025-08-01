import time
import typing
from functools import wraps

from soy_cli import logging

logger = logging.getLogger(__name__)


def timing_decorator(func: typing.Callable) -> typing.Callable:
    """Measure and log function execution time."""
    @wraps(func)
    def _timming_decorator_wrapper(*args: list[typing.Any], **kwargs: dict[str, typing.Any]) -> typing.Any:
        start_time = time.time()

        post_logger = logger.bind(func_args=list(
            args), func_kwargs=kwargs, func_name=func.__name__)
        try:
            result = func(*args, **kwargs)
        except Exception:
            end_time = time.time()
            execution_time = end_time - start_time
            post_logger.exception(
                "Failed to execute function", execution_time=execution_time)
            raise
        else:
            end_time = time.time()
            execution_time = end_time - start_time
            post_logger.debug(
                "Completed function {execution_time}",
                execution_time=execution_time,
            )
            return result
    return _timming_decorator_wrapper
