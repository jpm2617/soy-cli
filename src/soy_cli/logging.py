import re
import string
import typing
from typing import Any

import structlog
import structlog._frames
from structlog.processors import ExceptionDictTransformer, ExceptionRenderer
from structlog.typing import Processor

from soy_cli.config.env import env
from soy_cli.utils import convert_snake_case_to_camel_case, get_package_name

PACKAGE_NAME = get_package_name()
CAMEL_CASE_PACKAGE_NAME = convert_snake_case_to_camel_case(PACKAGE_NAME)

# Global logger cache
_logger_cache: dict[str, structlog.BoundLogger] = {}


class StructuredLogger:
    """A structured logger that enforces keyword-only arguments and provides string interpolation."""

    def __init__(self, logger: structlog.BoundLogger):
        self._logger = logger

    def debug(self, msg: str, **kwargs: Any) -> None:
        """Log a debug message with structured data."""
        self._log("debug", msg, **kwargs)

    def info(self, msg: str, **kwargs: Any) -> None:
        """Log an info message with structured data."""
        self._log("info", msg, **kwargs)

    def warning(self, msg: str, **kwargs: Any) -> None:
        """Log a warning message with structured data."""
        self._log("warning", msg, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        """Log an error message with structured data."""
        self._log("error", msg, **kwargs)

    def critical(self, msg: str, **kwargs: Any) -> None:
        """Log a critical message with structured data."""
        self._log("critical", msg, **kwargs)

    def exception(self, msg: str, **kwargs: Any) -> None:
        """Log an exception message with structured data."""
        kwargs["exc_info"] = True
        self._log("error", msg, **kwargs)

    def log(self, level: int | str, msg: str, **kwargs: Any) -> None:
        """Log a message at the specified level with structured data."""
        self._log(level, msg, **kwargs)

    def bind(self, **kwargs: Any) -> "StructuredLogger":
        """Bind additional context to the logger."""
        bound_logger = self._logger.bind(**kwargs)
        return StructuredLogger(bound_logger)

    def new(self, **kwargs: Any) -> "StructuredLogger":
        """Create a new logger with additional context."""
        new_logger = self._logger.new(**kwargs)
        return StructuredLogger(new_logger)

    def _log(self, level: int | str, msg: str, **kwargs: Any) -> None:
        # Merge 'extra' dict if present
        extra = kwargs.pop("extra", None)
        if extra and isinstance(extra, dict):
            kwargs = {**extra, **kwargs}
        # Check for old-style string interpolation patterns
        if _detect_old_style_interpolation(msg):
            raise TypeError(
                f"Old-style string interpolation detected in log message: '{msg}'. "
                "Please use structured logging with keyword arguments instead. "
                "Example: logger.info('User {user_id} logged in', user_id=123)"
            )

        # Handle string interpolation
        interpolated_msg, remaining_kwargs = _interpolate_message(msg, kwargs)

        # Get the appropriate log method
        log_method = getattr(self._logger, str(
            level).lower(), self._logger.info)

        # Log with remaining kwargs as structured data
        log_method(interpolated_msg, **remaining_kwargs)

    # Delegate all other methods to the underlying structlog logger
    def __getattr__(self, name: str) -> Any:
        """Delegate unknown attributes to the underlying structlog logger."""
        return getattr(self._logger, name)


def _detect_old_style_interpolation(msg: str) -> bool:
    """Detect old-style string interpolation patterns like %s, %d, %(name)s, etc.

    :param msg: The log message to check for old-style interpolation patterns.
    :return: bool: True if old-style interpolation patterns are found, False otherwise.
    """
    # Pattern to match old-style string interpolation
    # Matches: %s, %d, %f, %r, %c, %x, %o, %(name)s, %(key)d, etc.
    old_style_pattern = r"%(?:\([^)]+\))?[sdfrcoxi%]"
    return bool(re.search(old_style_pattern, msg))


def _interpolate_message(msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Interpolate only the placeholders for which a value is provided, leave others as {placeholder}."""

    class SafeDict(dict):
        def __missing__(self, key):
            return "{" + key + "}"

    try:
        interpolated_msg = string.Formatter().vformat(msg, (), SafeDict(**kwargs))
    except Exception:
        interpolated_msg = msg
    return interpolated_msg, kwargs


def patch_module_name(_: structlog.BoundLogger, __: str, event_dict: dict[str, typing.Any]) -> dict[str, typing.Any]:
    """Patch the module name in the event dictionary.

    In the structlog library, the module name is given by the file name of the callsite.
    In here we patch it to use the module name instead of the file name.
    THIS FUNCTION OVERRIDES THE DEFAULT BEHAVIOR OF STRUCTLOG FOR MODULE NAME CAPTURING.

    :param structlog.BoundLogger logger:
    :param str name: _ignored_ (not used in this function)
    :param dict[str, typing.Any] event_dict: Event dictionary containing log information.
    :return dict[str, typing.Any]: Event dictionary with the module name patched.

    Examples
    --------
    If a module is within a package, the module name will be the full package path.
    Returns module as "example_package.example_module" instead of "example_module".

    """
    # noinspection PyProtectedMember
    f, module_str = structlog._frames._find_first_app_frame_and_name(
        additional_ignores=[__name__])
    # frame has filename, caller and line number
    event_dict["module"] = module_str
    return event_dict


def configure_logging(use_json: bool | None = None, log_level: str | int = "INFO") -> None:
    """Configure structlog with custom processors and renderers.

    This logger is configured to enforce structured logging practices,
    ensuring that all log messages are passed as keyword arguments only.
    """
    # Convert string log level to numeric value if needed
    if isinstance(log_level, str):
        numeric_log_level = structlog.stdlib.NAME_TO_LEVEL.get(
            log_level.lower())
        if numeric_log_level is None:
            raise ValueError(f"Invalid log level: {log_level}")
        log_level = numeric_log_level

    shared_processors: list[Processor] = [
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.contextvars.merge_contextvars,
    ]

    if log_level < 20:
        shared_processors += [
            structlog.processors.CallsiteParameterAdder(
                [
                    structlog.processors.CallsiteParameter.LINENO,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.MODULE,
                ],
                additional_ignores=[__name__],
            ),
            patch_module_name,  # type: ignore[list-item]
        ]

    renderers: list[Processor]
    if use_json:
        renderers = [
            ExceptionRenderer(ExceptionDictTransformer(show_locals=False)),
            structlog.processors.JSONRenderer(),
        ]
    else:
        renderers = [
            structlog.dev.ConsoleRenderer(
                colors=True,
                level_styles={
                    "debug": "\033[36m",  # Cyan
                    "info": "\033[32m",  # Green
                    "warning": "\033[33m",  # Yellow
                    "error": "\033[31m",  # Red
                    "critical": "\033[41m",  # Red background
                    "exception": "\033[35m",  # Magenta
                },
            )
        ]

    # Clear the logger cache to ensure new loggers use the updated configuration
    _logger_cache.clear()

    # Configure structlog
    structlog.configure(
        processors=[*shared_processors, *renderers],
        cache_logger_on_first_use=True,
        wrapper_class=structlog.make_filtering_bound_logger(
            min_level=log_level),
    )


def getLogger(name: str) -> StructuredLogger:
    """Get a structured logger instance.

    This function returns a StructuredLogger that enforces keyword-only arguments
    and provides automatic string interpolation for placeholders in log messages.

    Examples
    --------
    Basic usage with keyword arguments:
        logger = getLogger(__name__)
        logger.info("User logged in", user_id=123, session_id="abc123")
        # Output: "User logged in" with structured data: user_id=123, session_id="abc123"

    String interpolation with placeholders:
        logger.info("User {user_id} performed action {action}",
                   user_id=123, action="login", extra_data="value")
        # Output: "User 123 performed action login" with structured data: extra_data="value"

    Multiple placeholders:
        logger.warning("Database query took {duration}ms for table {table}",
                      duration=250, table="users", query_id="abc123")
        # Output: "Database query took 250ms for table users" with structured data: query_id="abc123"

    Missing placeholder (safe fallback):
        logger.error("Error in {module} at {timestamp}",
                    module="auth", other_data="value")
        # Output: "Error in auth at {timestamp}" with structured data: other_data="value"

    With nested data:
        logger.info("Request {method} {path} returned {status}",
                   method="POST", path="/api/users", status=201, request_id="req123")
        # Output: "Request POST /api/users returned 201" with structured data: request_id="req123"

    Error handling - invalid format string:
        logger.info("Invalid format {", key="value")
        # Output: "Invalid format {" with structured data: key="value"

    No placeholders:
        logger.info("Simple message without placeholders", extra_data="value")
        # Output: "Simple message without placeholders" with structured data: extra_data="value"

    Exception logging:
        try:
            raise ValueError("Something went wrong")
        except ValueError:
            logger.exception("An error occurred", error_code=500, user_id=123)
            # Output: "An error occurred" with structured data: error_code=500, user_id=123
            # and full exception traceback

    Using bind() for context:
        logger = getLogger(__name__)
        bound_logger = logger.bind(user_id=123, session_id="abc123")
        bound_logger.info("User action", action="login")
        # Output: "User action" with structured data: user_id=123, session_id="abc123, action="login"
    """
    if name not in _logger_cache:
        # Get the structlog logger
        structlog_logger = structlog.get_logger(name)
        # Wrap it in our StructuredLogger
        _logger_cache[name] = StructuredLogger(structlog_logger)

    return _logger_cache[name]
