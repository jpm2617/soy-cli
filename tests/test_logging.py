import json

import pytest
from pytest import CaptureFixture

import soy_cli.logging as soy_cli_logging


# --- Helpers ---
def get_log_output(capfd: CaptureFixture):
    out, err = capfd.readouterr().out, capfd.readouterr().err
    output = out.strip() if out.strip() else err.strip()
    if not output:
        raise AssertionError("No log output captured in either stdout or stderr.")
    return output


# --- Basic log output test (parameterized) ---
@pytest.mark.parametrize(
    "method, expected_level",
    [
        ("debug", "debug"),
        ("info", "info"),
        ("warning", "warning"),
        ("error", "error"),
        ("critical", "critical"),
    ],
)
@pytest.mark.parametrize("use_json", [True, False])
def test_log_output_levels_and_formats(capfd, method, expected_level, use_json):
    """Test log output for each level and format (JSON/console)."""
    soy_cli_logging.configure_logging(use_json=use_json, log_level="DEBUG")
    logger = soy_cli_logging.getLogger(__name__)
    getattr(logger, method)("Hello, structured {key}!", key="logging", key1="value1", key2="value2")
    output = get_log_output(capfd)
    if use_json:
        parsed = json.loads(output)
        assert parsed["event"] == "Hello, structured logging!"
        assert parsed["key"] == "logging"
        assert parsed["key1"] == "value1"
        assert parsed["key2"] == "value2"
        assert parsed["level"] == expected_level
        assert "timestamp" in parsed

        # Check for debug fields only when level is debug
        if expected_level == "debug":
            assert "lineno" in parsed
            assert "module" in parsed
            assert "func_name" in parsed
            assert isinstance(parsed["lineno"], int)
            assert isinstance(parsed["module"], str)
            assert isinstance(parsed["func_name"], str)
    else:
        assert "Hello, structured logging!" in output
        assert "key1" in output and "value1" in output


# --- Edge case and unique behavior tests ---
def test_string_interpolation_missing_placeholder(capfd):
    """Test fallback when a placeholder is missing from extra data."""
    soy_cli_logging.configure_logging(use_json=True, log_level="DEBUG")
    logger = soy_cli_logging.getLogger(__name__)
    logger.info("User {user_id} performed {action}", user_id=123)
    parsed = json.loads(get_log_output(capfd))
    assert parsed["event"] == "User 123 performed {action}"
    assert parsed["user_id"] == 123


def test_string_interpolation_invalid_format(capfd):
    """Test fallback when format string is invalid."""
    soy_cli_logging.configure_logging(use_json=True, log_level="DEBUG")
    logger = soy_cli_logging.getLogger(__name__)
    logger.info("Invalid format {incomplete", user_id=123)
    parsed = json.loads(get_log_output(capfd))
    assert parsed["event"] == "Invalid format {incomplete"
    assert parsed["user_id"] == 123


@pytest.mark.parametrize(
    "msg_pattern",
    [
        "Processing %d items",
        "User %(name)s logged in",
        "Temperature is %f degrees",
        "Error code: %x",
        "Found %r in data",
        "Character: %c",
        "Multiple %s and %d patterns",
    ],
)
def test_logger_with_old_interpolation_patterns(msg_pattern):
    """Test that old-style interpolation patterns raise TypeError."""
    soy_cli_logging.configure_logging(use_json=True, log_level="DEBUG")
    logger = soy_cli_logging.getLogger(__name__)
    with pytest.raises(TypeError, match="Old-style string interpolation detected"):
        logger.info(msg_pattern, key="value")


def test_logger_bind_context_propagation(capfd):
    """Test that logger.bind() context is propagated to log output."""
    soy_cli_logging.configure_logging(use_json=True, log_level="DEBUG")
    logger = soy_cli_logging.getLogger(__name__)
    bound_logger = logger.bind(user_id=123, session_id="abc123")
    bound_logger.info("User action", action="login")
    parsed = json.loads(get_log_output(capfd))
    assert parsed["user_id"] == 123
    assert parsed["session_id"] == "abc123"
    assert parsed["action"] == "login"
    assert parsed["event"] == "User action"


def test_exception_logging_json(capfd):
    """Test exception logging with JSON formatting."""
    soy_cli_logging.configure_logging(use_json=True, log_level="DEBUG")
    logger = soy_cli_logging.getLogger(__name__)
    try:
        raise ValueError("Test exception")  # noqa: TRY301
    except ValueError:
        logger.exception("An error occurred", error_code=500)
    parsed = json.loads(get_log_output(capfd))
    assert parsed["event"] == "An error occurred"
    assert parsed["error_code"] == 500
    assert parsed["level"] == "error"


def test_extra_parameter_merging(capfd):
    """Test that extra parameters are merged correctly."""
    soy_cli_logging.configure_logging(use_json=True, log_level="DEBUG")
    logger = soy_cli_logging.getLogger(__name__)
    existing_extra = {"existing_key": "existing_value"}
    logger.info("Test message", extra=existing_extra, new_key="new_value")
    parsed = json.loads(get_log_output(capfd))
    assert parsed["existing_key"] == "existing_value"
    assert parsed["new_key"] == "new_value"


def test_complex_data_types_in_extra(capfd):
    """Test logging with complex data types in extra parameters."""
    soy_cli_logging.configure_logging(use_json=True, log_level="DEBUG")
    logger = soy_cli_logging.getLogger(__name__)
    complex_data = {
        "list_data": [1, 2, 3],
        "dict_data": {"nested": "value"},
        "none_value": None,
        "bool_value": True,
    }
    logger.info("Complex data test", **complex_data)
    parsed = json.loads(get_log_output(capfd))
    assert parsed["list_data"] == [1, 2, 3]
    assert parsed["dict_data"] == {"nested": "value"}
    assert parsed["none_value"] is None
    assert parsed["bool_value"] is True


def test_empty_message(capfd):
    """Test logging with an empty message."""
    soy_cli_logging.configure_logging(use_json=True, log_level="DEBUG")
    logger = soy_cli_logging.getLogger(__name__)
    logger.info("", key="value")
    parsed = json.loads(get_log_output(capfd))
    assert parsed["event"] == ""
    assert parsed["key"] == "value"


def test_log_level_filtering(capfd):
    """Test that messages below configured log level are filtered out."""
    soy_cli_logging.configure_logging(use_json=True, log_level="WARNING")
    logger = soy_cli_logging.getLogger(__name__)
    logger.debug("This should not appear 1")
    logger.info("This should not appear 2")
    logger.warning("This should appear")

    out, err = capfd.readouterr().out, capfd.readouterr().err
    output = out.strip() if out.strip() else err.strip()

    # Should only contain the warning message
    assert "This should appear" in output
    assert "This should not appear 1" not in output
    assert "This should not appear 2" not in output


def test_logger_new_context_propagation(capfd):
    """Test that logger.new() creates a new logger with additional context."""
    soy_cli_logging.configure_logging(use_json=True, log_level="DEBUG")
    logger = soy_cli_logging.getLogger(__name__)
    new_logger = logger.new(user_id=123, session_id="abc123")
    new_logger.info("User action", action="login")
    parsed = json.loads(get_log_output(capfd))
    assert parsed["user_id"] == 123
    assert parsed["session_id"] == "abc123"
    assert parsed["action"] == "login"
    assert parsed["event"] == "User action"
