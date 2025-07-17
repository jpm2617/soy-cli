"""Pytest configuration for structured test output with clear log isolation."""
import logging
from io import StringIO

import pytest


class TestStructurePlugin:
    """Plugin to structure test output with clear separation."""

    def __init__(self):
        self.current_test = None
        self.log_capture = None

    def _print_test_header(self, item):
        """Print test start banner."""
        # test_name = f"{item.cls.__name__ if item.cls else 'Module'}::{item.name}"
        print(f"{'='*80}")

    def _setup_log_capture(self):
        """Set up log capture for application logs."""
        import sys

        # Capture both stdout and stderr to catch all log output
        self.log_capture = StringIO()
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr

        # Redirect stderr (where structlog usually outputs) to our capture
        sys.stderr = self.log_capture

        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]

        return original_handlers

    def _restore_streams(self, original_handlers):
        """Restore original stdout/stderr and handlers."""
        import sys

        # Restore original streams
        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr

        # Restore original handlers
        root_logger = logging.getLogger()
        root_logger.handlers = original_handlers

    def _print_logs_and_result(self, result):
        """Print application logs and test result."""
        log_content = self.log_capture.getvalue().strip()
        if log_content:
            print("\n\n📋 APPLICATION LOGS:")
            print("-" * 40)
            for line in log_content.split('\n'):
                if line.strip():
                    print(f"{line}")
        else:
            print("\n\n📋 APPLICATION LOGS:")
            print("-" * 40)
            print("- no logs captured -")
        print("-" * 40)

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_protocol(self, item, nextitem):
        """Structure test execution with clear boundaries."""
        self._print_test_header(item)
        original_handlers = self._setup_log_capture()

        try:
            outcome = yield
            result = outcome.get_result()
            self._print_logs_and_result(result)
        except Exception as e:
            print(f"❌ TEST RESULT: ERROR - {e}")
            raise
        finally:
            self._restore_streams(original_handlers)


def pytest_configure(config):
    """Configure pytest with our custom plugin."""
    config.pluginmanager.register(TestStructurePlugin(), "test_structure")
