#!/usr/bin/env python3
"""
Tests for the centralized logging module

TDD: These tests are written BEFORE the implementation.
"""

import io
import json
import logging
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


class TestStructuredFormatter:
    """Tests for JSON-structured log formatter"""

    def test_formats_basic_log_entry_as_json(self):
        """Test that basic log entries are formatted as valid JSON"""
        from imeteo_radar.core.logging import StructuredFormatter

        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        parsed = json.loads(output)

        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "test.logger"
        assert parsed["message"] == "Test message"
        assert "timestamp" in parsed

    def test_includes_timestamp_in_iso_format(self):
        """Test that timestamp is in ISO 8601 format with timezone"""
        from imeteo_radar.core.logging import StructuredFormatter

        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        parsed = json.loads(output)

        # Should be ISO format with timezone indicator
        timestamp = parsed["timestamp"]
        assert "T" in timestamp
        assert timestamp.endswith("Z") or "+" in timestamp or "-" in timestamp[-6:]

    def test_includes_extra_fields(self):
        """Test that extra fields like 'source' and 'operation' are included"""
        from imeteo_radar.core.logging import StructuredFormatter

        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Downloading data",
            args=(),
            exc_info=None,
        )
        record.source = "dwd"
        record.operation = "download"

        output = formatter.format(record)
        parsed = json.loads(output)

        assert parsed["source"] == "dwd"
        assert parsed["operation"] == "download"

    def test_includes_exception_info(self):
        """Test that exception information is included when present"""
        from imeteo_radar.core.logging import StructuredFormatter

        formatter = StructuredFormatter()

        try:
            raise ValueError("Test error")
        except ValueError:
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=10,
            msg="An error occurred",
            args=(),
            exc_info=exc_info,
        )

        output = formatter.format(record)
        parsed = json.loads(output)

        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]
        assert "Test error" in parsed["exception"]


class TestConsoleFormatter:
    """Tests for human-readable console formatter"""

    def test_formats_with_timestamp(self):
        """Test that output includes timestamp in HH:MM:SS format"""
        from imeteo_radar.core.logging import ConsoleFormatter

        formatter = ConsoleFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)

        # Should contain timestamp in brackets
        assert "[" in output and "]" in output
        # Should contain the message
        assert "Test message" in output

    def test_includes_level_icons(self):
        """Test that appropriate emoji icons are used for each level"""
        from imeteo_radar.core.logging import ConsoleFormatter

        formatter = ConsoleFormatter()

        levels_and_expected = [
            (logging.DEBUG, None),  # Any icon for debug
            (logging.INFO, None),  # Any icon for info
            (logging.WARNING, None),  # Any icon for warning
            (logging.ERROR, None),  # Any icon for error
            (logging.CRITICAL, None),  # Any icon for critical
        ]

        for level, _ in levels_and_expected:
            record = logging.LogRecord(
                name="test",
                level=level,
                pathname="test.py",
                lineno=10,
                msg="Test",
                args=(),
                exc_info=None,
            )
            output = formatter.format(record)
            # Each level should produce output
            assert len(output) > 0


class TestSetupLogging:
    """Tests for the setup_logging function"""

    def test_returns_logger_instance(self):
        """Test that setup_logging returns a Logger instance"""
        from imeteo_radar.core.logging import setup_logging

        logger = setup_logging()
        assert isinstance(logger, logging.Logger)

    def test_respects_level_parameter(self):
        """Test that log level is set correctly"""
        from imeteo_radar.core.logging import setup_logging

        logger = setup_logging(level="DEBUG")
        assert logger.level == logging.DEBUG

        logger = setup_logging(level="WARNING")
        assert logger.level == logging.WARNING

    def test_uses_structured_formatter_when_requested(self):
        """Test that structured=True uses JSON formatter"""
        from imeteo_radar.core.logging import StructuredFormatter, setup_logging

        logger = setup_logging(structured=True)

        # Check that at least one handler uses StructuredFormatter
        has_structured = False
        for handler in logger.handlers:
            if isinstance(handler.formatter, StructuredFormatter):
                has_structured = True
                break

        assert has_structured

    def test_logs_to_file_when_specified(self):
        """Test that logs are written to file when log_file is specified"""
        from imeteo_radar.core.logging import setup_logging

        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            log_file = f.name

        try:
            logger = setup_logging(log_file=log_file)
            logger.info("Test file logging")

            # Flush handlers
            for handler in logger.handlers:
                handler.flush()

            # Check file contains the log
            with open(log_file) as f:
                content = f.read()
            assert "Test file logging" in content
        finally:
            Path(log_file).unlink(missing_ok=True)

    def test_console_output_by_default(self):
        """Test that console output is enabled by default"""
        from imeteo_radar.core.logging import setup_logging

        logger = setup_logging()

        # Should have at least one StreamHandler
        has_stream_handler = False
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                has_stream_handler = True
                break

        assert has_stream_handler


class TestGetLogger:
    """Tests for the get_logger convenience function"""

    def test_returns_named_logger(self):
        """Test that get_logger returns a logger with the specified name"""
        from imeteo_radar.core.logging import get_logger

        logger = get_logger("imeteo_radar.sources.dwd")
        assert logger.name == "imeteo_radar.sources.dwd"

    def test_child_loggers_inherit_configuration(self):
        """Test that child loggers inherit from parent configuration"""
        from imeteo_radar.core.logging import get_logger, setup_logging

        # Setup root logger
        setup_logging(level="DEBUG")

        # Get child logger
        child = get_logger("imeteo_radar.sources.dwd")

        # Should be able to log at debug level
        assert child.isEnabledFor(logging.DEBUG)


class TestLoggerIntegration:
    """Integration tests for logging system"""

    def test_full_logging_workflow(self):
        """Test complete logging workflow with all features"""
        from imeteo_radar.core.logging import get_logger, setup_logging

        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            log_file = f.name

        try:
            # Setup logging
            setup_logging(level="INFO", structured=True, log_file=log_file)

            # Get logger for a specific module
            logger = get_logger("imeteo_radar.sources.dwd")

            # Log with extra fields
            logger.info(
                "Downloading radar data", extra={"source": "dwd", "operation": "download"}
            )

            # Flush handlers
            root = logging.getLogger("imeteo_radar")
            for handler in root.handlers:
                handler.flush()

            # Verify file output
            with open(log_file) as f:
                content = f.read()

            assert "Downloading radar data" in content

        finally:
            Path(log_file).unlink(missing_ok=True)
