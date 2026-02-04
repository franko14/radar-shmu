#!/usr/bin/env python3
"""
Centralized logging module for imeteo-radar

Provides structured JSON logging with timestamps and human-readable console output.
"""

import json
import logging
import os
import sys
from datetime import datetime, UTC


class StructuredFormatter(logging.Formatter):
    """JSON-structured log formatter with timestamps.

    Produces log entries as JSON objects with consistent fields:
    - timestamp: ISO 8601 format with UTC timezone
    - level: Log level name (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - logger: Logger name (e.g., imeteo_radar.sources.dwd)
    - message: The log message

    Extra fields can be added via the `extra` parameter:
    - source: Radar source name (dwd, shmu, chmi, etc.)
    - operation: Current operation (download, process, export, etc.)
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON string."""
        log_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add extra fields if present
        if hasattr(record, "source"):
            log_entry["source"] = record.source
        if hasattr(record, "operation"):
            log_entry["operation"] = record.operation
        if hasattr(record, "count"):
            log_entry["count"] = record.count
        if hasattr(record, "error"):
            log_entry["error"] = record.error

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


class ConsoleFormatter(logging.Formatter):
    """Human-readable formatter with timestamps for CLI output.

    Produces output in the format:
    [HH:MM:SS] <icon> <message>

    Icons indicate log levels for quick visual scanning.
    """

    LEVEL_ICONS = {
        "DEBUG": "\U0001f50d",  # Magnifying glass
        "INFO": "\U0001f4e1",  # Satellite antenna
        "WARNING": "\u26a0\ufe0f",  # Warning sign
        "ERROR": "\u274c",  # Red X
        "CRITICAL": "\U0001f6a8",  # Police light
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format log record for console output."""
        icon = self.LEVEL_ICONS.get(record.levelname, "")
        timestamp = datetime.now().strftime("%H:%M:%S")
        message = record.getMessage()

        return f"[{timestamp}] {icon} {message}"


# Global state for tracking if logging is already configured
_logging_configured = False


def setup_logging(
    level: str = "INFO",
    structured: bool = False,
    log_file: str | None = None,
) -> logging.Logger:
    """Configure application-wide logging.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        structured: Use JSON structured output (default: human-readable)
        log_file: Optional file path for log output

    Returns:
        Root logger for imeteo_radar

    Example:
        # Basic setup with console output
        setup_logging(level="INFO")

        # JSON output for machine parsing
        setup_logging(level="DEBUG", structured=True)

        # File logging for production
        setup_logging(level="INFO", log_file="/var/log/imeteo-radar.log")
    """
    global _logging_configured

    # Get or create root logger for imeteo_radar
    logger = logging.getLogger("imeteo_radar")

    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()

    # Set log level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)

    # Don't propagate to root logger
    logger.propagate = False

    # Choose formatter based on structured flag
    if structured:
        formatter = StructuredFormatter()
    else:
        formatter = ConsoleFormatter()

    # Add console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(numeric_level)
    logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        # Always use structured format for file logs
        file_handler.setFormatter(StructuredFormatter() if structured else formatter)
        file_handler.setLevel(numeric_level)
        logger.addHandler(file_handler)

    _logging_configured = True
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a named logger for a specific module.

    Args:
        name: Logger name (e.g., "imeteo_radar.sources.dwd")

    Returns:
        Logger instance

    Example:
        logger = get_logger("imeteo_radar.sources.dwd")
        logger.info("Downloading data", extra={"source": "dwd"})
    """
    return logging.getLogger(name)


def configure_from_env() -> logging.Logger:
    """Configure logging from environment variables.

    Reads:
        IMETEO_LOG_LEVEL: Log level (default: INFO)
        IMETEO_LOG_FORMAT: Format type - "json" or "console" (default: console)
        IMETEO_LOG_FILE: Optional log file path

    Returns:
        Configured root logger
    """
    level = os.environ.get("IMETEO_LOG_LEVEL", "INFO")
    log_format = os.environ.get("IMETEO_LOG_FORMAT", "console")
    log_file = os.environ.get("IMETEO_LOG_FILE")

    return setup_logging(
        level=level,
        structured=(log_format == "json"),
        log_file=log_file,
    )
