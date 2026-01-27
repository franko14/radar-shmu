#!/usr/bin/env python3
"""
Alert management system for imeteo-radar

Provides centralized tracking of failures and alerts for monitoring
the health of radar data sources.
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class AlertLevel(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Represents an alert event.

    Attributes:
        level: Severity level of the alert
        source: Radar source that triggered the alert
        message: Human-readable alert message
        details: Optional additional details as key-value pairs
    """

    level: AlertLevel
    source: str
    message: str
    details: Optional[Dict[str, Any]] = None


class AlertManager:
    """Centralized alert management for tracking source failures.

    Tracks consecutive failures per source and triggers alerts when
    a configurable threshold is reached.

    Example:
        manager = AlertManager()
        manager.alert_threshold = 3

        try:
            result = download_data()
            manager.record_success("dwd")
        except Exception as e:
            manager.record_failure("dwd", str(e))
    """

    def __init__(self):
        """Initialize the AlertManager."""
        self.logger = logging.getLogger("imeteo_radar.alerts")
        self.handlers: List[Callable[[Alert], None]] = []
        self.failure_counts: Dict[str, int] = {}
        self.alert_threshold = 3  # consecutive failures before alert

    def record_failure(self, source: str, error: str):
        """Record a failure for a source and potentially trigger an alert.

        Args:
            source: Radar source identifier (e.g., 'dwd', 'shmu')
            error: Error message or description
        """
        self.failure_counts[source] = self.failure_counts.get(source, 0) + 1

        if self.failure_counts[source] >= self.alert_threshold:
            self.send_alert(
                Alert(
                    level=AlertLevel.ERROR,
                    source=source,
                    message=f"Source {source} has failed {self.failure_counts[source]} consecutive times",
                    details={"last_error": error},
                )
            )

    def record_success(self, source: str):
        """Record a successful operation and reset the failure count.

        Args:
            source: Radar source identifier
        """
        self.failure_counts[source] = 0

    def send_alert(self, alert: Alert):
        """Send an alert via the logger and any registered handlers.

        Args:
            alert: Alert to send
        """
        # Map AlertLevel to logging level
        log_level = {
            AlertLevel.INFO: logging.INFO,
            AlertLevel.WARNING: logging.WARNING,
            AlertLevel.ERROR: logging.ERROR,
            AlertLevel.CRITICAL: logging.CRITICAL,
        }.get(alert.level, logging.ERROR)

        self.logger.log(log_level, f"[ALERT] {alert.source}: {alert.message}")

        # Call registered handlers
        for handler in self.handlers:
            try:
                handler(alert)
            except Exception as e:
                self.logger.error(f"Alert handler failed: {e}")

    def add_handler(self, handler: Callable[[Alert], None]):
        """Register an alert handler.

        Handlers receive Alert objects and can perform additional
        actions like sending emails, webhooks, etc.

        Args:
            handler: Callable that takes an Alert parameter
        """
        self.handlers.append(handler)

    def get_failure_count(self, source: str) -> int:
        """Get the current failure count for a source.

        Args:
            source: Radar source identifier

        Returns:
            Number of consecutive failures
        """
        return self.failure_counts.get(source, 0)

    def get_all_failure_counts(self) -> Dict[str, int]:
        """Get failure counts for all tracked sources.

        Returns:
            Dictionary mapping source names to failure counts
        """
        return dict(self.failure_counts)


# Global singleton instance
_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """Get the global AlertManager instance.

    Returns:
        Singleton AlertManager instance
    """
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


def reset_alert_manager():
    """Reset the global AlertManager instance.

    Useful for testing to ensure clean state.
    """
    global _alert_manager
    _alert_manager = None
