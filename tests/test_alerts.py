#!/usr/bin/env python3
"""
Tests for the alert management system

TDD: These tests are written BEFORE the implementation.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest


class TestAlertLevel:
    """Tests for AlertLevel enum"""

    def test_has_info_level(self):
        """Test INFO level exists"""
        from imeteo_radar.core.alerts import AlertLevel

        assert AlertLevel.INFO.value == "info"

    def test_has_warning_level(self):
        """Test WARNING level exists"""
        from imeteo_radar.core.alerts import AlertLevel

        assert AlertLevel.WARNING.value == "warning"

    def test_has_error_level(self):
        """Test ERROR level exists"""
        from imeteo_radar.core.alerts import AlertLevel

        assert AlertLevel.ERROR.value == "error"

    def test_has_critical_level(self):
        """Test CRITICAL level exists"""
        from imeteo_radar.core.alerts import AlertLevel

        assert AlertLevel.CRITICAL.value == "critical"


class TestAlert:
    """Tests for Alert dataclass"""

    def test_creates_alert_with_required_fields(self):
        """Test creating an alert with required fields"""
        from imeteo_radar.core.alerts import Alert, AlertLevel

        alert = Alert(
            level=AlertLevel.ERROR,
            source="dwd",
            message="Download failed",
        )

        assert alert.level == AlertLevel.ERROR
        assert alert.source == "dwd"
        assert alert.message == "Download failed"
        assert alert.details is None

    def test_creates_alert_with_details(self):
        """Test creating an alert with optional details"""
        from imeteo_radar.core.alerts import Alert, AlertLevel

        alert = Alert(
            level=AlertLevel.ERROR,
            source="shmu",
            message="Connection timeout",
            details={"last_error": "TimeoutError", "retry_count": 3},
        )

        assert alert.details == {"last_error": "TimeoutError", "retry_count": 3}


class TestAlertManager:
    """Tests for AlertManager class"""

    def test_initializes_with_empty_failure_counts(self):
        """Test that AlertManager starts with no failure counts"""
        from imeteo_radar.core.alerts import AlertManager

        manager = AlertManager()
        assert len(manager.failure_counts) == 0

    def test_record_failure_increments_count(self):
        """Test that recording a failure increments the count"""
        from imeteo_radar.core.alerts import AlertManager

        manager = AlertManager()
        manager.record_failure("dwd", "Connection error")

        assert manager.failure_counts["dwd"] == 1

    def test_record_success_resets_count(self):
        """Test that recording success resets the failure count"""
        from imeteo_radar.core.alerts import AlertManager

        manager = AlertManager()
        manager.record_failure("dwd", "Error 1")
        manager.record_failure("dwd", "Error 2")
        manager.record_success("dwd")

        assert manager.failure_counts["dwd"] == 0

    def test_alert_sent_after_threshold_reached(self):
        """Test that an alert is sent after consecutive failures reach threshold"""
        from imeteo_radar.core.alerts import AlertManager

        manager = AlertManager()
        manager.alert_threshold = 3

        # Record failures
        manager.record_failure("dwd", "Error 1")
        manager.record_failure("dwd", "Error 2")

        # Mock the send_alert method
        manager.send_alert = MagicMock()

        # This should trigger the alert
        manager.record_failure("dwd", "Error 3")

        assert manager.send_alert.called

    def test_no_alert_before_threshold(self):
        """Test that no alert is sent before threshold is reached"""
        from imeteo_radar.core.alerts import AlertManager

        manager = AlertManager()
        manager.alert_threshold = 3
        manager.send_alert = MagicMock()

        manager.record_failure("dwd", "Error 1")
        manager.record_failure("dwd", "Error 2")

        assert not manager.send_alert.called

    def test_send_alert_logs_message(self):
        """Test that send_alert logs the alert message"""
        from imeteo_radar.core.alerts import Alert, AlertLevel, AlertManager

        manager = AlertManager()

        # Capture log output
        with patch.object(manager.logger, "log") as mock_log:
            alert = Alert(
                level=AlertLevel.ERROR, source="dwd", message="Test alert message"
            )
            manager.send_alert(alert)

            mock_log.assert_called()

    def test_tracks_multiple_sources_independently(self):
        """Test that failure counts are tracked independently per source"""
        from imeteo_radar.core.alerts import AlertManager

        manager = AlertManager()

        manager.record_failure("dwd", "Error")
        manager.record_failure("dwd", "Error")
        manager.record_failure("shmu", "Error")

        assert manager.failure_counts["dwd"] == 2
        assert manager.failure_counts["shmu"] == 1

    def test_success_only_resets_specific_source(self):
        """Test that success only resets the count for the specific source"""
        from imeteo_radar.core.alerts import AlertManager

        manager = AlertManager()

        manager.record_failure("dwd", "Error")
        manager.record_failure("shmu", "Error")
        manager.record_success("dwd")

        assert manager.failure_counts["dwd"] == 0
        assert manager.failure_counts["shmu"] == 1


class TestGlobalAlertManager:
    """Tests for global alert manager instance"""

    def test_get_alert_manager_returns_singleton(self):
        """Test that get_alert_manager returns the same instance"""
        from imeteo_radar.core.alerts import get_alert_manager

        manager1 = get_alert_manager()
        manager2 = get_alert_manager()

        assert manager1 is manager2

    def test_global_alert_manager_is_instance_of_alert_manager(self):
        """Test that global manager is an AlertManager instance"""
        from imeteo_radar.core.alerts import AlertManager, get_alert_manager

        manager = get_alert_manager()
        assert isinstance(manager, AlertManager)


class TestAlertManagerConfiguration:
    """Tests for AlertManager configuration"""

    def test_default_threshold_is_3(self):
        """Test that default alert threshold is 3"""
        from imeteo_radar.core.alerts import AlertManager

        manager = AlertManager()
        assert manager.alert_threshold == 3

    def test_threshold_can_be_configured(self):
        """Test that alert threshold can be changed"""
        from imeteo_radar.core.alerts import AlertManager

        manager = AlertManager()
        manager.alert_threshold = 5

        assert manager.alert_threshold == 5


class TestAlertIntegration:
    """Integration tests for alert system"""

    def test_full_failure_recovery_cycle(self):
        """Test complete failure -> alert -> recovery cycle"""
        from imeteo_radar.core.alerts import AlertManager

        manager = AlertManager()
        manager.alert_threshold = 2
        alerts_sent = []

        def capture_alert(alert):
            alerts_sent.append(alert)

        # Replace send_alert with capture function
        original_send = manager.send_alert
        manager.send_alert = capture_alert

        # First failure - no alert
        manager.record_failure("dwd", "Error 1")
        assert len(alerts_sent) == 0

        # Second failure - alert triggered
        manager.record_failure("dwd", "Error 2")
        assert len(alerts_sent) == 1
        assert alerts_sent[0].source == "dwd"

        # Success - resets counter
        manager.record_success("dwd")
        assert manager.failure_counts["dwd"] == 0

        # New failures start fresh
        manager.record_failure("dwd", "Error 3")
        assert len(alerts_sent) == 1  # No new alert yet
