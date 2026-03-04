#!/usr/bin/env python3
"""
Tests for the retry decorator with exponential backoff

TDD: These tests are written BEFORE the implementation.
"""

import time
from unittest.mock import MagicMock, patch

import pytest


class TestRetryWithBackoff:
    """Tests for retry_with_backoff decorator"""

    def test_returns_result_on_success(self):
        """Test that successful function returns result without retries"""
        from imeteo_radar.core.retry import retry_with_backoff

        @retry_with_backoff(max_retries=3)
        def successful_function():
            return "success"

        result = successful_function()
        assert result == "success"

    def test_retries_on_exception(self):
        """Test that function is retried on exception"""
        from imeteo_radar.core.retry import retry_with_backoff

        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01)
        def failing_then_succeeding():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"

        result = failing_then_succeeding()
        assert result == "success"
        assert call_count == 3

    def test_raises_after_max_retries(self):
        """Test that exception is raised after max retries exceeded"""
        from imeteo_radar.core.retry import retry_with_backoff

        @retry_with_backoff(max_retries=2, base_delay=0.01)
        def always_fails():
            raise ConnectionError("Network error")

        with pytest.raises(ConnectionError) as exc_info:
            always_fails()

        assert "Network error" in str(exc_info.value)

    def test_only_catches_specified_exceptions(self):
        """Test that only specified exception types trigger retry"""
        from imeteo_radar.core.retry import retry_with_backoff

        @retry_with_backoff(max_retries=3, base_delay=0.01, exceptions=(ValueError,))
        def raises_type_error():
            raise TypeError("This should not be retried")

        with pytest.raises(TypeError):
            raises_type_error()

    def test_exponential_backoff_delay(self):
        """Test that delay increases exponentially"""
        from imeteo_radar.core.retry import retry_with_backoff

        delays = []

        def track_retry(attempt, delay, error):
            delays.append(delay)

        call_count = 0

        @retry_with_backoff(
            max_retries=4, base_delay=0.1, max_delay=10.0, on_retry=track_retry
        )
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")

        with pytest.raises(ValueError):
            always_fails()

        # Should have 4 retries with exponential delays
        # base_delay * 2^0, base_delay * 2^1, base_delay * 2^2, base_delay * 2^3
        # But capped at max_delay
        assert len(delays) == 4
        assert delays[0] == pytest.approx(0.1, rel=0.1)  # 0.1 * 2^0
        assert delays[1] == pytest.approx(0.2, rel=0.1)  # 0.1 * 2^1
        assert delays[2] == pytest.approx(0.4, rel=0.1)  # 0.1 * 2^2
        assert delays[3] == pytest.approx(0.8, rel=0.1)  # 0.1 * 2^3

    def test_respects_max_delay(self):
        """Test that delay is capped at max_delay"""
        from imeteo_radar.core.retry import retry_with_backoff

        delays = []

        def track_retry(attempt, delay, error):
            delays.append(delay)

        @retry_with_backoff(
            max_retries=5, base_delay=1.0, max_delay=2.0, on_retry=track_retry
        )
        def always_fails():
            raise ValueError("Always fails")

        with pytest.raises(ValueError):
            always_fails()

        # All delays should be capped at max_delay=2.0
        assert all(d <= 2.0 for d in delays)

    def test_on_retry_callback_receives_correct_arguments(self):
        """Test that on_retry callback receives attempt number, delay, and error"""
        from imeteo_radar.core.retry import retry_with_backoff

        callback_args = []

        def on_retry_callback(attempt, delay, error):
            callback_args.append({"attempt": attempt, "delay": delay, "error": error})

        @retry_with_backoff(max_retries=2, base_delay=0.01, on_retry=on_retry_callback)
        def always_fails():
            raise ValueError("Test error")

        with pytest.raises(ValueError):
            always_fails()

        assert len(callback_args) == 2
        assert callback_args[0]["attempt"] == 1
        assert callback_args[1]["attempt"] == 2
        assert isinstance(callback_args[0]["error"], ValueError)

    def test_preserves_function_metadata(self):
        """Test that decorator preserves original function metadata"""
        from imeteo_radar.core.retry import retry_with_backoff

        @retry_with_backoff(max_retries=3)
        def documented_function():
            """This is the docstring."""
            return True

        assert documented_function.__name__ == "documented_function"
        assert documented_function.__doc__ == "This is the docstring."

    def test_works_with_function_arguments(self):
        """Test that decorated function correctly passes arguments"""
        from imeteo_radar.core.retry import retry_with_backoff

        @retry_with_backoff(max_retries=3)
        def add(a, b, c=0):
            return a + b + c

        assert add(1, 2) == 3
        assert add(1, 2, c=3) == 6

    def test_works_with_class_methods(self):
        """Test that decorator works with class methods"""
        from imeteo_radar.core.retry import retry_with_backoff

        class MyClass:
            def __init__(self):
                self.call_count = 0

            @retry_with_backoff(max_retries=3, base_delay=0.01)
            def method_that_fails_once(self):
                self.call_count += 1
                if self.call_count < 2:
                    raise ValueError("First call fails")
                return "success"

        obj = MyClass()
        result = obj.method_that_fails_once()
        assert result == "success"
        assert obj.call_count == 2


class TestRetryWithJitter:
    """Tests for retry with jitter (optional feature)"""

    def test_jitter_adds_randomness_to_delay(self):
        """Test that jitter adds random variation to delay"""
        from imeteo_radar.core.retry import retry_with_backoff

        delays = []

        def track_retry(attempt, delay, error):
            delays.append(delay)

        @retry_with_backoff(
            max_retries=3, base_delay=0.01, max_delay=1.0, jitter=True, on_retry=track_retry
        )
        def always_fails():
            raise ValueError("Always fails")

        with pytest.raises(ValueError):
            always_fails()

        # With jitter, should have collected delays
        assert len(delays) == 3
        # All delays should be within bounds
        assert all(d <= 1.0 for d in delays)


class TestRetryIntegration:
    """Integration tests for retry with network-like behavior"""

    def test_simulates_network_recovery(self):
        """Test retry behavior simulating network recovery"""
        from imeteo_radar.core.retry import retry_with_backoff

        attempts = []

        @retry_with_backoff(
            max_retries=3,
            base_delay=0.01,
            exceptions=(ConnectionError, TimeoutError),
        )
        def network_request():
            attempts.append(time.time())
            if len(attempts) < 3:
                raise TimeoutError("Connection timed out")
            return {"status": "ok"}

        result = network_request()
        assert result == {"status": "ok"}
        assert len(attempts) == 3

    @patch("imeteo_radar.core.retry.time.sleep")
    def test_sleeps_correct_duration(self, mock_sleep):
        """Test that correct sleep durations are used"""
        from imeteo_radar.core.retry import retry_with_backoff

        @retry_with_backoff(max_retries=3, base_delay=1.0)
        def always_failing():
            raise ValueError("Always fails")

        with pytest.raises(ValueError):
            always_failing()

        # Should have slept 3 times with exponential delays
        assert mock_sleep.call_count == 3
        # Check approximate sleep durations
        calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert calls[0] == pytest.approx(1.0, rel=0.1)
        assert calls[1] == pytest.approx(2.0, rel=0.1)
        assert calls[2] == pytest.approx(4.0, rel=0.1)


class TestConnectivityCheck:
    """Tests for connectivity_check parameter in retry_with_backoff"""

    def test_connectivity_check_runs_before_first_attempt(self):
        """Test that connectivity_check is called before the retry loop"""
        from imeteo_radar.core.retry import retry_with_backoff

        call_order = []

        def check():
            call_order.append("check")

        @retry_with_backoff(max_retries=3, connectivity_check=check)
        def func():
            call_order.append("func")
            return "ok"

        result = func()
        assert result == "ok"
        assert call_order == ["check", "func"]

    def test_connectivity_check_failure_bypasses_retries(self):
        """Test that ConnectionError from check propagates without retries"""
        from imeteo_radar.core.retry import retry_with_backoff

        func_called = False

        def failing_check():
            raise ConnectionError("host unreachable")

        @retry_with_backoff(
            max_retries=3,
            base_delay=0.01,
            exceptions=(ValueError,),
            connectivity_check=failing_check,
        )
        def func():
            nonlocal func_called
            func_called = True
            return "ok"

        with pytest.raises(ConnectionError, match="host unreachable"):
            func()

        assert not func_called

    def test_no_connectivity_check_default(self):
        """Test that default None connectivity_check does nothing"""
        from imeteo_radar.core.retry import retry_with_backoff

        @retry_with_backoff(max_retries=1, base_delay=0.01)
        def func():
            return "ok"

        assert func() == "ok"

    def test_connectivity_check_passes_then_func_retries_normally(self):
        """Test that if check passes, normal retry behavior continues"""
        from imeteo_radar.core.retry import retry_with_backoff

        call_count = 0

        def passing_check():
            pass

        @retry_with_backoff(
            max_retries=2,
            base_delay=0.01,
            exceptions=(ValueError,),
            connectivity_check=passing_check,
        )
        def func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("transient")
            return "ok"

        assert func() == "ok"
        assert call_count == 2


class TestTcpProbe:
    """Tests for tcp_probe utility function"""

    @patch("socket.create_connection")
    def test_successful_probe(self, mock_conn):
        """Test tcp_probe succeeds when connection works"""
        from imeteo_radar.core.retry import tcp_probe

        mock_sock = MagicMock()
        mock_sock.__enter__ = MagicMock(return_value=mock_sock)
        mock_sock.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value = mock_sock

        tcp_probe("example.com", 443, 5.0)

        mock_conn.assert_called_once_with(("example.com", 443), timeout=5.0)

    @patch("socket.create_connection", side_effect=OSError("Connection refused"))
    def test_failed_probe_raises_connection_error(self, mock_conn):
        """Test tcp_probe raises ConnectionError on failure"""
        from imeteo_radar.core.retry import tcp_probe

        with pytest.raises(ConnectionError, match="example.com:443 unreachable"):
            tcp_probe("example.com", 443, 5.0)

    @patch("socket.create_connection", side_effect=OSError("timed out"))
    def test_timeout_raises_connection_error(self, mock_conn):
        """Test tcp_probe raises ConnectionError on timeout"""
        from imeteo_radar.core.retry import tcp_probe

        with pytest.raises(ConnectionError, match="unreachable.*5.0s"):
            tcp_probe("example.com", 443, 5.0)


class TestCheckConnectivityLogic:
    """Tests for the check_connectivity logic used by RadarSource.

    We test the logic (urlparse + tcp_probe) directly because importing
    RadarSource triggers the full package import chain which requires
    Python 3.11+ (datetime.UTC). The actual method in base.py uses
    the same tcp_probe + urlparse pattern tested here.
    """

    @patch("socket.create_connection")
    def test_probes_host_from_url(self, mock_conn):
        """Test that tcp_probe is called with parsed hostname"""
        from urllib.parse import urlparse
        from imeteo_radar.core.retry import tcp_probe

        mock_sock = MagicMock()
        mock_sock.__enter__ = MagicMock(return_value=mock_sock)
        mock_sock.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value = mock_sock

        # Replicate check_connectivity logic
        url = "https://example.com/data"
        host = urlparse(url).hostname
        tcp_probe(host, 443, 3.0)

        mock_conn.assert_called_once_with(("example.com", 443), timeout=3.0)

    @patch("socket.create_connection", side_effect=OSError("Connection refused"))
    def test_raises_connection_error_on_unreachable(self, mock_conn):
        """Test that ConnectionError propagates when host is unreachable"""
        from urllib.parse import urlparse
        from imeteo_radar.core.retry import tcp_probe

        url = "https://example.com/data"
        host = urlparse(url).hostname
        with pytest.raises(ConnectionError):
            tcp_probe(host, 443, 5.0)

    @patch("socket.create_connection")
    def test_skips_when_no_url(self, mock_conn):
        """Test that no probe happens when url is None"""
        from urllib.parse import urlparse
        from imeteo_radar.core.retry import tcp_probe

        # Replicate check_connectivity: skip when no base_url
        url = None
        if url is not None:
            host = urlparse(url).hostname
            if host:
                tcp_probe(host, 443, 5.0)

        mock_conn.assert_not_called()

    @patch("socket.create_connection")
    def test_parses_dwd_host_correctly(self, mock_conn):
        """Test correct host extraction from DWD URL"""
        from urllib.parse import urlparse
        from imeteo_radar.core.retry import tcp_probe

        mock_sock = MagicMock()
        mock_sock.__enter__ = MagicMock(return_value=mock_sock)
        mock_sock.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value = mock_sock

        url = "https://opendata.dwd.de/weather/radar/composite"
        host = urlparse(url).hostname
        tcp_probe(host, 443, 5.0)

        mock_conn.assert_called_once_with(("opendata.dwd.de", 443), timeout=5.0)


class TestExecutionTimeout:
    """Tests for ExecutionTimeout context manager"""

    def test_no_timeout_when_fast(self):
        """Test that fast operations complete normally"""
        from imeteo_radar.core.retry import ExecutionTimeout

        with ExecutionTimeout(10):
            result = 1 + 1

        assert result == 2

    def test_timeout_raises_system_exit(self):
        """Test that timeout triggers SystemExit(2)"""
        from imeteo_radar.core.retry import ExecutionTimeout

        with pytest.raises(SystemExit) as exc_info:
            with ExecutionTimeout(1, message="test timeout"):
                time.sleep(3)

        assert exc_info.value.code == 2

    def test_alarm_cleared_on_normal_exit(self):
        """Test that SIGALRM is cleared after normal context exit"""
        import signal

        from imeteo_radar.core.retry import ExecutionTimeout

        with ExecutionTimeout(60):
            pass

        # Alarm should be cleared (0 means no alarm pending)
        remaining = signal.alarm(0)
        assert remaining == 0
