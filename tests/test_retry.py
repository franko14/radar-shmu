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
