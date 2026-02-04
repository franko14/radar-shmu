#!/usr/bin/env python3
"""
Retry decorator with exponential backoff for imeteo-radar

Provides robust retry logic for network operations with configurable
backoff strategies and callback hooks.
"""

import random
import time
from functools import wraps
from collections.abc import Callable


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    on_retry: Callable[[int, float, Exception], None] | None = None,
    jitter: bool = False,
):
    """Decorator for retry with exponential backoff.

    Retries a function on specified exceptions with exponentially increasing
    delays between attempts. Useful for network operations that may fail
    transiently.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay in seconds (default: 1.0)
        max_delay: Maximum delay cap in seconds (default: 30.0)
        exceptions: Tuple of exception types to catch and retry (default: all)
        on_retry: Optional callback called before each retry with
            (attempt_number, delay, exception)
        jitter: Add random jitter to delays to prevent thundering herd

    Returns:
        Decorated function with retry behavior

    Example:
        @retry_with_backoff(max_retries=3, exceptions=(requests.RequestException,))
        def download_file(url):
            return requests.get(url)

        @retry_with_backoff(
            max_retries=5,
            base_delay=0.5,
            on_retry=lambda attempt, delay, e: print(f"Retry {attempt}: {e}")
        )
        def api_call():
            return fetch_data()
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    # If we've exhausted retries, raise
                    if attempt >= max_retries:
                        raise

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (2**attempt), max_delay)

                    # Add jitter if enabled (0-100% of delay)
                    if jitter:
                        delay = delay * (0.5 + random.random())
                        delay = min(delay, max_delay)

                    # Call on_retry callback if provided
                    if on_retry is not None:
                        on_retry(attempt + 1, delay, e)

                    # Sleep before retry
                    time.sleep(delay)

            # Should not reach here, but just in case
            if last_exception is not None:
                raise last_exception

        return wrapper

    return decorator


class RetryableOperation:
    """Context manager for retryable operations with state tracking.

    Alternative to decorator for cases where more control is needed.

    Example:
        with RetryableOperation(max_retries=3) as retry:
            while retry.should_continue():
                try:
                    result = do_something()
                    retry.success()
                    break
                except Exception as e:
                    retry.failed(e)
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        exceptions: tuple[type[Exception], ...] = (Exception,),
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exceptions = exceptions
        self.attempt = 0
        self.last_exception: Exception | None = None
        self._succeeded = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Don't suppress exceptions
        return False

    def should_continue(self) -> bool:
        """Check if we should continue trying."""
        return self.attempt <= self.max_retries and not self._succeeded

    def failed(self, exception: Exception):
        """Record a failed attempt and sleep before retry."""
        self.last_exception = exception
        self.attempt += 1

        if self.attempt <= self.max_retries:
            delay = min(self.base_delay * (2 ** (self.attempt - 1)), self.max_delay)
            time.sleep(delay)
        else:
            raise exception

    def success(self):
        """Mark operation as successful."""
        self._succeeded = True
