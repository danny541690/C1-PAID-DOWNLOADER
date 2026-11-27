"""Bounded Drive read retries. Callers reconcile write outcomes separately."""

from contextlib import contextmanager
from contextvars import ContextVar
from http.client import IncompleteRead
from random import uniform
from socket import EAI_AGAIN, gaierror
from time import sleep, time
from urllib.error import HTTPError, URLError

from .quota import retry_deadline


ATTEMPTS = 4
MAX_DELAY = 30
_context = ContextVar("drive_retry_context", default=("drive", "none"))


@contextmanager
def operation_scope(phase, task_key=None):
    key = task_key[:12] if task_key and all(c in "0123456789abcdef" for c in task_key) else "none"
    token = _context.set((phase, key))
    try:
        yield
    finally:
        _context.reset(token)


def transient(error):
    if isinstance(error, HTTPError):
        return error.code in (408, 429) or 500 <= error.code < 600
    if isinstance(error, URLError):
        reason = error.reason
        if isinstance(reason, str):
            return any(token in reason.lower() for token in ("timed out", "reset", "temporary failure"))
        return transient(reason)
    if isinstance(error, gaierror):
        return error.errno == EAI_AGAIN
    return isinstance(error, (TimeoutError, ConnectionError, IncompleteRead))


def error_class(error):
    return f"HTTP_{error.code}" if isinstance(error, HTTPError) else type(error).__name__


def log_attempt(operation, error, attempt, delay=0, retry=False):
    phase, key = _context.get()
    print(f"drive_operation phase={phase} operation={operation} category={error_class(error)} "
          f"task={key} attempt={attempt} delay_seconds={delay:.2f} retry={str(retry).lower()}")


def retry_read(read, operation):
    """Make an initial read and up to three retries for transient errors only."""
    for attempt in range(1, ATTEMPTS + 1):
        try:
            return read()
        except (OSError, IncompleteRead) as error:
            retry = transient(error) and attempt < ATTEMPTS
            delay = 0
            if retry:
                base = 2 ** (attempt - 1)
                delay = base + uniform(0, base / 2)
                if isinstance(error, HTTPError) and error.headers and error.headers.get("Retry-After"):
                    now = time()
                    delay = max(delay, retry_deadline(error.headers["Retry-After"], now) - now)
                if delay > MAX_DELAY:
                    retry = False
            if not isinstance(error, HTTPError) or error.code != 404:
                log_attempt(operation, error, attempt, delay, retry)
            if not retry:
                raise
            sleep(delay)
