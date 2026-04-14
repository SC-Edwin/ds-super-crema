from __future__ import annotations

import logging
import time
from typing import Callable, Iterable

import requests

logger = logging.getLogger(__name__)


class HttpRequestError(RuntimeError):
    """Normalized HTTP request error for upload automation modules."""

    def __init__(self, *, method: str, url: str, status_code: int | None, message: str):
        super().__init__(message)
        self.method = method
        self.url = url
        self.status_code = status_code


def request_with_retry(
    *,
    method: str,
    url: str,
    headers: dict | None = None,
    params: dict | None = None,
    json: dict | None = None,
    files: dict | None = None,
    data: dict | None = None,
    timeout: int | float = 60,
    max_retries: int = 3,
    retry_statuses: Iterable[int] = (429, 500, 502, 503, 504),
    backoff_seconds: Callable[[int], float] | None = None,
    should_retry: Callable[[requests.Response], bool] | None = None,
    on_response: Callable[[requests.Response], None] | None = None,
    on_retry: Callable[[int, requests.Response | None, Exception | None], None] | None = None,
) -> requests.Response:
    """
    Execute a HTTP request with retry/backoff.

    attempt index passed to callbacks is 0-based.
    """
    if backoff_seconds is None:
        backoff_seconds = lambda attempt: float(2 ** attempt)

    retry_status_set = set(retry_statuses)
    last_exc: Exception | None = None
    method_up = method.upper()

    for attempt in range(max_retries + 1):
        resp: requests.Response | None = None
        try:
            resp = requests.request(
                method=method_up,
                url=url,
                headers=headers,
                params=params,
                json=json,
                files=files,
                data=data,
                timeout=timeout,
            )
            if on_response:
                on_response(resp)

            retryable = resp.status_code in retry_status_set
            if should_retry:
                retryable = retryable or bool(should_retry(resp))
            if retryable and attempt < max_retries:
                if on_retry:
                    on_retry(attempt, resp, None)
                sleep_s = max(0.0, float(backoff_seconds(attempt)))
                time.sleep(sleep_s)
                continue
            return resp
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt >= max_retries:
                break
            if on_retry:
                on_retry(attempt, None, exc)
            sleep_s = max(0.0, float(backoff_seconds(attempt)))
            time.sleep(sleep_s)

    msg = f"HTTP {method_up} failed after {max_retries + 1} attempts: {url}"
    logger.error(msg, exc_info=last_exc)
    raise HttpRequestError(method=method_up, url=url, status_code=None, message=msg) from last_exc
