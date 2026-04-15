from __future__ import annotations

import logging
import time
import warnings
from typing import Callable, Iterable

import requests
from modules.upload_automation.network.dto import (
    HttpRequestDTO,
    RequestExecutionContextDTO,
    RetryPolicyDTO,
)

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
    session: requests.Session | None = None,
) -> requests.Response:
    """
    Execute a HTTP request with retry/backoff.

    attempt index passed to callbacks is 0-based.

    If ``session`` is provided, ``session.request`` is used (connection pooling);
    otherwise ``requests.request`` is used.
    """
    warnings.warn(
        "request_with_retry(...) is legacy. Prefer execute_request(HttpRequestDTO, RetryPolicyDTO).",
        DeprecationWarning,
        stacklevel=2,
    )
    request_dto = HttpRequestDTO(
        method=method,
        url=url,
        headers=headers,
        params=params,
        json=json,
        files=files,
        data=data,
        timeout=timeout,
    )
    retry_dto = RetryPolicyDTO(
        max_retries=max_retries,
        retry_statuses=retry_statuses,
        backoff_strategy=backoff_seconds,
        should_retry=should_retry,
    )
    context_dto = RequestExecutionContextDTO(
        session=session,
        on_response=on_response,
        on_retry=on_retry,
    )
    return execute_request(request_dto, retry_dto, context=context_dto)


def execute_request(
    request: HttpRequestDTO,
    retry_policy: RetryPolicyDTO | None = None,
    *,
    context: RequestExecutionContextDTO | None = None,
) -> requests.Response:
    """Execute HTTP request using DTOs and normalized retry behavior."""
    retry_policy = retry_policy or RetryPolicyDTO()
    context = context or RequestExecutionContextDTO()

    backoff_seconds: Callable[[int], float]
    backoff_seconds = retry_policy.backoff_strategy or (lambda attempt: float(2 ** attempt))
    retry_status_set = set(retry_policy.retry_statuses)
    last_exc: Exception | None = None
    method_up = request.method.upper()
    req_fn = context.session.request if context.session is not None else requests.request

    for attempt in range(retry_policy.max_retries + 1):
        resp: requests.Response | None = None
        try:
            resp = req_fn(
                method=method_up,
                url=request.url,
                headers=request.headers,
                params=request.params,
                json=request.json,
                files=request.files,
                data=request.data,
                timeout=request.timeout,
            )
            if context.on_response:
                context.on_response(resp)

            retryable = resp.status_code in retry_status_set
            if retry_policy.should_retry:
                retryable = retryable or bool(retry_policy.should_retry(resp))
            if retryable and attempt < retry_policy.max_retries:
                if context.on_retry:
                    context.on_retry(attempt, resp, None)
                sleep_s = max(0.0, float(backoff_seconds(attempt)))
                time.sleep(sleep_s)
                continue
            return resp
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt >= retry_policy.max_retries:
                break
            if context.on_retry:
                context.on_retry(attempt, None, exc)
            sleep_s = max(0.0, float(backoff_seconds(attempt)))
            time.sleep(sleep_s)

    msg = f"HTTP {method_up} failed after {retry_policy.max_retries + 1} attempts: {request.url}"
    logger.error(msg, exc_info=last_exc)
    raise HttpRequestError(method=method_up, url=request.url, status_code=None, message=msg) from last_exc
