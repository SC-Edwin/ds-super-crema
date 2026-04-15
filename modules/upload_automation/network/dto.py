from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

import requests


RetryPredicate = Callable[[requests.Response], bool]
BackoffStrategy = Callable[[int], float]
OnResponseHook = Callable[[requests.Response], None]
OnRetryHook = Callable[[int, requests.Response | None, Exception | None], None]


@dataclass
class HttpRequestDTO:
    method: str
    url: str
    headers: dict[str, Any] | None = None
    params: dict[str, Any] | None = None
    json: dict[str, Any] | None = None
    files: dict[str, Any] | None = None
    data: dict[str, Any] | None = None
    timeout: int | float = 60


@dataclass
class RetryPolicyDTO:
    max_retries: int = 3
    retry_statuses: Iterable[int] = field(default_factory=lambda: (429, 500, 502, 503, 504))
    backoff_strategy: BackoffStrategy | None = None
    should_retry: RetryPredicate | None = None


@dataclass
class RequestExecutionContextDTO:
    session: requests.Session | None = None
    on_response: OnResponseHook | None = None
    on_retry: OnRetryHook | None = None
    trace_label: str | None = None
