from __future__ import annotations

from modules.upload_automation.network.dto import RetryPolicyDTO


def _default_backoff(attempt: int) -> float:
    return float(2 ** attempt)


def _upload_backoff(attempt: int) -> float:
    return float(1 + (attempt * 2))


def build_default_api_policy(*, max_retries: int = 3) -> RetryPolicyDTO:
    return RetryPolicyDTO(max_retries=max_retries, backoff_strategy=_default_backoff)


def build_upload_multipart_policy(*, max_retries: int = 2) -> RetryPolicyDTO:
    return RetryPolicyDTO(max_retries=max_retries, backoff_strategy=_upload_backoff)


def build_no_retry_policy() -> RetryPolicyDTO:
    return RetryPolicyDTO(max_retries=0, backoff_strategy=lambda _: 0.0)


def build_mintegral_api_policy(*, max_retries: int = 2) -> RetryPolicyDTO:
    """Mintegral/Applovin-style linear backoff (1s, 3s, 5s, ...)."""
    return RetryPolicyDTO(max_retries=max_retries, backoff_strategy=_upload_backoff)


def build_applovin_api_policy(*, max_retries: int = 2) -> RetryPolicyDTO:
    """Same backoff family as Mintegral (historically identical in callers)."""
    return RetryPolicyDTO(max_retries=max_retries, backoff_strategy=_upload_backoff)
