from __future__ import annotations

from modules.upload_automation.network.dto import HttpRequestDTO

from .constants import UNITY_ADVERTISE_API_BASE


def unity_api_url(path: str) -> str:
    """Return absolute URL for an Advertise API path (no leading slash required)."""
    return f"{UNITY_ADVERTISE_API_BASE.rstrip('/')}/{path.lstrip('/')}"


def build_unity_request(
    method: str,
    path: str,
    *,
    headers: dict | None = None,
    params: dict | None = None,
    json: dict | None = None,
    files: dict | None = None,
    data: dict | None = None,
    timeout: int | float = 60,
) -> HttpRequestDTO:
    """Build HttpRequestDTO for Unity Advertise v1 (path under advertise/v1)."""
    return HttpRequestDTO(
        method=method,
        url=unity_api_url(path),
        headers=headers,
        params=params,
        json=json,
        files=files,
        data=data,
        timeout=timeout,
    )
