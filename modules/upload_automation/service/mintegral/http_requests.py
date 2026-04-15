from __future__ import annotations

from modules.upload_automation.network.dto import HttpRequestDTO


def build_mintegral_http_request(
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    params: dict | None = None,
    json: dict | None = None,
    files: dict | None = None,
    timeout: int | float = 30,
) -> HttpRequestDTO:
    """Build HttpRequestDTO for Mintegral Open API or storage upload."""
    return HttpRequestDTO(
        method=method,
        url=url,
        headers=headers,
        params=params,
        json=json,
        files=files,
        timeout=timeout,
    )
