from __future__ import annotations

from modules.upload_automation.network.dto import HttpRequestDTO

GRAPH_API_VERSION = "v24.0"
GRAPH_HOST = "https://graph.facebook.com"


def graph_url(*segments: str) -> str:
    """Join https://graph.facebook.com/v24.0/<segment>/..."""
    parts = [GRAPH_HOST, GRAPH_API_VERSION] + [s.strip("/") for s in segments if s]
    return "/".join(parts)


def build_adimages_upload_request(
    *,
    account_id: str,
    data: dict,
    files: dict,
    timeout: int | float = 60,
) -> HttpRequestDTO:
    """POST /{account_id}/adimages (multipart + form data with access_token)."""
    return HttpRequestDTO(
        method="POST",
        url=graph_url(account_id, "adimages"),
        data=data,
        files=files,
        timeout=timeout,
    )


def build_advideos_resumable_request(
    *,
    account_id: str,
    data: dict,
    files: dict | None = None,
    timeout: int | float = 180,
) -> HttpRequestDTO:
    """POST /{account_id}/advideos (resumable upload phases)."""
    return HttpRequestDTO(
        method="POST",
        url=graph_url(account_id, "advideos"),
        data=data,
        files=files,
        timeout=timeout,
    )
