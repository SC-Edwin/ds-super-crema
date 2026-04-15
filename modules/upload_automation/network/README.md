# upload_automation `network` 패키지

**HTTP 전송 공통층**입니다. `requests` 호출 인자·재시도 규칙·세션/훅을 DTO로 묶고, `execute_request`가 재시도 루프를 담당합니다.

광고 네트워크(Meta, Unity, Mintegral, Applovin 등)별 URL 조립·엔드포인트 팩토리는 **`modules.upload_automation.service`** 로 분리되어 있습니다. 여기서의 “network”는 **광고 네트워크가 아니라 네트워크 I/O(HTTP)** 를 뜻합니다.

## 구성 요소 (`network` 내부)

| 구분 | 모듈 | 역할 |
|------|------|------|
| DTO | `dto.py` | `HttpRequestDTO`, `RetryPolicyDTO`, `RequestExecutionContextDTO` |
| 실행기 | `http_client.py` | `execute_request`, 레거시 `request_with_retry`(DTO로 위임) |
| 정책 | `retry_policies.py` | 공통 `RetryPolicyDTO` 빌더(기본 API, 멀티파트, 선형 백오프 등) |

## 플랫폼별 요청 조립 (`service` 패키지)

네트워크별 하위 패키지에 두며, 공개 심볼은 각 패키지 `__init__.py`에서 re-export 합니다.

| 패키지 | 주요 모듈 | 용도 |
|--------|-----------|------|
| `service.facebook` | `graph_requests.py` | Graph URL, adimages / advideos 요청 DTO |
| `service.unity` | `constants.py`, `api_requests.py` | Unity Advertise 베이스 URL·`build_unity_request` |
| `service.mintegral` | `http_requests.py` | Mintegral용 `HttpRequestDTO` 조립 |
| `service.applovin` | `http_requests.py` | Applovin용 `HttpRequestDTO` 조립 |

## 호출 흐름 (요약)

```
플랫폼 UI·비즈니스 코드 (facebook_ads.py 등)
  → service.* — HttpRequestDTO 생성
  → network.retry_policies — RetryPolicyDTO
  → network.http_client.execute_request(...)
```

## DTO·회귀 검토

- **`HttpRequestDTO`**: `requests.request`에 대응하는 필드만 담습니다. API 형식 차이는 `service`의 팩토리에서 흡수합니다.
- **`RetryPolicyDTO`**: `max_retries`는 추가 시도 횟수이며, 전체 시도는 `max_retries + 1`입니다. `backoff_strategy(attempt)`의 `attempt`는 0부터입니다.
- **`RequestExecutionContextDTO`**: `session`, `on_response`, `on_retry`.
- **비재시도 HTTP 응답**: 재시도 조건에 안 걸리면 `Response`를 그대로 반환합니다. `HttpRequestError`는 주로 `RequestException` 소진 시.
- **레거시 `request_with_retry`**: `DeprecationWarning` 후 DTO 경로와 동일 루프.

## 새 플랫폼 추가 시

1. `service/<네트워크>/` 아래에 상수·URL·`build_*_request` 팩토리 모듈을 둡니다.  
2. 재시도 템플릿이 공통이면 `network/retry_policies.py`에 빌더를 추가합니다.
