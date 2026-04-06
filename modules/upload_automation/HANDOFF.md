# Upload Automation 모듈 인수인계 문서

## 개요

이 폴더는 **광고 소재(영상/플레이어블)를 여러 광고 플랫폼에 업로드하는 자동화 시스템**입니다.
Streamlit 기반이며, 두 가지 모드로 운영됩니다:

- **Test Mode (운영팀)**: 캠페인/광고세트 생성부터 소재 업로드까지 전체 워크플로우
- **Marketer Mode (마케터)**: 기존 캠페인/광고세트에 소재만 업로드하는 간소화된 워크플로우

---

## 파일 구조 한눈에 보기

```
upload_automation/
│
├── main.py              ← 메인 UI (탭 라우팅, 파일 임포트, 업로드 실행)
├── __init__.py          ← run() 함수 export용
│
│  ── Facebook ──
├── facebook_ads.py      ← Facebook API 핵심 함수 (Test Mode + 공용 헬퍼)
├── fb.py                ← Facebook 마케터 모드 (UI + 업로드 로직)
│
│  ── Unity ──
├── unity_ads.py         ← Unity Ads API 핵심 함수 (Test Mode + 공용 헬퍼)
├── uni.py               ← Unity 마케터 모드 (UI + 업로드 로직)
│
│  ── Google Ads ──
├── google_ads.py        ← Google Ads API 핵심 함수
├── ga.py                ← Google Ads 마케터 모드 (UI + 배포 로직)
│
│  ── 기타 플랫폼 ──
├── mintegral.py         ← Mintegral 미디어 라이브러리 업로드 (마케터 전용)
├── applovin.py          ← Applovin 미디어 라이브러리 + 크리에이티브 관리 (마케터 전용)
│
│  ── 유틸리티 ──
├── drive_import.py      ← Google Drive 폴더에서 영상 다운로드 (병렬 처리)
├── game_manager.py      ← 게임 목록 관리 (games_config.json)
├── upload_logger.py     ← BigQuery 감사 로그 (업로드 이벤트 기록)
├── devtools.py          ← 개발자 모드 로깅/디버깅 패널
├── generate_refresh_token.py  ← Google Ads OAuth 토큰 생성 (1회성 유틸)
├── vietnam.py           ← 베트남팀 전용 탭 (Test Mode 복제)
│
├── Document.md          ← 기존 아키텍처 문서
└── HANDOFF.md           ← 이 파일
```

---

## 파일별 상세 설명

### 1. `main.py` — 메인 UI 허브

**역할**: 전체 업로드 자동화 UI의 중심. 게임별 탭 생성, 플랫폼 선택, 파일 임포트, 업로드 실행을 총괄합니다.

**핵심 흐름**:
1. 사용자가 게임 선택 (탭)
2. 플랫폼 선택 (Facebook, Unity, Mintegral 등)
3. 파일 임포트 (Google Drive 또는 로컬 업로드)
4. 각 플랫폼 설정 패널 렌더링
5. "업로드" 버튼 → 해당 플랫폼 모듈 호출

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `render_main_app()` | 게임 탭, 플랫폼 선택, 파일 임포트 UI, 실행 버튼 렌더링 |
| `init_state(prefix)` | 세션 상태 초기화 (업로드 파일, 설정값 컨테이너) |
| `validate_count(files)` | 업로드 파일 유효성 검사 (.mp4, .html, .zip) |

**어떤 모듈을 호출하는지**:
- Test Mode: `facebook_ads` + `unity_ads`
- Marketer Mode: `fb` + `uni` + `mintegral` + `applovin` + `ga`

---

### 2. `facebook_ads.py` — Facebook API 핵심 (Test Mode + 공용)

**역할**: Meta/Facebook Business SDK를 래핑한 저수준 API 함수 모음. Test Mode에서 직접 사용하고, `fb.py`(마케터 모드)에서도 이 함수들을 호출합니다.

**주요 기능**:
- Facebook SDK 초기화 (`init_fb_from_secrets`)
- 영상 업로드 및 상태 폴링 (`wait_for_video_ready`)
- 썸네일 추출/업로드 (`extract_thumbnail_from_video`, `upload_thumbnail_image`)
- 타겟팅 설정 빌드 (`build_targeting_from_settings`)
- 예산 계산 (`compute_budget_from_settings`)
- 게임별 기본값: `GAME_DEFAULTS` (앱 ID, 스토어 URL 등 12개 게임)

**사용 API**: Facebook Graph API v24.0 + Facebook Business SDK

---

### 3. `fb.py` — Facebook 마케터 모드

**역할**: 마케터가 사용하는 Facebook 업로드 전체 워크플로우. 캠페인/광고세트 선택 → 광고 포맷 선택 → 업로드 실행.

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `render_facebook_settings_panel()` | 마케터용 설정 UI (캠페인, 광고세트, 광고 포맷, 텍스트, CTA) |
| `fetch_latest_ad_creative_defaults()` | 기존 광고에서 텍스트/CTA 템플릿 자동 로드 |
| `upload_to_facebook()` | 업로드 실행 (포맷별 분기) |

**지원하는 광고 포맷**:
| 포맷 | 설명 |
|------|------|
| 단일 영상 | 영상 1개당 광고 1개 생성 |
| 다이내믹-single video | 3가지 사이즈(정방/가로/세로) 묶어서 Flexible Ad 1개 |
| 다이내믹-1x1 (정방) | 1080x1080 영상 최대 10개 → Flexible Ad 1개 |
| 다이내믹-16:9 (가로) | 1920x1080 영상 최대 10개 → Flexible Ad 1개 |
| 다이내믹-9x16 (세로) | 1080x1920 영상 최대 10개 → Flexible Ad 1개 |

**의존**: `facebook_ads.py`의 헬퍼 함수들을 import

---

### 4. `unity_ads.py` — Unity Ads API 핵심 (Test Mode + 공용)

**역할**: Unity Ads API 래핑. 크리에이티브 팩 생성, 플레이어블 업로드, 캠페인 할당.

**주요 기능**:
- 게임별 앱/캠페인 ID 관리 (`UNITY_APP_IDS_ALL`, `UNITY_CAMPAIGN_SET_IDS_ALL` 등)
- 플레이어블 크리에이티브 업로드 (`_unity_create_playable_creative`)
- 크리에이티브 팩 생성 (`_unity_create_creative_pack`)
- 캠페인에 팩 할당 (`_unity_assign_creative_pack`)
- API 호출 (`_unity_get`) — 인증 헤더 + 레이트 리밋 자동 재시도
- API 키 쿼터 초과 시 보조 키로 자동 전환 (`_switch_to_next_key`)

**영상 네이밍 규칙**: `video123_1080x1080`, `video123_1920x1080`, `video123_1080x1920` — 같은 번호의 3가지 사이즈를 1개 팩으로 묶음

**사용 API**: Unity Ads API (`services.api.unity.com/advertise/v1`)

---

### 5. `uni.py` — Unity 마케터 모드

**역할**: `unity_ads.py` 위에 마케터 전용 UI와 기능을 추가한 얇은 래퍼(wrapper).

**마케터 모드 추가 기능**:
- 플랫폼 멀티 선택 (AOS + iOS 동시 처리)
- 캠페인 목록 캐싱
- 이어하기(Resume) 지원 — 이미 할당된 팩 건너뛰기
- 병렬 할당 (최대 2개 캠페인 동시)

---

### 6. `google_ads.py` — Google Ads API 핵심

**역할**: Google Ads API 클라이언트 래핑. UAC(앱 캠페인) 전용.

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `list_campaigns(game)` | 게임별 멀티채널 캠페인 목록 조회 |
| `list_ad_groups_with_spend()` | 광고 그룹 + 7일 지출액 조회 |
| `upload_asset()` | 영상/이미지를 에셋 그룹에 업로드 |
| `distribute_assets_to_categories()` | 에셋을 카테고리별 광고 그룹에 일괄 배포 |

**사용 API**: Google Ads API v20 (gRPC)

---

### 7. `ga.py` — Google Ads 마케터 모드

**역할**: Google Ads 마케터 UI. 카테고리 기반 영상 배포 시스템.

**카테고리 시스템**:
| 카테고리 | 설명 |
|----------|------|
| 일반 | 기본 영상 |
| 로컬라이징 | 지역별 현지화 영상 |
| AI | AI 생성 소재 |
| 인플루언서 | 인플루언서 협업 소재 |

**영상 방향 감지**: 파일명에서 해상도 파싱 → 세로/가로/정방 자동 분류

---

### 8. `mintegral.py` — Mintegral (마케터 전용)

**역할**: Mintegral 미디어 라이브러리에 영상/플레이어블 업로드.

**주요 기능**:
- MD5 기반 인증 토큰 생성
- 크리에이티브 목록 조회 (페이지네이션)
- 오퍼(광고 배치) 목록 조회
- 배치 업로드 (병렬, max 3 workers)
- 게임명 → Mintegral 게임 매핑 (`XP HERO` → `weaponrpg` 등)

**사용 API**: Mintegral API (`ss-api.mintegral.com`)

---

### 9. `applovin.py` — Applovin (마케터 전용)

**역할**: Applovin 미디어 라이브러리 업로드 + 크리에이티브 세트 관리.

**주요 기능**:
- 미디어 라이브러리 배치 업로드 (병렬)
- 캠페인/광고 그룹/크리에이티브 세트 조회
- 크리에이티브 이름 자동 생성 (예: `video100-109_playable456`)

**사용 API**: Applovin Campaign Management API (`api.ads.axon.ai`)

---

### 10. `drive_import.py` — Google Drive 파일 임포트

**역할**: Google Drive 폴더에서 영상/플레이어블 파일을 병렬 다운로드.

**핵심 흐름**:
1. Drive 폴더 URL/ID 입력
2. 폴더 내 파일 목록 조회 (공유 드라이브 지원)
3. ThreadPoolExecutor로 병렬 다운로드 (기본 6~8 workers)
4. 임시 디렉토리에 저장 후 파일 경로 반환

**지원 파일 타입**:
- 영상: `.mp4`, `.mov`, `.mkv`, `.mpeg4`
- 이미지: `.png`, `.jpg`, `.jpeg`, `.gif`, `.webp`
- 플레이어블: `.zip`, `.html`

---

### 11. `game_manager.py` — 게임 목록 관리

**역할**: 게임 드롭다운에 표시할 게임 목록 로드. `games_config.json`에서 커스텀 게임을 추가 가능.

**기본 게임 (12개)**:
Cafe Life, Dino Universe, Snake Clash, Pizza Ready, XP HERO, Suzy's Restaurant, Office Life, Lumber Chopper, Burger Please, Prison Life, Arrow Flow, Downhill Racer

---

### 12. `upload_logger.py` — BigQuery 감사 로그

**역할**: 모든 업로드/임포트 이벤트를 BigQuery에 비동기 기록. UI를 블로킹하지 않도록 백그라운드 스레드에서 실행.

**로그 테이블**: `roas-test-456808.data_check.crema_upload_log`

**기록 이벤트 종류**: `drive_import`, `local_upload`, `fb_upload`, `unity_upload`, `mintegral_library`, `applovin_media_library`, `fb_media_library`, `google_asset_upload`

---

### 13. `devtools.py` — 개발자 모드

**역할**: 개발자 전용 디버깅 도구. URL에 `?dev=1` 추가하거나 secrets에 `developer_mode=true` 설정하면 활성화.

**기능**:
- 예외 발생 시 상세 트레이스백 기록
- 최근 200줄 로그 링버퍼
- UI에 접이식 디버깅 패널 표시

---

### 14. `generate_refresh_token.py` — Google Ads OAuth 토큰 생성

**역할**: 1회성 유틸리티. 브라우저에서 Google OAuth 인증 후 refresh_token을 발급받아 `secrets.toml`에 붙여넣기용.

**사용법**: `python3 modules/upload_automation/generate_refresh_token.py`

---

### 15. `vietnam.py` — 베트남팀 전용 탭

**역할**: Test Mode를 복제한 베트남팀 전용 탭. 독립된 세션 상태(`prefix="vn"`)로 격리. 필요 시 게임 목록이나 API 키를 별도 커스터마이징 가능.

---

### 16. `Document.md` — 기존 아키텍처 문서

아키텍처, 모듈 구조, 광고 포맷, 에러 처리 전략 등 기술 문서.

---

## 파일 간 관계도

```
app.py (루트)
  └─ main.py::run()
       │
       ├─ game_manager ─── 게임 목록 로드
       ├─ drive_import ─── Google Drive 파일 다운로드
       ├─ devtools ─────── 개발자 디버깅 패널
       ├─ upload_logger ── BigQuery 이벤트 로그
       │
       │  [Test Mode]
       ├─ facebook_ads ──── Facebook 캠페인/광고 생성
       ├─ unity_ads ─────── Unity 크리에이티브 팩 생성/할당
       │
       │  [Marketer Mode]
       ├─ fb ────────────── Facebook 마케터 업로드 (→ facebook_ads 호출)
       ├─ uni ──────────── Unity 마케터 업로드 (→ unity_ads 호출)
       ├─ mintegral ────── Mintegral 미디어 라이브러리 업로드
       ├─ applovin ─────── Applovin 미디어 라이브러리 업로드
       └─ ga ──────────── Google Ads 에셋 배포 (→ google_ads 호출)

vietnam.py (별도 탭)
  └─ main.py::render_main_app() 재사용 (prefix="vn")
```

---

## 인증 및 시크릿 구조

모든 API 키/토큰은 `.streamlit/secrets.toml`에 저장됩니다 (Git에 포함되지 않음):

| 섹션 | 용도 |
|------|------|
| `[facebook]` | `access_token` — Meta Graph API 인증 |
| `[unity]` | `organization_id`, `authorization_header` (+ 보조키) — Unity API 인증 |
| `[mintegral]` | `access_key`, `api_key` — Mintegral API 인증 (MD5 토큰 생성) |
| `[applovin]` | `campaign_management_api_key`, `reporting_api_key` — Applovin API 인증 |
| `[google_ads]` | `developer_token`, `client_id`, `client_secret`, `refresh_token`, `customer_id` |
| `[gcp_service_account]` | GCP 서비스 계정 JSON — Drive API + BigQuery 인증 |

---

## 에러 처리 패턴

모든 플랫폼 모듈이 동일한 3단계 패턴을 따릅니다:

```python
try:
    # API 호출
except Exception as e:
    st.error("간결한 한국어 에러 메시지")         # 1. 사용자에게 보여줌
    devtools.record_exception("컨텍스트", e)      # 2. 개발자 모드에 기록
    log_event(..., error_message=str(e))          # 3. BigQuery에 감사 로그
```

---

## 알아둬야 할 제약사항

| 플랫폼 | 제약 |
|--------|------|
| Facebook | 대만 타겟팅은 API로 불가 (수동 설정 필요), Flexible Ad 텍스트 최대 5개, 다이내믹 영상 최대 10개 |
| Unity | API 키 쿼터 초과 시 보조 키 필요, 캠페인 세트는 미리 생성되어 있어야 함, 플레이어블은 .zip/.html만 |
| Google Ads | UAC(멀티채널) 캠페인만 지원, refresh_token 만료 시 `generate_refresh_token.py`로 재발급 |
| 파일 임포트 | 로컬 업로드는 ~12개 제한 (메모리), 대용량은 Drive 권장, 중복 제거는 대소문자 무시 |

---

## 처음 세팅할 때

1. `.streamlit/secrets.toml` 설정 (위 인증 섹션 참고)
2. `pip install -r requirements.txt`
3. `streamlit run app.py` 실행
4. URL에 `?dev=1` 붙여서 개발자 모드 확인
5. Test Mode에서 Facebook 업로드 한 건 따라가보면 전체 흐름 파악 가능
