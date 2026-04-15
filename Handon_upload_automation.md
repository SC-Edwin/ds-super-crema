# Upload Automation 모듈 인수인계 문서

## 목차

1. [개요](#개요)
2. [파일 구조](#파일-구조)
3. [전체 데이터 흐름](#전체-데이터-흐름)
4. [main.py — 메인 UI 허브](#1-mainpy--메인-ui-허브)
5. [Facebook 모듈](#2-facebook-모듈)
6. [Unity 모듈](#3-unity-모듈)
7. [Google Ads 모듈](#4-google-ads-모듈)
8. [Mintegral 모듈](#5-mintegral-모듈)
9. [Applovin 모듈](#6-applovin-모듈)
10. [유틸리티 모듈](#7-유틸리티-모듈)
11. [세션 상태 전체 구조](#8-세션-상태-전체-구조)
12. [인증 및 시크릿 구조](#9-인증-및-시크릿-구조)
13. [에러 처리 패턴](#10-에러-처리-패턴)
14. [제약사항 및 주의점](#11-제약사항-및-주의점)
15. [처음 세팅 가이드](#12-처음-세팅-가이드)

---

## 개요

이 폴더는 **광고 소재(영상/플레이어블)를 여러 광고 플랫폼에 업로드하는 자동화 시스템**입니다.
Streamlit 기반이며, 두 가지 모드로 운영됩니다:

| 모드 | 대상 | 기능 범위 |
|------|------|----------|
| **Test Mode** | 운영팀 (OPS) | 캠페인/광고세트 생성 + 소재 업로드 (Facebook, Unity만) |
| **Marketer Mode** | 마케터 | 기존 캠페인에 소재 업로드 (Facebook, Unity, Mintegral, Applovin, Google Ads) |

---

## 파일 구조

```
upload_automation/
│
├── main.py                    ← 메인 UI (탭 라우팅, 파일 임포트, 업로드 실행)
├── __init__.py                ← run() 함수 export (from .main import run)
│
│  ── Facebook ──
├── facebook_ads.py            ← Facebook API 핵심 함수 (Test Mode + 공용 헬퍼)
├── fb.py                      ← Facebook 마케터 모드 (UI + 업로드 로직)
│
│  ── Unity ──
├── unity_ads.py               ← Unity Ads API 핵심 함수 (Test Mode + 공용 헬퍼)
├── uni.py                     ← Unity 마케터 모드 (UI + 업로드 로직)
│
│  ── Google Ads ──
├── google_ads.py              ← Google Ads API 핵심 함수 (GAQL 쿼리, 에셋 업로드)
├── ga.py                      ← Google Ads 마케터 모드 (카테고리 기반 배포 UI)
│
│  ── 기타 플랫폼 ──
├── mintegral.py               ← Mintegral 크리에이티브 세트 관리 (마케터 전용)
├── applovin.py                ← Applovin 크리에이티브 세트 관리 (마케터 전용)
│
│  ── 유틸리티 ──
├── drive_import.py            ← Google Drive 폴더에서 파일 병렬 다운로드
├── game_manager.py            ← 게임 목록 관리 (games_config.json)
├── upload_logger.py           ← BigQuery 감사 로그 (비동기, 백그라운드 스레드)
├── devtools.py                ← 개발자 모드 로깅/디버깅 패널
├── scripts/generate_refresh_token.py  ← Google Ads OAuth 토큰 생성 (1회성 CLI)
└── vietnam.py                 ← 베트남팀 전용 탭 (Test Mode 복제, prefix="vn")
```

---

## 전체 데이터 흐름

### 사용자 조작 흐름

```
1. 모드 선택 (Test / Marketer)
2. 게임 탭 선택 (Cafe Life, XP HERO, ...)
3. 플랫폼 선택 (Facebook, Unity Ads, Mintegral, ...)
4. 파일 임포트 (Google Drive URL 입력 or 로컬 파일 업로드)
5. 우측 패널에서 플랫폼별 설정 (캠페인, 광고세트, 포맷 등)
6. 업로드 버튼 클릭
7. 결과 확인 (성공/실패 카운트, 에러 메시지)
```

### 코드 호출 흐름

```
app.py
  └─ main.py::run()
       ├─ 모드 선택 UI → st.session_state["page"] 설정
       └─ render_main_app(fb_module, unity_module, is_marketer, prefix)
            │
            ├─ 게임 탭 생성 ← game_manager.get_all_game_names()
            │
            ├─ 파일 임포트 (좌측)
            │   ├─ Google Drive → drive_import.import_drive_folder_files_parallel()
            │   └─ 로컬 업로드 → tempfile에 청크 저장 (1MB씩)
            │
            ├─ 설정 패널 (우측) — 플랫폼별 분기
            │   ├─ Facebook  → fb.render_facebook_settings_panel()      [마케터]
            │   │              facebook_ads 직접 사용                    [Test]
            │   ├─ Unity     → uni.render_unity_settings_panel()        [마케터]
            │   │              unity_ads.render_unity_settings_panel()   [Test]
            │   ├─ Mintegral → mintegral.render_mintegral_settings_panel()
            │   ├─ Applovin  → applovin.render_applovin_settings_panel()
            │   └─ Google    → ga.render_google_settings_panel()
            │
            ├─ 업로드 실행 (버튼 클릭 시)
            │   ├─ Facebook  → fb.upload_to_facebook()
            │   ├─ Unity     → uni.upload_unity_creatives_to_campaign()
            │   │              → uni.apply_unity_creative_packs_to_campaign()
            │   ├─ Mintegral → mintegral.upload_to_mintegral()
            │   ├─ Applovin  → applovin._upload_creative_set()
            │   └─ Google    → ga.distribute_by_category()
            │
            ├─ 로그 기록 → upload_logger.log_event()
            └─ 디버그 패널 → devtools.render_dev_panel()
```

---

## 1. main.py — 메인 UI 허브

전체 업로드 자동화 UI의 중심. 게임별 탭 생성, 플랫폼 선택, 파일 임포트, 업로드 실행을 총괄합니다.

### 주요 함수

| 함수 | 파라미터 | 반환값 | 역할 |
|------|---------|--------|------|
| `run()` | 없음 | None | 진입점. 모드 선택 버튼 렌더링 후 `render_main_app()` 호출 |
| `render_main_app()` | `title, fb_module, unity_module, is_marketer, prefix` | None | 게임 탭, 플랫폼 선택, 파일 임포트 UI, 실행 버튼 렌더링 |
| `init_state(prefix)` | `prefix: str` | None | `uploads`, `settings` 세션 상태 초기화 |
| `init_remote_state(prefix)` | `prefix: str` | None | `remote_videos` 세션 상태 초기화 |
| `validate_count(files)` | `files: list` | `(bool, str)` | 파일 개수/확장자 검증 (.mp4, .mpeg4, .html, .zip만 허용) |
| `_run_drive_import()` | `folder_url, max_workers, on_progress` | `list[dict]` | Drive 임포트 래퍼 (진행률 콜백 지원) |
| `_key(prefix, name)` | `prefix, name: str` | `str` | 네임스페이스 세션키 생성 (예: `"vn_uploads"`) |

### 파일 임포트 상세

#### Google Drive 임포트

```
1. 사용자가 Drive 폴더 URL/ID 입력
2. 고급 설정: 병렬 워커 수 조절 (기본 8, 최소 1, 최대 16)
3. "드라이브에서 Creative 가져오기" 버튼 클릭
4. 프로그레스 바 + 로그 박스 생성
5. drive_import.import_drive_folder_files_parallel() 호출
   → ThreadPoolExecutor로 병렬 다운로드
   → 콜백으로 "✅ {파일명}" 또는 "❌ {파일명} — {에러}" 표시
   → 0.3초 간격으로 UI 갱신 (래그 방지)
6. 기존 파일과 합산 후 중복 제거 (파일명 기준, 대소문자 무시)
7. st.session_state[remote_videos][game]에 저장
```

#### 로컬 파일 업로드

```
1. Streamlit file_uploader (mp4, mov, png, jpg, jpeg, zip, html)
2. 제한: 파일 12개까지, 개별 파일 100MB까지 (초과 시 Drive 권장)
3. "로컬 파일 추가하기" 버튼 클릭
4. 각 파일을 1MB 청크로 임시 디렉토리에 저장
5. 기존 파일과 합산 후 중복 제거
6. st.rerun()으로 uploader 위젯 초기화
```

### 플랫폼 라우팅

| 모드 | 사용 가능 플랫폼 |
|------|-----------------|
| Test Mode | Facebook, Unity Ads |
| Marketer Mode | Facebook, Unity Ads, Mintegral, Applovin, Google Ads (설치 시) |

### 상수값

| 상수 | 값 | 설명 |
|------|-----|------|
| `MAX_FILES` | 12 | 로컬 업로드 최대 파일 수 |
| `MAX_SIZE_MB` | 100 | 개별 파일 최대 크기 |
| `CHUNK_SIZE` | 1MB | 파일 쓰기 청크 크기 |
| `allowed` | `.mp4, .mpeg4, .html, .zip` | 업로드 검증 허용 확장자 |
| 기본 workers | 8 | Drive 병렬 다운로드 기본값 |
| 프로그레스 갱신 간격 | 0.3초 | UI 래그 방지 |

---

## 2. Facebook 모듈

### facebook_ads.py — API 핵심 (Test Mode + 공용 헬퍼)

Meta/Facebook Business SDK를 래핑한 저수준 API 함수 모음. Test Mode에서 직접 사용하고, `fb.py`(마케터 모드)에서도 이 함수들을 import합니다.

#### 주요 함수

**SDK 초기화**

| 함수 | 역할 |
|------|------|
| `init_fb_from_secrets(ad_account_id)` | `st.secrets[facebook][access_token]`으로 SDK 초기화, AdAccount 반환. 기본값: XP HERO 계정 |
| `init_fb_game_defaults(prefix)` | 게임별 기본 app_id, store_url을 세션 상태에 세팅 |
| `validate_page_binding(account, page_id)` | Facebook 페이지 ID 유효성 검증 + Instagram 비즈니스 계정 ID 조회 |

**영상 업로드**

| 함수 | 역할 |
|------|------|
| `upload_video_resumable(path)` | 3단계 리쥬머블 업로드 (start → transfer 청크 → finish). 5회 재시도, 지수 백오프 [0, 2, 4, 8, 12초] |
| `wait_for_video_ready(account, video_id, max_wait=300)` | 영상 처리 완료 폴링. 5초 간격 체크, 최대 300초 |
| `extract_thumbnail_from_video(video_path, output_path)` | OpenCV로 영상 중간 프레임 추출 → JPEG 저장 |
| `upload_thumbnail_image(account, image_path)` | 썸네일을 Meta adimages에 업로드 → image_hash 반환 |

**리쥬머블 업로드 상세 흐름:**

```
Phase 1 — Start
  POST /v24.0/{account_id}/advideos
    upload_phase=start, file_size=(bytes), content_category=VIDEO_GAMING
  → upload_session_id, video_id, start_offset, end_offset 반환

Phase 2 — Transfer (반복)
  파일을 start_offset~end_offset 범위로 읽어서 전송
  POST /v24.0/{account_id}/advideos
    upload_phase=transfer, upload_session_id, start_offset, video_file_chunk
  → 새 start_offset, end_offset 반환
  → start_offset == file_size가 될 때까지 반복

Phase 3 — Finish
  POST /v24.0/{account_id}/advideos
    upload_phase=finish, upload_session_id

Phase 4 — 상태 폴링
  GET /v24.0/{video_id}?fields=status
  "READY" | "FINISHED" | "COMPLETED" 될 때까지 반복
  지수 백오프: 1초 → ×1.5 → 최대 8초, 타임아웃 180~240초
```

**광고 생성**

| 함수 | 역할 |
|------|------|
| `upload_videos_create_ads(account, page_id, adset_id, files, ...)` | 영상 업로드 → 크리에이티브 → 광고 생성 파이프라인 (ThreadPoolExecutor) |
| `create_creativetest_adset(account, campaign_id, adset_name, ...)` | ACTIVE 광고세트 생성. 대만(TW) 타겟팅 차단 |
| `_plan_upload(account, campaign_id, adset_prefix, ...)` | 광고세트 이름/예산/스케줄 계획 (드라이런) |

**설정 빌더**

| 함수 | 역할 |
|------|------|
| `build_targeting_from_settings(countries, age_min, settings)` | 지역/연령/OS 타겟팅 딕셔너리 생성. store_url에서 OS 자동 감지 (play.google.com → Android, apps.apple.com → iOS) |
| `compute_budget_from_settings(files, settings, fallback=10)` | 일일 예산 = 영상 수 × 영상당 예산 (USD). 최소 1달러 |
| `sanitize_store_url(raw)` | 스토어 URL 정규화 (Google Play: id 파라미터만 유지, App Store: 쿼리 제거) |
| `requires_special_compliance(countries)` | 대만 등 특수 컴플라이언스 국가 체크 |

#### 게임 기본값 (GAME_DEFAULTS)

12개 이상의 게임에 대한 Facebook App ID와 스토어 URL이 하드코딩되어 있습니다.

```python
GAME_DEFAULTS = {
    "XP HERO": {
        "fb_app_id": "519275767201283",
        "store_url": "https://play.google.com/store/apps/details?id=io.supercent.weaponrpg",
    },
    # ... Cafe Life, Dino Universe, Snake Clash, Pizza Ready 등
}
```

#### 상수

| 상수 | 설명 |
|------|------|
| `COUNTRY_OPTIONS` | 25개+ 국가 드롭다운 (US, CA, JP, KR, ...) |
| `ANDROID_OS_CHOICES` | 안드로이드 OS 버전 필터 (6.0+ ~ 14.0+) |
| `IOS_OS_CHOICES` | iOS 버전 필터 (11.0+ ~ 18.0+) |
| `OPT_GOAL_LABEL_TO_API` | 한국어 라벨 → Meta enum 매핑 (앱 설치수 극대화 → APP_INSTALLS 등) |

---

### fb.py — 마케터 모드

마케터가 사용하는 Facebook 업로드 전체 워크플로우.

#### 핵심 흐름

```
1. 캠페인 선택 (캐시 10분 TTL)
2. 광고세트 선택 (캐시 10분 TTL)
3. 템플릿 소스 선택
   ├─ 빈칸: 수동 입력
   ├─ 🏆 Highest: 광고세트 내 가장 높은 번호 광고에서 텍스트/CTA 자동 추출
   └─ 📄 특정 광고: 해당 광고에서 추출
4. 크리에이티브 설정 (Primary Text, Headlines, CTA, 광고 포맷)
5. 업로드 실행
```

#### 주요 함수

| 함수 | 역할 |
|------|------|
| `render_facebook_settings_panel(container, game, idx)` | 마케터용 설정 UI 전체 렌더링 |
| `fetch_latest_ad_creative_defaults(adset_id)` | 광고세트 내 최고 번호 광고에서 텍스트/CTA/스토어URL 추출 |
| `fetch_active_campaigns_cached(account_id)` | ACTIVE 캠페인 목록 (10분 캐시) |
| `fetch_active_adsets_cached(account_id, campaign_id)` | 광고세트 목록 (DELETED/ARCHIVED 제외, 10분 캐시) |
| `upload_to_facebook(game, files, settings)` | **메인 업로드 함수**. 광고 포맷별 분기 |
| `upload_videos_to_library_and_create_single_ads(...)` | 기존 광고세트에 업로드 (마케터) |
| `upload_all_videos_to_media_library(account, files, max_workers=6)` | 미디어 라이브러리에만 업로드 (광고 생성 없이) |
| `wait_video_ready(video_id, timeout_s=180)` | 영상 처리 완료 폴링 (지수 백오프, 1초→×1.5→최대 8초) |
| `with_retry(fn, tries=4, base_wait=1.0, max_wait=12.0)` | 지수 백오프 재시도 래퍼 |

#### 광고 포맷별 동작

| 포맷 | 입력 조건 | 결과물 |
|------|----------|--------|
| **단일 영상** | 영상 N개 | 영상 1개당 광고 1개 (N개 광고 생성) |
| **다이내믹-single video** | 같은 번호의 3가지 사이즈 (정방/가로/세로) | Flexible Ad 1개 |
| **다이내믹-1x1 (정방)** | 1080x1080 영상 최대 10개 | Flexible Ad 1개 |
| **다이내믹-16:9 (가로)** | 1920x1080 영상 최대 10개 | Flexible Ad 1개 |
| **다이내믹-9x16 (세로)** | 1080x1920 영상 최대 10개 | Flexible Ad 1개 |

#### 영상 해상도 그룹핑 로직

파일명에서 번호와 해상도를 추출하여 그룹핑합니다.

```
파일명 패턴: video{번호}_{해상도}.mp4

예시 입력:
  video164_1080x1080.mp4  (정방)
  video164_1920x1080.mp4  (가로)
  video165_1080x1920.mp4  (세로)

그룹핑 결과:
  video164 → 정방 파일 선택 (우선순위: 1080x1080 > 1920x1080 > 1080x1920)
  video165 → 세로 파일 선택
```

#### 네이밍 규칙

**광고세트 이름 (Test Mode):**
```
{adset_prefix에서 지역 토큰 교체}{ai_접미사}_{순서}{날짜}

예시:
  weaponrpg_aos_facebook_jp_ai_2nd_240315
  ├─ 원본 prefix: weaponrpg_aos_facebook_us_creativetest
  ├─ 지역 교체: us → jp (1개국), 2개국 이상이면 ww
  ├─ AI 접미사: _ai (settings.use_ai일 때)
  ├─ 순서: 2nd (1st, 2nd, 3rd, 4th...)
  └─ 날짜: _240315 (settings.add_launch_date일 때)
```

**광고 이름 (Marketer Mode):**
```
[{접두사}_]{파일명}[_{접미사}]

예시:
  a_video164.mp4       (접두사만)
  video164_mp4_v1      (접미사만)
  a_video164_v1        (둘 다)
```

**Flexible Ad 이름 (다이내믹):**
```
연속 번호를 범위로 압축:
  [481, 483, 484, 485, 486, 487, 488, 489]
  → "video481, video483-489"

  [100, 101, 102, 103, 104, 123]
  → "video123, video100-104"
```

#### 레이트 리밋 처리

```
감지: FacebookRequestError 코드 17(사용자 리밋), 32(API 호출 초과), 4(앱 리밋)
대응: 전역 5분 쿨다운 설정 (st.session_state["fb_rate_limit_until"])
UI: "⚠️ Facebook API 호출 한도에 도달했습니다. {N}분 {N}초 후 자동으로 재시도됩니다."
캐시 보호: 레이트 리밋 에러는 캐시에 저장하지 않음 (빈 결과 캐싱 방지)
```

#### 게임 매핑 (FB_GAME_MAPPING)

```python
FB_GAME_MAPPING = {
    "XP HERO": {
        "account_id": "act_692755193188182",
        "campaign_id": "120218934861590118",
        "campaign_name": "weaponrpg_aos_facebook_us_creativetest",
        "adset_prefix": "weaponrpg_aos_facebook_us_creativetest",
        "page_id_key": "page_id_xp",    # st.secrets에서 참조할 키
    },
    # ... 15개+ 게임
}
```

#### CTA 옵션

```python
FB_CTA_OPTIONS = [
    "INSTALL_MOBILE_APP", "PLAY_GAME", "USE_APP", "DOWNLOAD",
    "SHOP_NOW", "LEARN_MORE", "SIGN_UP", "WATCH_MORE", "NO_BUTTON"
]
```

---

## 3. Unity 모듈

### unity_ads.py — API 핵심 (Test Mode + 공용 헬퍼)

#### API 기본 정보

```
Base URL: https://services.api.unity.com/advertise/v1
인증: Authorization 헤더 (Bearer 토큰)
```

#### 주요 엔드포인트

| 메서드 | 경로 | 용도 |
|--------|------|------|
| GET | `/organizations/{org}/apps/{app}/campaigns` | 캠페인 목록 |
| GET | `/organizations/{org}/apps/{app}/creatives` | 크리에이티브 목록 |
| POST | `/organizations/{org}/apps/{app}/creatives` | 크리에이티브 업로드 (Multipart) |
| GET | `/organizations/{org}/apps/{app}/creative-packs` | 팩 목록 |
| POST | `/organizations/{org}/apps/{app}/creative-packs` | 팩 생성 |
| GET | `.../campaigns/{cid}/assigned-creative-packs` | 캠페인 할당 팩 조회 |
| POST | `.../campaigns/{cid}/assigned-creative-packs` | 팩 캠페인 할당 |
| DELETE | `.../assigned-creative-packs/{id}` | 팩 할당 해제 |

#### 주요 함수

**ID 조회**

| 함수 | 역할 |
|------|------|
| `get_unity_app_id(game, platform="aos")` | 게임+플랫폼별 앱(타이틀) ID 반환 |
| `get_unity_campaign_set_id(game, platform="aos")` | 캠페인세트 ID 반환 |

**API 호출 래퍼**

| 함수 | 재시도 | 백오프 |
|------|--------|--------|
| `_unity_get(path, params)` | 5회 | 2^(attempt+1)초 |
| `_unity_post(path, json_body)` | 8회 | 2^(attempt+1)초 |
| `_unity_put(path, json_body)` | 없음 | - |
| `_unity_delete(path)` | 없음 | - |

**크리에이티브 관리**

| 함수 | 역할 |
|------|------|
| `_unity_create_video_creative(org_id, title_id, video_path, name, language)` | 영상 업로드 (Multipart). 8회 재시도, 5×(attempt+1)초 백오프 |
| `_unity_create_playable_creative(org_id, title_id, playable_path, name, language)` | 플레이어블 업로드 (.html/.zip). 8회 재시도, 3×(attempt+1)초 백오프 |
| `_check_existing_creative(org_id, title_id, name)` | 이름으로 기존 크리에이티브 검색 (중복 방지) |
| `_fetch_all_creatives_map(org_id, title_id)` | 전체 크리에이티브 캐시 `{이름: ID}` (N개 API 호출 대신 1회) |

**팩 관리**

| 함수 | 역할 |
|------|------|
| `_unity_create_creative_pack(org_id, title_id, pack_name, creative_ids, pack_type)` | 크리에이티브 팩 생성. creative_ids = [세로, 가로, 플레이어블] |
| `_check_existing_pack(org_id, title_id, pack_name)` | 이름으로 기존 팩 검색 |
| `_check_existing_pack_by_creatives(org_id, title_id, creative_ids)` | 같은 크리에이티브 조합의 팩 검색 |
| `_unity_assign_creative_pack(org_id, title_id, campaign_id, pack_id)` | 팩을 캠페인에 할당 |
| `_unity_unassign_creative_pack(org_id, title_id, campaign_id, assigned_pack_id)` | 팩 할당 해제 |

**메인 업로드 함수**

| 함수 | 역할 |
|------|------|
| `upload_unity_creatives_to_campaign(game, videos, settings)` | 팩 생성 (이어하기 지원). 세로+가로+플레이어블 묶어서 팩 생성 |
| `apply_unity_creative_packs_to_campaign(game, pack_ids, settings)` | 팩을 캠페인에 할당. Test Mode는 기존 팩 전체 해제 후 할당, Marketer는 추가만 |
| `preview_unity_upload(game, videos, settings)` | 드라이런 (실제 실행 없이 계획 표시) |
| `render_unity_settings_panel(right_col, game, idx, is_marketer, prefix)` | 설정 UI (플랫폼, 캠페인, 플레이어블, 언어 선택) |

#### 크리에이티브 팩 생성 상세 흐름

```
1. 사전 캐싱
   ├─ 전체 크리에이티브 목록 캐시: {이름: ID}
   └─ 전체 팩 목록 캐시: {이름: ID}, {크리에이티브조합: (ID, 이름)}

2. 플레이어블 처리
   ├─ Drive에서 선택한 파일 → 기존 존재 확인 → 없으면 업로드
   └─ 기존 플레이어블 ID 직접 사용

3. 영상 페어링
   파일명에서 base name 추출 (첫 번째 언더스코어 이전)
   ├─ video001_1080x1920.mp4 → base="video001" (세로)
   └─ video001_1920x1080.mp4 → base="video001" (가로)
   → 세로+가로 모두 있어야 팩 생성 가능

4. 팩 생성 루프 (각 페어별)
   a. 팩 이름 생성: "{video_part}_{playable_part}"
      예: "video001_playable001vari"
   b. 캐시에서 동일 이름/조합 팩 검색 → 있으면 재사용
   c. 세로 크리에이티브 업로드 (또는 기존 재사용) → 2초 대기
   d. 가로 크리에이티브 업로드 (또는 기존 재사용) → 2초 대기
   e. 팩 생성: [세로ID, 가로ID, 플레이어블ID] → 2초 대기
   f. 상태 저장 (이어하기용)

5. 에러 시
   ├─ 용량 초과: 루프 중단, 상태 저장, "Creative 개수가 최대입니다" 메시지
   ├─ 레이트 리밋 (429): 루프 중단, 상태 저장, "다시 시도해주세요" 메시지
   └─ 기타: 에러 기록, 다음 팩으로 계속
```

#### API 키 페일오버

```
시크릿 구조:
  unity.authorization_header    → 기본 키
  unity.authorization_header_2  → 보조 키 (선택)

동작:
  1. API 429 응답 수신
  2. 응답에 "quota" 포함 여부 확인
  3. quota 에러 → _switch_to_next_key()로 보조 키 전환
  4. 전환 성공 → 같은 요청 재시도
  5. 키 모두 소진 → "Unity Quota Exceeded (all keys exhausted)" 에러
```

#### 이어하기(Resume) 지원

업로드 상태가 세션에 저장되어 중간에 실패해도 이어서 작업 가능:

```python
# 세션 상태 키: unity_upload_state_{game}_{campaign_id}
{
    "video_creatives": {"video001_1080x1920.mp4": "creative_id_or_None"},
    "playable_creative": "playable_id_or_None",
    "creative_packs": {"video001_playable001vari": "pack_id_or_None"},
    "completed_packs": ["pack_id_1", "pack_id_2"],
    "total_expected": 10
}
```

#### 게임 ID 구조

```python
UNITY_APP_IDS_ALL = {
    "XP HERO": {"aos": "500230240", "ios": "500236189"},
    # ...
}
UNITY_CAMPAIGN_SET_IDS_ALL = {
    "XP HERO": {"aos": "67d0...", "ios": "683d..."},
    # ...
}
UNITY_CAMPAIGN_IDS_ALL = {
    "XP HERO": {"aos": ["id1", "id2"], "ios": ["id3"]},
    # ...
}
```

#### 지원 언어 (19개)

en, ko, ja, zh-CN, zh-TW, fr, de, es, pt, it, id, th, vi, ru, ar, tr, hi, nl, pl

---

### uni.py — 마케터 모드

`unity_ads.py` 위에 마케터 전용 기능을 추가한 래퍼입니다.

**마케터 모드 추가 기능:**

| 기능 | 설명 |
|------|------|
| 플랫폼 멀티 선택 | AOS + iOS 동시 처리 |
| 캠페인 목록 캐싱 | 플랫폼별 캠페인 목록 자동 로드 |
| 이어하기 (Resume) | 이미 할당된 팩 건너뛰기 |
| 병렬 할당 | 최대 2개 캠페인 동시 처리 |
| 기존 팩 유지 | Test Mode와 달리 기존 팩 해제 없이 추가만 |

---

## 4. Google Ads 모듈

### google_ads.py — API 핵심

Google Ads API v20 클라이언트. UAC(앱 캠페인) 전용.

#### 인증

```
인증: OAuth 2.0 (client_id, client_secret, refresh_token)
쿼리 언어: GAQL (Google Ads Query Language)
MCC 지원: login_customer_id 헤더로 매니저 계정 접근
```

#### 주요 함수

**캠페인/광고그룹 조회**

| 함수 | 역할 |
|------|------|
| `list_campaigns(game)` | MULTI_CHANNEL 캠페인 목록. 3회 재시도, 지수 백오프 |
| `list_ad_groups_with_spend(campaign_id, days=7)` | 광고 그룹 + 7일 지출액 (지출 내림차순 정렬) |
| `list_ad_group_videos(campaign_id, ad_group_id)` | 광고그룹 내 영상 에셋 + 성과 라벨 (BEST/GOOD/LOW/LEARNING) |
| `filter_ad_groups_by_category(ad_groups, category)` | 카테고리별 광고그룹 필터링 |

**에셋 업로드**

| 함수 | 역할 |
|------|------|
| `upload_video_asset(video_bytes, display_name)` | 중복 확인 → YouTube 업로드 → 5회 폴링 (VIDEO_NOT_FOUND 대응, 5/10/15/20초 간격) → 에셋 등록 |
| `upload_html5_asset(html5_bytes, display_name)` | 플레이어블(HTML5) 에셋 업로드 (MEDIA_BUNDLE) |
| `_upload_to_youtube(video_bytes, title)` | YouTube Data API v3로 비공개 업로드 |

**광고그룹 조작**

| 함수 | 역할 |
|------|------|
| `mutate_app_ad_videos(ad_resource_name, new_video_assets)` | 광고그룹의 영상 에셋 전체 교체 |
| `add_playable_to_app_ad(ad_resource_name, current, new)` | 플레이어블 추가 (기존 유지) |
| `clone_ad_group(campaign_id, source_id, new_name, new_videos, copy_playables)` | 광고그룹 복제 (새 영상으로) |
| `distribute_videos(campaign_id, new_videos, exception_map, ad_groups)` | 영상 분배 계획 생성 (LOW 성과 제거 → 빈 슬롯 채우기, 최대 20개) |
| `execute_distribution(campaign_id, plan)` | 분배 계획 실행. CONCURRENT_MODIFICATION 에러 시 3회 재시도 |

#### 카테고리 자동 감지

파일명에서 카테고리를 자동 판별합니다:

```
인플루언서: "500-" 또는 "500_"로 시작
AI: 언더스코어 구분자 중 "eli" 포함
로컬라이징: 언어 코드 포함 (fr, jp, cn, kr, th, vn, de, es, pt, id, tr, ar, ru, it, hi, ms, pl, nl, sv)
일반: 위 해당 없음
```

#### 광고그룹당 최대 영상 수: 20개

---

### ga.py — 마케터 모드

카테고리 기반 영상 배포 UI.

#### 카테고리 시스템

| 카테고리 | 한국어 | 설명 |
|----------|--------|------|
| normal | 일반 | 기본 영상 |
| localized | 로컬라이징 | 지역별 현지화 영상 |
| AI | AI | AI 생성 소재 |
| influencer | 인플루언서 | 인플루언서 협업 소재 |

#### 주요 함수

| 함수 | 역할 |
|------|------|
| `render_google_settings_panel(container, game, idx, ...)` | 캠페인 선택 → 광고그룹 로드(지출 순) → 클론 옵션 → 카테고리별 에셋 선택 UI |
| `upload_assets_to_library(game, files, prefix, on_progress)` | 파일을 GA 에셋 라이브러리에 업로드 (.mp4→영상, .html/.zip→플레이어블) |
| `preview_google_upload(game, prefix)` | 카테고리별 배포 계획 미리보기 |
| `distribute_by_category(game, prefix, on_progress)` | 카테고리별 배포 실행 (영상 분배 → 플레이어블 추가 → 자동 클론) |

#### 배포 흐름

```
1. 카테고리별 영상/플레이어블 수집 (일반→로컬→AI→인플루언서 순서)
2. 각 카테고리의 광고그룹 필터링
3. 영상 배포: LOW 성과 에셋 제거 → 빈 슬롯에 새 영상 채우기 (최대 20개)
4. 플레이어블 배포: 각 광고그룹에 추가 (기존 유지)
5. 자동 클론: 미배치 영상이 남으면 광고그룹 복제하여 배포
```

#### 영상 방향 감지

파일명에서 해상도를 파싱하여 방향을 자동 분류합니다:

```
regex: \d{3,4}x\d{3,4}

1080x1920 → 세로 (portrait)
1920x1080 → 가로 (landscape)
1080x1080 → 정방 (square)
```

**방향 변형 자동 선택:** 같은 영상 번호의 다른 방향 변형을 자동 추천합니다.

---

## 5. Mintegral 모듈

### mintegral.py — 크리에이티브 세트 관리 (마케터 전용)

#### API 기본 정보

```
Base URL: https://ss-api.mintegral.com/api/open/v1
Storage URL: https://ss-storage-api.mintegral.com/api/open/v1
인증: MD5 토큰 = md5(api_key + md5(timestamp))
헤더: access-key, token, timestamp
```

#### 주요 함수

**인증 & 설정**

| 함수 | 역할 |
|------|------|
| `_get_api_config()` | `st.secrets["mintegral"]`에서 access_key, api_key 로드 |
| `_generate_token(api_key)` | MD5 토큰 생성: `md5(api_key + md5(timestamp))` |
| `_get_auth_headers()` | 인증 헤더 빌드 |
| `_get_game_mapping(game)` | 게임명 → Mintegral 숏네임 매핑 |

**데이터 조회 (캐시 5분 TTL)**

| 함수 | 역할 |
|------|------|
| `get_creatives(creative_type, game_filter, max_pages)` | 크리에이티브 목록 (병렬 페이지네이션, 5 workers) |
| `get_offers(game_filter, max_pages, only_running)` | 오퍼(광고 배치) 목록. RUNNING/OVER_DAILY_CAP/PARTIALLY_OVER_CAP 상태만 |
| `_fetch_all_creative_sets(game_short, max_pages)` | 게임의 모든 크리에이티브 세트 조회 (오퍼별 병렬) |

**업로드/관리**

| 함수 | 역할 |
|------|------|
| `upload_creative_to_library(file_path, creative_type)` | 단일 파일 업로드 (VIDEO/IMAGE/PLAYABLE) |
| `batch_upload_to_library(files, max_workers=3, on_progress)` | 배치 업로드 (병렬). 확장자로 타입 자동 감지 |
| `upload_to_mintegral(game, videos, settings)` | 메인 오케스트레이터. settings["mode"]로 분기 |
| `_upload_creative_set(game, videos, settings)` | 크리에이티브 세트 생성 (이미지+영상+플레이어블 → 오퍼에 할당) |
| `_copy_creative_sets(game, settings)` | 크리에이티브 세트를 다른 오퍼로 복사 |
| `_delete_creative_sets(game, settings)` | 크리에이티브 세트 삭제 |

**UI**

| 함수 | 역할 |
|------|------|
| `render_mintegral_settings_panel(container, game, idx)` | 3가지 모드 드롭다운 (Upload/Copy/Delete) |
| `_render_upload_creative_set(game, idx, cur)` | 업로드 모드 UI: 세트 이름, 이미지/영상/플레이어블 선택, 오퍼 선택 |
| `_render_copy_creative_set(game, idx, cur)` | 복사 모드 UI: 소스 세트 선택, 타겟 오퍼 선택 |
| `_render_delete_creative_set(game, idx, cur)` | 삭제 모드 UI: DataFrame 표시, 선택 + 확인 체크박스 |

#### 게임 매핑

```python
# 기본 매핑 (시크릿에서 오버라이드 가능)
{
    "XP HERO": ["weaponrpg"],
    "Dino Universe": ["dinouniverse"],
    "Snake Clash": ["snakeclash"],
    "Pizza Ready": ["pizzaready"],
    "Cafe Life": ["cafelife"],
    "Suzy's Restaurant": ["suzyrest"],
    "Office Life": ["officelife"],
    "Lumber Chopper": ["lumberchop"],
    "Burger Please": ["burgerplease"],
    "Prison Life": ["prisonlife"],
}
```

#### Ad Output 코드

크리에이티브 세트 생성 시 사용하는 고정 광고 출력 타입:

```python
[111, 121, 122, 131, 132, 211, 212, 213, 221, 231, 311]
# 111: Native - Image           # 212: Native - Video Landscape
# 121: Interstitial - Full      # 213: Native - Video Square
# 122: Interstitial - Large     # 221: Interstitial - Video
# 131: Banner - Standard        # 231: Banner - Video
# 132: Banner - Large           # 311: Playable
# 211: Native - Video Portrait
```

---

## 6. Applovin 모듈

### applovin.py — 크리에이티브 세트 관리 (마케터 전용)

#### API 기본 정보

```
Base URL: https://api.ads.axon.ai/manage/v1
리포팅: https://r.applovin.com/assetReport
인증: API 키 (campaign_management_api_key)
```

#### 주요 엔드포인트

| 메서드 | 경로 | 용도 |
|--------|------|------|
| GET | `/campaign/list` | 캠페인 목록 (페이지네이션, 최대 2000개) |
| GET | `/asset/list` | 에셋 목록 (VIDEO/HTML) |
| POST | `/asset/upload` | 파일 업로드 → upload_id 반환 |
| GET | `/asset/upload_result` | 업로드 상태 폴링 (최대 30회, 1초 간격) |
| POST | `/creative_set/create` | 크리에이티브 세트 생성 |
| POST | `/creative_set/clone` | 크리에이티브 세트 복제 |
| GET | `/creative_set/list_by_campaign_id` | 캠페인별 크리에이티브 세트 (최대 5000개) |

#### 주요 함수

| 함수 | 역할 |
|------|------|
| `get_campaigns(game)` | LIVE 캠페인 조회 (병렬 페이지네이션, 5 workers) |
| `get_assets(game)` | 에셋 조회. `{videos: [...], playables: [...]}` 분리 반환 (병렬, 10 workers) |
| `_upload_assets_to_media_library(files, max_workers=3)` | 배치 업로드 + 상태 폴링 (FINISHED까지 최대 30초) |
| `_create_creative_set_api(campaign_id, name, video_ids, playable_ids, status, ...)` | 세트 생성 API 호출 |
| `_clone_creative_sets_api(source_campaign_id, target_campaign_id, creative_set_ids, status)` | 세트 복제 |
| `_upload_creative_set(game, idx, status)` | UI 오케스트레이터. Import/Create 모드 분기, 단일/배치 모드 지원 |
| `get_playable_performance(campaign_id, campaign_name)` | 7일 플레이어블 지출 데이터 조회 (리포팅 API) |
| `render_applovin_settings_panel(container, game, idx)` | 설정 UI (캠페인 선택, Create/Import 토글, 영상/플레이어블 선택) |

#### 크리에이티브 네이밍

```
1 영상 + 1 플레이어블: video123_playable456
여러 영상 + 1 플레이어블: video100-109_playable456
1 영상 + 여러 플레이어블: video123_playabletop{N}
여러 영상 + 여러 플레이어블: video100-109_playabletop{N}
```

#### 배치 모드

Create 모드에서 배치 모드를 활성화하면:
- 영상 최대 30개 × 플레이어블 N개 → N×M개 크리에이티브 세트 생성
- 여러 캠페인에 동시 적용

#### 지원 언어 (33개)

ENGLISH, KOREAN, JAPANESE, CHINESE_SIMPLIFIED, CHINESE_TRADITIONAL, FRENCH, GERMAN, SPANISH, PORTUGUESE, ITALIAN, INDONESIAN, THAI, VIETNAMESE, RUSSIAN, ARABIC, TURKISH, HINDI, DUTCH, POLISH, SWEDISH, NORWEGIAN, DANISH, FINNISH, CZECH, ROMANIAN, HUNGARIAN, GREEK, HEBREW, MALAY

#### 지원 국가 (42개)

US, CA, GB, AU, DE, FR, JP, KR, CN, TW, HK, SG, TH, VN, ID, MY, PH, IN, BR, MX, IT, ES, PT, NL, SE, NO, DK, FI, PL, CZ, RU, TR, SA, AE, IL, EG, ZA, NZ, AR, CL, CO, PE

---

## 7. 유틸리티 모듈

### drive_import.py — Google Drive 파일 임포트

#### 상수

```python
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
VIDEO_EXTS  = {".mp4", ".mpeg4", ".mov", ".mkv"}
IMAGE_EXTS  = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
PLAYABLE_EXTS = {".zip", ".html"}
```

#### 주요 함수

| 함수 | 역할 |
|------|------|
| `get_drive_service_from_secrets()` | Drive API 클라이언트 생성. 인증 우선순위: 1) `st.secrets["gcp_service_account"]` 2) 환경변수 `GOOGLE_CREDENTIALS` 3) `GOOGLE_APPLICATION_CREDENTIALS` |
| `extract_drive_folder_id(url_or_id)` | URL에서 폴더 ID 추출 또는 ID 그대로 반환 |
| `list_drive_files_in_folder(service, folder_id, file_type)` | 폴더 내 파일 목록 (공유 드라이브 지원, 페이지당 1000개, `name_natural` 정렬) |
| `download_drive_file_to_tmp(service, file_id, filename_hint, max_retries=5)` | 32MB 청크 스트리밍 다운로드. 지수 백오프 재시도 `min(2^attempt, 30)초` |
| `import_drive_folder_files_parallel(folder_url, file_type, max_workers=6, on_progress)` | 병렬 다운로드 (ThreadPoolExecutor). 워커별 Drive 서비스 생성 (스레드 안전). 실패 파일은 건너뛰고 성공한 파일만 반환 |

---

### game_manager.py — 게임 목록 관리

#### 기본 게임 (12개)

Cafe Life, Dino Universe, Snake Clash, Pizza Ready, XP HERO, Suzy's Restaurant, Office Life, Lumber Chopper, Burger Please, Prison Life, Arrow Flow, Downhill Racer

#### 주요 함수

| 함수 | 역할 |
|------|------|
| `load_custom_config()` | `games_config.json` 로드. 파일 없으면 빈 딕셔너리 |
| `get_all_game_names(include_custom=True)` | 기본 + 커스텀 게임 목록 (중복 제거). Test Mode는 기본만, Marketer는 커스텀 포함 |
| `save_new_game(game_name, fb_account_id, fb_page_id, unity_game_id)` | `games_config.json`에 저장 + `st.cache_data.clear()` |
| `get_game_config(game_name, platform)` | 게임+플랫폼별 설정 딕셔너리 반환 |

---

### upload_logger.py — BigQuery 감사 로그

#### BigQuery 테이블

```
roas-test-456808.data_check.crema_upload_log
```

#### 주요 함수

| 함수 | 역할 |
|------|------|
| `_get_client()` | BigQuery 클라이언트 싱글톤 (lazy init) |
| `_session_id()` | 16자 hex UUID. 세션 내 이벤트 연관용 |
| `_insert_row(client, row)` | 백그라운드 데몬 스레드에서 행 삽입 (UI 블로킹 없음) |
| `log_event(event_type, *, mode, game, platform, ...)` | **메인 로깅 함수**. fire-and-forget (절대 예외 발생 안 함) |

#### 기록하는 이벤트 종류

| event_type | 시점 |
|------------|------|
| `drive_import` | Drive 파일 다운로드 시 |
| `local_upload` | 로컬 파일 업로드 시 |
| `fb_upload` | Facebook 크리에이티브 업로드 시 |
| `unity_upload` | Unity 크리에이티브 팩 생성 시 |
| `mintegral_library` | Mintegral 미디어 라이브러리 업로드 시 |
| `applovin_media_library` | Applovin 미디어 라이브러리 업로드 시 |
| `fb_media_library` | Facebook 미디어 라이브러리 업로드 시 |
| `google_asset_upload` | Google Ads 에셋 업로드 시 |
| `mode_select` | 모드 전환 시 |

#### 자동 수집 정보

```python
row = {
    "log_id": UUID,
    "created_at": UTC ISO,
    "session_id": 16자 hex,
    "user_email": st.session_state["user_email"],
    "user_name": st.session_state["user_name"],
    "user_role": st.session_state["user_role"],
    "login_method": st.session_state["login_method"],
    "user_agent": st.context.headers["User-Agent"],
    # + 전달된 파라미터들
}
# error_message: 최대 2000자, settings_json/result_json: 최대 5000자
```

---

### devtools.py — 개발자 모드

#### 활성화 방법

```
방법 1: URL에 ?dev=1 추가 (또는 ?dev=true, ?dev=yes, ?dev=on)
방법 2: st.secrets["developer_mode"] = true
```

#### 주요 함수

| 함수 | 역할 |
|------|------|
| `dev_enabled()` | 개발자 모드 활성 여부 (쿼리 파라미터 OR 시크릿) |
| `init_dev_logging()` | 루트 로거에 링버퍼 핸들러 설치 (최대 800줄, INFO 레벨) |
| `record_exception(context, exc)` | 예외 기록. 항상 서버 로그에 기록 + dev 모드면 세션에도 저장 (최대 30개) |
| `render_dev_panel()` | 접이식 디버깅 패널 (최근 예외 5개 + 최근 로그 200줄) |

---

### generate_refresh_token.py — Google Ads OAuth 토큰 생성

1회성 CLI 유틸리티. 다른 모듈에서 import하지 않습니다.

```bash
python3 modules/upload_automation/scripts/generate_refresh_token.py
```

**동작:**
1. `.streamlit/secrets.toml`에서 `[google_ads]`의 client_id, client_secret 로드
2. OAuth2 브라우저 인증 플로우 실행 (포트 8080)
3. refresh_token 출력 → `secrets.toml`에 붙여넣기

**스코프:** `adwords` + `youtube.upload`

---

### vietnam.py — 베트남팀 전용 탭

```python
PREFIX = "vn"          # 세션 상태 네임스페이스
GAMES = None           # 기본 게임 목록 사용 (오버라이드 가능)
FB_MODULE = fb_ops     # Facebook 모듈 (교체 가능)
UNITY_MODULE = uni_ops # Unity 모듈 (교체 가능)
```

**역할:** Test Mode를 복제한 독립 탭. `prefix="vn"`으로 세션 상태가 격리되어 메인 탭과 충돌하지 않습니다.

---

## 8. 세션 상태 전체 구조

```
st.session_state
│
├── 전역
│   ├── page                          ← 현재 모드 (Test/Marketer)
│   ├── current_tab_index             ← 선택된 게임 탭
│   ├── log_session_id                ← BigQuery 로그 세션 ID
│   ├── user_email / user_name        ← 로그인 사용자 정보
│   ├── user_role / login_method      ← 권한/로그인 방식
│   ├── _dev_log_buffer               ← 디버그 로그 링버퍼 (800줄)
│   ├── _dev_tracebacks               ← 예외 트레이스백 (30개)
│   └── _dev_log_init                 ← 로깅 초기화 플래그
│
├── 파일 관리 (prefix 가능: vn_uploads 등)
│   ├── {prefix_}uploads              ← {game: [files]}
│   ├── {prefix_}settings             ← {game: {platform_settings}}
│   └── {prefix_}remote_videos        ← {game: [{name, path}]}
│
├── Facebook
│   ├── fb_rate_limit_until           ← 레이트 리밋 쿨다운 타임스탬프
│   ├── fb_c_{idx}                    ← 선택된 캠페인 ID
│   ├── fb_a_{idx}                    ← 선택된 광고세트 ID
│   ├── template_source_{idx}         ← 템플릿 소스 (빈칸/🏆/📄)
│   ├── primary_texts_{idx}           ← 편집 중인 Primary Text 목록
│   ├── headlines_{idx}               ← 편집 중인 Headline 목록
│   ├── mimic_data_{idx}              ← 캐시된 템플릿 데이터
│   └── ig_actor_id_from_page         ← Instagram 비즈니스 계정 ID
│
├── Unity
│   ├── {prefix_}unity_settings       ← {game: {title_id, campaign_id, ...}}
│   ├── {prefix_}unity_created_packs  ← {game: [pack_ids]}
│   ├── unity_upload_state_{game}_{campaign_id}  ← 이어하기 상태
│   └── unity_mkt_platform_{idx}      ← 마케터 모드 플랫폼 선택
│
├── Google Ads
│   ├── google_settings               ← {game: {campaign_id, category_selections, ...}}
│   ├── gads_campaigns_{game}         ← 캠페인 목록 캐시
│   ├── gads_adgroups_{campaign_id}   ← 광고그룹 캐시
│   └── gads_uploaded_assets_{game}   ← 업로드된 에셋 {name: resource_name}
│
├── Mintegral
│   ├── mintegral_settings            ← {game: {mode, selected_offers, ...}}
│   └── mintegral_{type}_data_{idx}   ← 크리에이티브 데이터 캐시
│
└── Applovin
    ├── applovin_settings             ← {game: {campaigns, videos, playables, ...}}
    ├── applovin_campaigns_{game}     ← 캠페인 목록 캐시
    └── applovin_assets_{game}        ← 에셋 {videos: [...], playables: [...]}
```

---

## 9. 인증 및 시크릿 구조

모든 API 키/토큰은 `.streamlit/secrets.toml`에 저장됩니다 (Git에 포함되지 않음).

### secrets.toml 가져오는 방법

로컬에서 직접 만들 필요 없이, **Streamlit Cloud의 Settings에서 복사**하면 됩니다:

1. [Streamlit Cloud](https://share.streamlit.io/) 접속
2. 해당 앱의 Settings → Secrets 탭
3. `dev-eader` 또는 `main` 브랜치의 시크릿 내용을 복사
4. 로컬 `.streamlit/secrets.toml` 파일에 붙여넣기

> `dev-eader`와 `main` 브랜치에 각각 별도의 시크릿이 설정되어 있을 수 있으니, 작업 중인 브랜치에 맞는 시크릿을 사용하세요.

### API 토큰/키 갱신 주의사항

아래 토큰들은 **유효기간이 있거나 갱신이 필요**할 수 있습니다. 업로드 시 인증 에러가 발생하면 해당 토큰 만료를 의심하세요.

| 시크릿 | 갱신 주기/조건 | 갱신 방법 |
|--------|--------------|----------|
| `[facebook] access_token` | 만료 시 (보통 60일) | Meta Business Suite에서 새 토큰 발급 후 교체 |
| `[unity] authorization_header` | 만료 시 | Unity Dashboard에서 새 API 키 발급 |
| `[unity] authorization_header_2` | 위와 동일 | 보조 키도 같이 갱신 |
| `[google_ads] refresh_token` | 취소/만료 시 | `python3 modules/upload_automation/scripts/generate_refresh_token.py` 실행 |
| `[mintegral] api_key` | 변경 시 | Mintegral 대시보드에서 확인 |
| `[applovin] campaign_management_api_key` | 변경 시 | Applovin 대시보드에서 확인 |
| `[gcp_service_account]` | 키 로테이션 시 | GCP 콘솔에서 새 서비스 계정 키 JSON 발급 |

> 로컬 `secrets.toml`을 갱신했다면, **Streamlit Cloud의 Settings → Secrets도 같이 업데이트**해야 배포 환경에 반영됩니다.

```toml
# ── Facebook ──
[facebook]
access_token = "EAA..."
page_id_xp = "123456789"            # XP HERO 페이지 ID
page_id_cafe = "987654321"          # Cafe Life 페이지 ID
# ... 게임별 page_id_* 키

# ── Unity ──
[unity]
organization_id = "..."
client_id = "..."
client_secret = "..."
authorization_header = "Bearer ..."   # 기본 API 키
authorization_header_2 = "Bearer ..."  # 보조 API 키 (쿼터 페일오버용)

[unity.game_ids.XP_HERO]
aos_app_id = "500230240"
ios_app_id = "500236189"
# ... 게임별

[unity.campaign_sets.XP_HERO]
aos = "67d0..."
ios = "683d..."

[unity.campaign_ids.XP_HERO]
aos = ["id1", "id2"]
ios = ["id3"]

# ── Mintegral ──
[mintegral]
access_key = "..."
api_key = "..."

[mintegral.game_mappings]
XP_HERO = ["weaponrpg"]
# ... 게임별

# ── Applovin ──
[applovin]
campaign_management_api_key = "..."
reporting_api_key = "..."
account_id = "..."

[applovin.game_mapping]
# 게임별 매핑

# ── Google Ads ──
[google_ads]
developer_token = "..."
client_id = "..."
client_secret = "..."
refresh_token = "..."
customer_id = "1234567890"
login_customer_id = "..."            # MCC 계정용 (선택)

[google_ads.game_mapping]
# 게임별 코드네임

# ── GCP 서비스 계정 (Drive + BigQuery) ──
[gcp_service_account]
type = "service_account"
project_id = "roas-test-456808"
private_key = "-----BEGIN PRIVATE KEY-----\n..."
client_email = "...@....iam.gserviceaccount.com"
# ... 표준 서비스 계정 JSON 필드

# ── 개발자 모드 ──
developer_mode = false
```

---

## 10. 에러 처리 패턴

### 공통 3단계 패턴

모든 플랫폼 모듈이 동일한 패턴을 따릅니다:

```python
try:
    result = platform_upload_function(...)
except Exception as e:
    st.error("간결한 한국어 에러 메시지")          # 1. 사용자에게 보여줌
    devtools.record_exception("컨텍스트", e)       # 2. 개발자 모드에 기록
    upload_logger.log_event(..., error_message=str(e))  # 3. BigQuery에 감사 로그
```

### 플랫폼별 재시도 전략

| 플랫폼 | 재시도 횟수 | 백오프 전략 | 특이사항 |
|--------|------------|-----------|---------|
| Facebook (영상 업로드) | 5회 | [0, 2, 4, 8, 12]초 | 코드 390은 일시적 에러로 재시도 |
| Facebook (영상 폴링) | 타임아웃 180초 | 1초→×1.5→최대 8초 | |
| Facebook (일반 API) | 4회 | 1초→×2→최대 12초 | 레이트 리밋 시 5분 글로벌 쿨다운 |
| Unity (POST) | 8회 | 2^(n+1)초 | 쿼터 에러 시 보조 키 전환 |
| Unity (GET) | 5회 | 2^(n+1)초 | |
| Unity (영상 업로드) | 8회 | 5×(n+1)초 | 용량 초과 시 즉시 중단 |
| Unity (플레이어블) | 8회 | 3×(n+1)초 | |
| Google Ads (캠페인 조회) | 3회 | 2^n초 | INTERNAL/500 에러 시 클라이언트 재생성 |
| Google Ads (영상 폴링) | 5회 | [5, 10, 15, 20]초 | VIDEO_NOT_FOUND 전용 |
| Google Ads (배포 실행) | 3회 | 2~4초 | CONCURRENT_MODIFICATION 전용 |
| Drive (파일 다운로드) | 5회 | min(2^n, 30)초 | |

### 용량/리밋 에러 키워드

```
Unity: "최대", "capacity", "full", "maximum", "exceeded", "limit"
Facebook: 코드 17(사용자), 32(API), 4(앱)
Google Ads: CONCURRENT_MODIFICATION, VIDEO_NOT_FOUND
```

---

## 11. 제약사항 및 주의점

### Facebook

| 제약 | 설명 |
|------|------|
| 대만 타겟팅 불가 | API로 TW 타겟팅 생성 시 차단됨. 수동 설정 필요 |
| Flexible Ad 텍스트 제한 | 타입당 최대 5개 (Meta 요구사항) |
| 다이내믹 영상 제한 | 사이즈별 최대 10개 |
| 스토어 URL 우선순위 | AdSet의 promoted_object URL > 크리에이티브 URL |

### Unity

| 제약 | 설명 |
|------|------|
| API 키 쿼터 | 보조 키 없으면 쿼터 소진 시 작업 불가 |
| 캠페인 세트 사전 생성 | 앱에서 캠페인 세트를 생성하지 않음 (Unity 대시보드에서 미리 생성) |
| 플레이어블 포맷 | .zip 또는 .html만 지원 |
| 영상 페어링 필수 | 세로(1080x1920) + 가로(1920x1080) 둘 다 있어야 팩 생성 |

### Google Ads

| 제약 | 설명 |
|------|------|
| UAC 전용 | MULTI_CHANNEL(앱 캠페인)만 지원 |
| 광고그룹당 영상 제한 | 최대 20개 |
| refresh_token 만료 | 만료 시 `scripts/generate_refresh_token.py`로 재발급 필요 |

### Mintegral

| 제약 | 설명 |
|------|------|
| Ad Output 고정 | 11종 고정 코드 (커스터마이징 불가) |
| 게임 매핑 필수 | 숏네임 매핑 없으면 크리에이티브 필터링 안 됨 |

### Applovin

| 제약 | 설명 |
|------|------|
| 업로드 폴링 | 최대 30초. 초과 시 실패 처리 |
| 배치 모드 영상 제한 | 최대 30개 |

### 파일 임포트 공통

| 제약 | 설명 |
|------|------|
| 로컬 업로드 | 파일 12개, 개별 100MB까지 (초과 시 Drive 권장) |
| 중복 제거 | 파일명 기준, 대소문자 무시 |
| 지원 포맷 | 영상: .mp4, .mov, .mkv, .mpeg4 / 이미지: .png, .jpg, .jpeg, .gif, .webp / 플레이어블: .zip, .html |

---

## 12. 처음 세팅 가이드

```bash
# 1. 의존성 설치
pip install -r requirements.txt

# 2. 시크릿 파일 설정 (위 '인증 및 시크릿 구조' 섹션 참고)
# .streamlit/secrets.toml 생성

# 3. GCP 인증 (Drive + BigQuery용)
gcloud auth application-default login

# 4. Google Ads refresh_token 발급 (최초 1회)
python3 modules/upload_automation/scripts/generate_refresh_token.py

# 5. 앱 실행
streamlit run app.py

# 6. 개발자 모드 확인
# 브라우저에서 URL 뒤에 ?dev=1 추가
```

### 추천 학습 순서

1. Test Mode에서 Facebook 업로드 1건 따라가보기 (전체 흐름 파악)
2. Marketer Mode에서 각 플랫폼 설정 패널 둘러보기
3. `devtools.py` + `upload_logger.py`로 디버깅/모니터링 방법 이해
4. 새 게임 추가 시: `game_manager.py` → 각 플랫폼 모듈의 게임 매핑에 추가

---

## 13. 리팩토링 TODO (구현 관점)

운영 모드(Test/Marketer) 지원 범위와 무관하게, 코드 구조 단순화와 유지보수성을 위해 아래 항목을 순차적으로 진행합니다.

### 공통 네트워크/재시도 레이어

- [x] `modules/upload_automation/network/http_client.py` 추가
  - `request_with_retry()` 공통 함수
  - `HttpRequestError` 표준 예외
- [x] Unity API 헬퍼(`_unity_get/_post/_put/_delete`) 및 멀티파트 크리에이티브 생성(`_unity_create_*`)를 공통 레이어 기반으로 전환
- [x] Mintegral 주요 API 호출을 `_mt_request()`를 통해 공통 레이어로 통합
- [x] Applovin 주요 API 호출을 `_applovin_request()`를 통해 공통 레이어로 통합
- [x] Facebook Graph `POST` 경로(`facebook_ads.py`, `fb.py`)를 `request_with_retry`로 이관 (`fb.py`는 `requests.Session` 연결 재사용을 `session=` 인자로 유지)
- [ ] 공통 Retry 정책 표준화 (기본 backoff, 429/5xx 재시도, timeout 프로파일)

### 설정/시크릿 접근 표준화

- [ ] 플랫폼별 `st.secrets` 접근 로직을 공통 `config` 모듈로 집약
- [ ] 필수 키 누락 시 early-fail 검증 함수 추가 (메시지/진단 포맷 통일)
- [ ] 로그-safe 시크릿 요약 포맷 표준화

### 업로드 파이프라인/타입 정리

- [ ] 공통 업로드 단계 추상화: validate -> upload -> poll -> aggregate
- [ ] `TypedDict`/`dataclass`로 `settings`, `upload file`, `result` 구조 명시
- [ ] 플랫폼별 결과 payload 스키마 통일 (`success/failed/errors/details`)

### 상태/로깅/에러 처리 정리

- [ ] 세션 키 네이밍 상수화 (`state_keys.py`) 및 키 생성기 일원화
- [ ] `upload_logger.log_event()` payload 필드 표준 계약 정의
- [ ] 플랫폼별 문자열 에러를 공통 에러 코드로 정규화 (`RATE_LIMIT`, `AUTH_FAILED`, `INVALID_ID` 등)

### 디렉터리 구조 재편 (flat 완화)

- [ ] `platforms/{facebook,unity,mintegral,applovin,google_ads}/` 구조로 분리
- [ ] `shared/` 아래 공통 모듈(`network`, `config`, `state`, `errors`) 이동
- [ ] 기존 import 경로 하위호환 레이어 제공 후 단계적 정리
