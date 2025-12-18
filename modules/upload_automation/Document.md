# Upload Automation (Facebook + Unity) — Module Guide

This folder powers the Streamlit “Creative 자동 업로드” app: it imports videos (typically from Drive), then uploads/creates creatives and ads for **Meta (Facebook)** and **Unity Ads**.

The codebase has two “faces”:
- **Test Mode (OPS / Operator)**: full-access flows (create adsets/campaign structures when needed).
- **Marketer Mode**: constrained flows for marketers (select an existing destination, then upload into it).

---

## Entry points / high-level architecture

### `app.py` (root)
- Streamlit shell + global UI theme.
- Calls into the upload automation app (via `modules/upload_automation/main.py`).

### `modules/upload_automation/main.py`
The **UI orchestrator** and routing hub.

- **Routing**
  - `run()` renders two buttons (“Test”, “Marketer”), then routes to:
    - `render_main_app("Test Mode", fb_ops, uni_ops, is_marketer=False)`
    - `render_main_app("Marketer Mode", fb_marketer, uni_marketer, is_marketer=True)`
- **Imports**
  - OPS modules:
    - `modules/upload_automation/facebook_ads.py` as `fb_ops`
    - `modules/upload_automation/unity_ads.py` as `uni_ops`
  - Marketer modules:
    - `modules/upload_automation/fb.py` as `fb_marketer`
    - `modules/upload_automation/uni.py` as `uni_marketer`
- **Dev mode**
  - Always mounts `devtools.render_dev_panel()` and hides most noisy status banners unless `?dev=1` or `developer_mode=true` in secrets.

Conceptually:

```text
app.py
  └─ modules/upload_automation/main.py
        ├─ Test Mode:    facebook_ads.py + unity_ads.py
        └─ Marketer Mode: fb.py + uni.py
```

---

## Developer-mode logging / error policy

### `modules/upload_automation/devtools.py`
- `dev_enabled()`: toggled by `?dev=1` or secrets `developer_mode=true`.
- `record_exception(context, exc)`: always logs server-side; in dev mode also stores tracebacks in `st.session_state`.
- `render_dev_panel()`: shows recent exceptions + recent logs **only in dev mode**.

Design rule: **users see concise error messages only; detailed tracebacks/logs live in dev mode** [[memory:12358019]].

---

## Facebook / Meta modules

### What’s “OPS” vs “Marketer” on Facebook?

- **OPS (Test Mode)**: `facebook_ads.py`
  - Lower-level Meta SDK/Graph integration.
  - Can create ad sets for creative testing, build targeting, etc.
  - Owns reusable primitives like thumbnail extraction/upload and (resumable) video upload.

- **Marketer**: `fb.py`
  - Streamlit settings panel and upload orchestration for marketer workflows.
  - When a campaign/adset is selected: **uploads into the selected AdSet** without creating new campaign structures.
  - Implements the “mimic test mode upload” behavior while applying marketer requirements (template copying, flexible ad formats, validations, naming conventions, progress).

### `modules/upload_automation/facebook_ads.py` (OPS / core API layer)
Core responsibilities:

- **Secrets → SDK initialization**
  - Reads `st.secrets` (access token, account/page IDs, per-game defaults).
  - Initializes Facebook Business SDK objects and maps.

- **Media primitives**
  - `extract_thumbnail_from_video(...)`: generates a thumbnail frame.
  - `upload_thumbnail_image(...)`: uploads image via Graph and returns URL/hash usable in creatives.
  - Video upload helpers (including retry/polling patterns).

- **Campaign/AdSet/Creative helpers**
  - Creates/updates entities via Facebook Marketing API.
  - `validate_page_binding(...)`: checks Page and returns Page-backed Instagram business account id (PBIA) if present.
  - Default store URL logic and other per-game defaults.

This module is intended to be reusable by both OPS and Marketer flows.

### `modules/upload_automation/fb.py` (Marketer-side Facebook flow)
Core responsibilities:

- **Settings UI**
  - Campaign / AdSet selection (Marketer Mode).
  - “Ad Format” selection:
    - `단일 영상`
    - `다이내믹-single video`
    - `다이내믹-1x1`
    - `다이내믹-9x16`
    - `다이내믹-16:9`
  - Primary texts / headlines list editing.
  - CTA selection.
  - Store URL derived from AdSet promoted_object (preferred) and sanitized.
  - Multi-advertiser ads toggle:
    - UI default is OFF.
    - Upload defaults to `contextual_multi_ads.enroll_status=OPT_OUT`.

- **Template “mimic” behavior (Marketer Mode)**
  - When an AdSet is selected, the app fetches a template from the **highest-numbered ACTIVE ad** in that AdSet and copies:
    - primary texts
    - headlines
    - CTA
    - store URL (as fallback; AdSet promoted_object store URL is prioritized for consistency)
  - Key helper: `fetch_latest_ad_creative_defaults(adset_id)` (cached, active-only).

- **Upload orchestration**
  - `upload_to_facebook(game_name, uploaded_files, settings)` is the entry called by `main.py`.
  - It prepares `account/page_id`, chooses the correct ad-creation path and calls:
    - `upload_videos_to_library_and_create_single_ads(...)`
      - dispatches based on `settings["dco_aspect_ratio"]`

#### Facebook “Ad Format” behaviors (in Marketer Mode)

##### 1) `단일 영상`
- Uses classic single-video creative path (object story spec, single asset).

##### 2) `다이내믹-single video`
- Creates **Flexible Ad Format** creatives (`creative_asset_groups_spec`).
- Input expectation: for each base group `videoxxx`, all 3 sizes must exist:
  - `1080x1080`
  - `1920x1080`
  - `1080x1920`
- If any size missing: raises a user-facing error.
- If multiple groups exist: creates multiple flexible ads while preserving `videoxxx` naming.
- Text assets are truncated to **max 5 per `text_type`** (Facebook requirement).

##### 3) `다이내믹-1x1`
- Validates all videos are `1080x1080`, else error: `비디오 사이즈 체크 바랍니다`.
- Validates count `<= 10`, else error: `다이내믹 광고는 10개이상의 동영상을 수용할 수 없습니다`.
- Creates **one** flexible ad using the uploaded square videos.
- Ad naming:
  - If user specified Ad Name → use it.
  - Else build from filename-derived game name + video ranges:
    - `video481, video483-489_<gamename>_flexible_정방`

##### 4) `다이내믹-16:9`
- Same as `다이내믹-1x1`, but size check is `1920x1080`, suffix is `가로`.

##### 5) `다이내믹-9x16`
- Same as `다이내믹-1x1`, but size check is `1080x1920`, suffix is `세로`.

---

## Unity modules

### `modules/upload_automation/unity_ads.py` (OPS / core Unity API layer)
Core responsibilities:

- **Secrets parsing + derived maps**
  - Reads `st.secrets["unity"]` and builds maps for:
    - `UNITY_APP_IDS_ALL` (per-game per-platform app/title IDs)
    - `UNITY_CAMPAIGN_SET_IDS_ALL` (campaign set IDs)
    - `UNITY_CAMPAIGN_IDS_ALL` and defaults

- **Settings state**
  - `_ensure_unity_settings_state()` and `get_unity_settings(game)`.

- **Settings UI**
  - `render_unity_settings_panel(...)` renders per-game Unity settings:
    - title/app ID, campaign id, org id
    - playable selection (Drive playable or existing playable on Unity)
  - Stores settings under `st.session_state.unity_settings[game]`.

- **Upload/apply**
  - `upload_unity_creatives_to_campaign(...)`: uploads videos/playables and creates/updates creative packs.
  - `apply_unity_creative_packs_to_campaign(...)`: attaches created packs to target campaigns.

- **Dev mode**
  - Uses `devtools.record_exception(...)` for detailed logging in dev mode while keeping user UI concise.

### `modules/upload_automation/uni.py` (Marketer-side Unity)
This module is a **thin marketer wrapper** over `unity_ads.py`:

- Re-exports:
  - `get_unity_settings`
  - `preview_unity_upload`
  - `apply_unity_creative_packs_to_campaign`
  - `upload_unity_creatives_to_campaign`
- Contains marketer-friendly cached queries (campaign lists, playables lists) and simplified behavior.

---

## Operational notes / invariants

- **AdSet store URL consistency (Facebook)**: when available, AdSet promoted_object `object_store_url` is prioritized to avoid API mismatch errors.
- **Flexible Ad Format text limits (Facebook)**: max **5** assets per `text_type` (`primary_text`, `headline`).
- **Dev-mode visibility**:
  - Normal mode: minimal UI + progress + concise errors.
  - Dev mode: additional status banners + logs + tracebacks.


