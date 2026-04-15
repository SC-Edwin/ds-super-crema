"""Meta 게임별 기본값·캠페인 매핑 (하드코딩 ID는 이 모듈에만 둡니다).

`facebook_ads` / `fb`는 `GAME_DEFAULTS`, `FB_GAME_MAPPING`을 여기서 가져옵니다.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Final


@dataclass(frozen=True)
class MetaGameAppDefaultsDTO:
    """게임별 Facebook App ID + 스토어 URL (세션 기본값 시드용)."""

    game_name: str
    fb_app_id: str
    store_url: str


@dataclass(frozen=True)
class MetaGameCampaignMappingDTO:
    """게임별 광고 계정·캠페인·페이지 secrets 키 (Test 업로드 플로우용)."""

    game_name: str
    account_id: str
    campaign_id: str
    campaign_name: str
    adset_prefix: str
    page_id_key: str

    def to_mapping(self) -> Dict[str, Any]:
        return {
            "account_id": self.account_id,
            "campaign_id": self.campaign_id,
            "campaign_name": self.campaign_name,
            "adset_prefix": self.adset_prefix,
            "page_id_key": self.page_id_key,
        }


# ---- 앱 기본값 (fb_app_id / store_url) ----
_META_APP_DEFAULTS: Final[tuple[MetaGameAppDefaultsDTO, ...]] = (
    MetaGameAppDefaultsDTO(
        "XP HERO",
        "519275767201283",
        "https://play.google.com/store/apps/details?id=io.supercent.weaponrpg",
    ),
    MetaGameAppDefaultsDTO(
        "Dino Universe",
        "1665399243918955",
        "https://play.google.com/store/apps/details?id=io.supercent.ageofdinosaurs",
    ),
    MetaGameAppDefaultsDTO(
        "Snake Clash",
        "1205179980183812",
        "https://play.google.com/store/apps/details?id=io.supercent.linkedcubic",
    ),
    MetaGameAppDefaultsDTO(
        "Pizza Ready",
        "1475920199615616",
        "https://play.google.com/store/apps/details?id=io.supercent.pizzaidle",
    ),
    MetaGameAppDefaultsDTO(
        "Cafe Life",
        "1343040866909064",
        "https://play.google.com/store/apps/details?id=com.fireshrike.h2",
    ),
    MetaGameAppDefaultsDTO(
        "Suzy's Restaurant",
        "836273807918279",
        "https://play.google.com/store/apps/details?id=com.corestudiso.suzyrest",
    ),
    MetaGameAppDefaultsDTO(
        "Office Life",
        "1570824996873176",
        "https://play.google.com/store/apps/details?id=com.funreal.corporatetycoon",
    ),
    MetaGameAppDefaultsDTO(
        "Lumber Chopper",
        "2824067207774178",
        "https://play.google.com/store/apps/details?id=dasi.prs2.lumberchopper",
    ),
    MetaGameAppDefaultsDTO(
        "Burger Please",
        "2967105673598896",
        "https://play.google.com/store/apps/details?id=io.supercent.burgeridle",
    ),
    MetaGameAppDefaultsDTO(
        "Prison Life",
        "6564765833603067",
        "https://play.google.com/store/apps/details?id=io.supercent.prison",
    ),
    MetaGameAppDefaultsDTO(
        "Arrow Flow",
        "1178896120788157",
        "https://play.google.com/store/apps/details?id=com.hg.arrow&hl=ko",
    ),
    MetaGameAppDefaultsDTO(
        "Roller Disco",
        "579397764432053",
        "https://play.google.com/store/apps/details?id=com.Albus.RollerDisco",
    ),
    MetaGameAppDefaultsDTO(
        "Waterpark Boys",
        "957490872253064",
        "https://play.google.com/store/apps/details?id=com.Albus.WaterParkBoys",
    ),
    MetaGameAppDefaultsDTO(
        "Downhill Racer",
        "1332540784297154",
        "https://play.google.com/store/apps/details?id=io.supercent.downhill",
    ),
)

GAME_DEFAULTS: Dict[str, Dict[str, str]] = {
    row.game_name: {"fb_app_id": row.fb_app_id, "store_url": row.store_url}
    for row in _META_APP_DEFAULTS
}


# ---- 계정·캠페인 매핑 ----
_META_CAMPAIGN_MAPPINGS: Final[tuple[MetaGameCampaignMappingDTO, ...]] = (
    MetaGameCampaignMappingDTO(
        "XP HERO",
        "act_692755193188182",
        "120218934861590118",
        "weaponrpg_aos_facebook_us_creativetest",
        "weaponrpg_aos_facebook_us_creativetest",
        "page_id_xp",
    ),
    MetaGameCampaignMappingDTO(
        "Dino Universe",
        "act_1400645283898971",
        "120203672340130431",
        "ageofdinosaurs_aos_facebook_us_test_6th+",
        "ageofdinosaurs_aos_facebook_us_test",
        "page_id_dino",
    ),
    MetaGameCampaignMappingDTO(
        "Snake Clash",
        "act_837301614677763",
        "120201313657080615",
        "linkedcubic_aos_facebook_us_test_14th above",
        "linkedcubic_aos_facebook_us_test",
        "page_id_snake",
    ),
    MetaGameCampaignMappingDTO(
        "Pizza Ready",
        "act_939943337267153",
        "120200161907250465",
        "pizzaidle_aos_facebook_us_test_12th+",
        "pizzaidle_aos_facebook_us_test",
        "page_id_pizza",
    ),
    MetaGameCampaignMappingDTO(
        "Cafe Life",
        "act_1425841598550220",
        "120231530818850361",
        "cafelife_aos_facebook_us_creativetest",
        "cafelife_aos_facebook_us_creativetest",
        "page_id_cafe",
    ),
    MetaGameCampaignMappingDTO(
        "Suzy's Restaurant",
        "act_953632226485498",
        "120217220153800643",
        "suzyrest_aos_facebook_us_creativetest",
        "suzyrest_aos_facebook_us_creativetest",
        "page_id_suzy",
    ),
    MetaGameCampaignMappingDTO(
        "Office Life",
        "act_733192439468531",
        "120228464454680636",
        "corporatetycoon_aos_facebook_us_creativetest",
        "corporatetycoon_aos_facebook_us_creativetest",
        "page_id_office",
    ),
    MetaGameCampaignMappingDTO(
        "Lumber Chopper",
        "act_1372896617079122",
        "120224569359980144",
        "lumberchopper_aos_facebook_us_creativetest",
        "lumberchopper_aos_facebook_us_creativetest",
        "page_id_lumber",
    ),
    MetaGameCampaignMappingDTO(
        "Burger Please",
        "act_3546175519039834",
        "120200361364790724",
        "burgeridle_aos_facebook_us_test_30th+",
        "burgeridle_aos_facebook_us_test",
        "page_id_burger",
    ),
    MetaGameCampaignMappingDTO(
        "Prison Life",
        "act_510600977962388",
        "120212520882120614",
        "prison_aos_facebook_us_install_test",
        "prison_aos_facebook_us_install_test",
        "page_id_prison",
    ),
    MetaGameCampaignMappingDTO(
        "Arrow Flow",
        "act_24856362507399374",
        "120240666247060394",
        "arrow_aos_facebook_us_test",
        "arrow_aos_facebook_us_test",
        "page_id_arrow",
    ),
    MetaGameCampaignMappingDTO(
        "Roller Disco",
        "act_505828195863528",
        "120216262440630087",
        "rollerdisco_aos_facebook_us_creativetest",
        "rollerdisco_aos_facebook_us_creativetest",
        "page_id_roller",
    ),
    MetaGameCampaignMappingDTO(
        "Waterpark Boys",
        "act_1088490002247518",
        "120209343960830376",
        "WaterParkBoys_aos_facebook_us_test",
        "WaterParkBoys_aos_facebook_us_test",
        "page_id_water",
    ),
    MetaGameCampaignMappingDTO(
        "Downhill Racer",
        "act_347210305097775",
        "",
        "",
        "downhill_aos_facebook_us_test",
        "page_id_downhill",
    ),
)

FB_GAME_MAPPING: Dict[str, Dict[str, Any]] = {
    row.game_name: row.to_mapping() for row in _META_CAMPAIGN_MAPPINGS
}

# `init_fb_from_secrets(None)` 시 사용 (XP HERO와 동일 계정)
DEFAULT_FB_AD_ACCOUNT_ID: Final[str] = next(
    r.account_id for r in _META_CAMPAIGN_MAPPINGS if r.game_name == "XP HERO"
)

__all__ = [
    "DEFAULT_FB_AD_ACCOUNT_ID",
    "FB_GAME_MAPPING",
    "GAME_DEFAULTS",
    "MetaGameAppDefaultsDTO",
    "MetaGameCampaignMappingDTO",
]
