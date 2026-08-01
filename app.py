import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import requests
import logging
import math
import threading
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
import pytz

# ─────────────────────────────────────────────
#  1. GLOBAL CONFIGURATION
# ─────────────────────────────────────────────
AIRPORT_ICAO             = "YBBN"
TIMEZONE                 = "Australia/Brisbane"
LOOKBACK_HOURS           = 4
LOOKAHEAD_HOURS          = 8
RECENT_LANDED_MAX        = 60   # minutes — fade out after this
GAP_MIN_MINUTES          = 20   # minimum gap size to display
GAP_DISPLAY_MIN          = 5    # minimum remaining time in gap to display
HEAVY_DELAY_HOURS        = 3    # orange warning threshold
SEVERE_DELAY_HOURS       = 12   # red critical threshold
IMMINENT_MINS            = 25   # red "hot" threshold — flight arriving within 25 min
API_LAG_MINS             = 10   # AeroDataBox lag observed in practice — typical 5-15 min range
# Per-aircraft detail lookup (age / seats / freighter), Tier 1 = 1 unit each.
# Re-enabled on the Ultra plan (60,000 units/month) — and this time protected by
# a hard daily budget so redeploy-driven cache wipes can never repeat the July
# quota incident: worst case AC_DAILY_BUDGET × 1 × 31 = 4,650/month... capped
# below at 150/day = 4,650. Combined worst case across FIDS + dep + aircraft
# stays around 33,000/month — barely half the Ultra allowance.
AIRCRAFT_INFO_ENABLED    = True
AC_DAILY_BUDGET          = 150
# Actual departure-time lookups (per-flight endpoint, assumed Tier 2 = 2 units).
# HARD-CAPPED: at most DEP_DAILY_BUDGET HTTP calls per calendar day, counted at
# the request site — redeploys, cache wipes, and retries all spend from the same
# daily pot. Ultra plan: worst case 120 × 2 × 31 = 7,440 units/month.
# Per-flight leg lookup. Kept ON because it powers registration cross-validation
# (today's airframe vs the stale previous-rotation reg that FIDS often carries).
# The departure TIME it also returns is intentionally not displayed — see render.
DEP_INFO_ENABLED         = True
DEP_DAILY_BUDGET         = 120
DEP_FAIL_TTL_SEC         = 180
EST_COMPENSATION_MINS    = 10   # AeroDataBox Est runs ~10 min later than actual touchdown (observed);
                                # subtract this from live radar estimates to better predict real arrival
OPENSKY_PREFER_UNDER_MIN = 60   # use OpenSky over AeroDataBox for flights < 60 min out
IMAGE_WORKERS            = 3    # Planespotters free API rate-limits aggressively (429s) — keep concurrency low
PHOTO_FAIL_TTL_SEC       = 180  # retry failed photo lookups after 3 min (was 10 — too long for transient failures)
SURGE_WINDOW_MINS        = 15   # cluster detection window
SURGE_MIN_FLIGHTS        = 3    # minimum flights in cluster to consider
SURGE_MIN_WEIGHT         = 4    # weight-based trigger: fires on 3+ flights OR weight>=4 (so 2 widebodies=6 also triggers)
DOMESTIC_TERMINALS       = ('D', 'DOM', 'D-ANC', 'GAT')
SMALL_AIRCRAFT_FILTER    = ('BEECH', 'FAIRCHILD', 'CESSNA', 'PIPER', 'PILATUS', 'KING AIR', 'METROLINER', 'SAAB')

# Add flight numbers here that appear in AeroDataBox but never actually operate to BNE
GHOST_FLIGHTS = set()

AIRBORNE_STATUSES = {"enroute", "departed", "approaching"}

CITY_MAP = {
    "Lapu-Lapu City": "Cebu", "Denpasar-Bali Island": "Bali",
    "Ho Chi Minh City": "Saigon", "Yaren District": "Nauru",
    "Guangzhou Baiyun": "Guangzhou",
    # Obscure airport-town names → names floor staff actually recognise
    "Avarua": "Cook Islands",            # RAR — Rarotonga's main town
    "Burnt Pine": "Norfolk Island",      # NLK — town on Norfolk Island
    "Luganville": "Santo (Vanuatu)",     # SON — Espiritu Santo
    "Bandar Seri Begawan": "Brunei",     # BWN — Royal Brunei
    "Taoyuan": "Taipei",                 # TPE — China Airlines
    "Taoyuan City": "Taipei",
}

# ── i18n ──────────────────────────────────────────────────────────────────────
# UI languages for the Lotte team: English, Traditional Chinese (Taiwan),
# Korean, Japanese. {x}/{n} are format placeholders.
LANG_OPTIONS = {"en": "English", "zh": "繁體中文", "ko": "한국어", "ja": "日本語"}

TRANSLATIONS = {
    "en": {
        "just_landed":   "Just Landed",
        "landed_ago":    "Landed {x} ago",
        "in_time":       "In {x}",
        "on_ground":     "On Ground",
        "no_update":     "NO UPDATE",
        "canceled":      "CANCELED",
        "diverted":      "✈️ DIVERTED",
        "check_board":   "⚠️ Check Board",
        "late":          "+{x} Late",
        "incoming":      "Incoming",
        "next_gap":      "Next Gap",
        "busiest":       "Busiest",
        "now_fmt":       "NOW ({m}m)",
        "gate":          "GATE",
        "active":        "🟢 ACTIVE",
        "gap_fmt":       "{x} GAP",
        "before_next":   "{x} BEFORE NEXT FLIGHT",
        "ends":          "Ends {x}",
        "approx":        "approx",
        "earlier":       "Earlier Arrivals",
        "surge_fmt":     "SURGE {a}–{b} ({n} flights)",
        "was_gate":      "⚠ was {x}",
        "seats":         "{n} seats",
        "dep_label":     "Dep {x}",
        "dep_est_label": "Dep~{x}",
        "more_photos":   "More photos",
        "age_years":     "{n} years",
        "age_months":    "{n} months",
        "freighter":     "📦 Freighter",
        "disruption":    "⚠️ Airport disruption — multiple diversions detected. Times below are unreliable; verify on the FIDS board.",
        "wx_cond":       "Conditions",
        "wx_temp":       "Temperature",
        "wx_wind":       "Wind",
        "wx_clear":      "Clear",
        "wx_pcloudy":    "Partly cloudy",
        "wx_cloudy":     "Cloudy",
        "wx_fog":        "Fog",
        "wx_drizzle":    "Drizzle",
        "wx_rain":       "Rain",
        "wx_showers":    "Showers",
        "wx_snow":       "Snow",
        "wx_storm":      "Thunderstorm",
        "wx_next3h":     "Next 3h:",
        "wx_loading":    "Loading weather…",
        "wx_vis":        "Vis",
        "updated_ago":   "Updated {x} ago",
        "just_now":      "Updated just now",
        "min_ago":       "{n} min",
        "lag_note":      "(+~10m lag)",
        "lag_tip":       "AeroDataBox data typically lags real-time by 5-15 min",
        "next_refresh":  "Next refresh: ",
        "loading":       "Loading data...",
        "text_size":     "Text Size",
        "theme":         "Theme",
        "language":      "Language",
        "dark":          "🌙 Dark",
        "light":         "☀️ Light",
        "quiet":         "🌙 Board is sleeping to save API quota. Wakes up at {h}:00 AEST.",
        "stale_title":   "STALE DATA — last update was {n} min ago",
        "stale_body":    "API refresh is failing. Treat all times below with caution and check the airport FIDS board.",
        "diverted_hdr":  "✈️ Diverted — not arriving at BNE",
        "canceled_hdr":  "❌ Canceled",
    },
    "zh": {
        "just_landed":   "剛降落",
        "landed_ago":    "{x}前降落",
        "in_time":       "還有 {x}",
        "on_ground":     "已落地滑行中",
        "no_update":     "無更新",
        "canceled":      "已取消",
        "diverted":      "✈️ 轉降他場",
        "check_board":   "⚠️ 請看機場看板",
        "late":          "誤點 +{x}",
        "incoming":      "進港中",
        "next_gap":      "下個空檔",
        "busiest":       "最忙時段",
        "now_fmt":       "現在（{m}分）",
        "gate":          "登機門",
        "active":        "🟢 進行中",
        "gap_fmt":       "{x} 空檔",
        "before_next":   "距下一班 {x}",
        "ends":          "{x} 結束",
        "approx":        "約",
        "earlier":       "較早抵達",
        "surge_fmt":     "高峰 {a}–{b}（{n} 班）",
        "was_gate":      "⚠ 原 {x}",
        "seats":         "{n} 座",
        "dep_label":     "起飛 {x}",
        "dep_est_label": "預計起飛 {x}",
        "more_photos":   "更多照片",
        "age_years":     "機齡 {n} 年",
        "age_months":    "機齡 {n} 個月",
        "freighter":     "📦 貨機",
        "disruption":    "⚠️ 機場營運異常 — 偵測到多班轉降。以下時間可能不準，請以機場看板為準。",
        "wx_cond":       "天氣",
        "wx_temp":       "氣溫",
        "wx_wind":       "風向風速",
        "wx_clear":      "晴",
        "wx_pcloudy":    "多雲時晴",
        "wx_cloudy":     "陰",
        "wx_fog":        "霧",
        "wx_drizzle":    "毛毛雨",
        "wx_rain":       "雨",
        "wx_showers":    "陣雨",
        "wx_snow":       "雪",
        "wx_storm":      "雷雨",
        "wx_next3h":     "未來3小時：",
        "wx_loading":    "天氣載入中…",
        "wx_vis":        "能見度",
        "updated_ago":   "更新於 {x}前",
        "just_now":      "剛剛更新",
        "min_ago":       "{n} 分鐘",
        "lag_note":      "（+約10分延遲）",
        "lag_tip":       "AeroDataBox 資料通常比即時慢 5-15 分鐘",
        "next_refresh":  "下次更新：",
        "loading":       "載入中...",
        "text_size":     "字體大小",
        "theme":         "主題",
        "language":      "語言",
        "dark":          "🌙 深色",
        "light":         "☀️ 淺色",
        "quiet":         "🌙 看板休眠中以節省 API 額度，將於 AEST {h}:00 喚醒。",
        "stale_title":   "資料過期 — 最後更新為 {n} 分鐘前",
        "stale_body":    "API 更新失敗中。以下時間僅供參考，請以機場看板為準。",
        "diverted_hdr":  "✈️ 轉降 — 不會抵達 BNE",
        "canceled_hdr":  "❌ 已取消",
    },
    "ko": {
        "just_landed":   "방금 착륙",
        "landed_ago":    "{x} 전 착륙",
        "in_time":       "{x} 후",
        "on_ground":     "지상 이동 중",
        "no_update":     "업데이트 없음",
        "canceled":      "취소됨",
        "diverted":      "✈️ 회항",
        "check_board":   "⚠️ 안내판 확인",
        "late":          "+{x} 지연",
        "incoming":      "도착 예정",
        "next_gap":      "다음 공백",
        "busiest":       "최대 혼잡",
        "now_fmt":       "지금 ({m}분)",
        "gate":          "게이트",
        "active":        "🟢 진행 중",
        "gap_fmt":       "{x} 공백",
        "before_next":   "다음 항공편까지 {x}",
        "ends":          "{x} 종료",
        "approx":        "약",
        "earlier":       "이전 도착",
        "surge_fmt":     "혼잡 {a}–{b} ({n}편)",
        "was_gate":      "⚠ 이전 {x}",
        "seats":         "{n}석",
        "dep_label":     "출발 {x}",
        "dep_est_label": "출발 예정 {x}",
        "more_photos":   "사진 더 보기",
        "age_years":     "기령 {n}년",
        "age_months":    "기령 {n}개월",
        "freighter":     "📦 화물기",
        "disruption":    "⚠️ 공항 운영 차질 — 다수의 회항 감지. 아래 시간은 부정확할 수 있으니 안내판을 확인하세요.",
        "wx_cond":       "날씨",
        "wx_temp":       "기온",
        "wx_wind":       "바람",
        "wx_clear":      "맑음",
        "wx_pcloudy":    "구름 조금",
        "wx_cloudy":     "흐림",
        "wx_fog":        "안개",
        "wx_drizzle":    "이슬비",
        "wx_rain":       "비",
        "wx_showers":    "소나기",
        "wx_snow":       "눈",
        "wx_storm":      "뇌우",
        "wx_next3h":     "향후 3시간:",
        "wx_loading":    "날씨 로딩 중…",
        "wx_vis":        "시정",
        "updated_ago":   "{x} 전 업데이트",
        "just_now":      "방금 업데이트",
        "min_ago":       "{n}분",
        "lag_note":      "(+약 10분 지연)",
        "lag_tip":       "AeroDataBox 데이터는 보통 실시간보다 5-15분 늦습니다",
        "next_refresh":  "다음 새로고침: ",
        "loading":       "로딩 중...",
        "text_size":     "글자 크기",
        "theme":         "테마",
        "language":      "언어",
        "dark":          "🌙 다크",
        "light":         "☀️ 라이트",
        "quiet":         "🌙 API 절약을 위해 대기 모드입니다. AEST {h}:00에 다시 시작됩니다.",
        "stale_title":   "오래된 데이터 — 마지막 업데이트 {n}분 전",
        "stale_body":    "API 갱신이 실패하고 있습니다. 아래 시간은 참고용이며 공항 안내판을 확인하세요.",
        "diverted_hdr":  "✈️ 회항 — BNE에 도착하지 않음",
        "canceled_hdr":  "❌ 취소됨",
    },
    "ja": {
        "just_landed":   "着陸直後",
        "landed_ago":    "{x}前に着陸",
        "in_time":       "あと{x}",
        "on_ground":     "地上走行中",
        "no_update":     "更新なし",
        "canceled":      "欠航",
        "diverted":      "✈️ ダイバート",
        "check_board":   "⚠️ 案内板確認",
        "late":          "+{x} 遅延",
        "incoming":      "到着予定",
        "next_gap":      "次の空き時間",
        "busiest":       "最混雑",
        "now_fmt":       "現在（{m}分）",
        "gate":          "ゲート",
        "active":        "🟢 進行中",
        "gap_fmt":       "{x} 空き",
        "before_next":   "次の便まで {x}",
        "ends":          "{x} 終了",
        "approx":        "約",
        "earlier":       "以前の到着",
        "surge_fmt":     "ピーク {a}–{b}（{n}便）",
        "was_gate":      "⚠ 旧 {x}",
        "seats":         "{n}席",
        "dep_label":     "出発 {x}",
        "dep_est_label": "出発予定 {x}",
        "more_photos":   "他の写真",
        "age_years":     "機齢{n}年",
        "age_months":    "機齢{n}ヶ月",
        "freighter":     "📦 貨物機",
        "disruption":    "⚠️ 空港運用の乱れ — 複数のダイバートを検出。以下の時刻は不正確な場合があります。案内板をご確認ください。",
        "wx_cond":       "天気",
        "wx_temp":       "気温",
        "wx_wind":       "風",
        "wx_clear":      "晴れ",
        "wx_pcloudy":    "晴れ時々曇り",
        "wx_cloudy":     "曇り",
        "wx_fog":        "霧",
        "wx_drizzle":    "霧雨",
        "wx_rain":       "雨",
        "wx_showers":    "にわか雨",
        "wx_snow":       "雪",
        "wx_storm":      "雷雨",
        "wx_next3h":     "今後3時間：",
        "wx_loading":    "天気を読み込み中…",
        "wx_vis":        "視程",
        "updated_ago":   "{x}前に更新",
        "just_now":      "たった今更新",
        "min_ago":       "{n}分",
        "lag_note":      "（+約10分遅延）",
        "lag_tip":       "AeroDataBoxのデータは通常リアルタイムより5〜15分遅れます",
        "next_refresh":  "次の更新: ",
        "loading":       "読み込み中...",
        "text_size":     "文字サイズ",
        "theme":         "テーマ",
        "language":      "言語",
        "dark":          "🌙 ダーク",
        "light":         "☀️ ライト",
        "quiet":         "🌙 API節約のためスリープ中。AEST {h}:00に再開します。",
        "stale_title":   "古いデータ — 最終更新は{n}分前",
        "stale_body":    "API更新が失敗しています。以下の時刻は参考程度とし、空港の案内板をご確認ください。",
        "diverted_hdr":  "✈️ ダイバート — BNEには到着しません",
        "canceled_hdr":  "❌ 欠航",
    },
}


def L(key: str, **kw) -> str:
    """Translate a UI string into the currently selected language. Falls back
    to English if the key is missing from the active language pack."""
    lang = st.session_state.get("lang", "en")
    template = TRANSLATIONS.get(lang, TRANSLATIONS["en"]).get(key) or TRANSLATIONS["en"].get(key, key)
    try:
        return template.format(**kw) if kw else template
    except (KeyError, IndexError):
        return template

# ── OpenSky Network ──────────────────────────────────────────────────────────
YBBN_LAT, YBBN_LON = -27.3842, 153.1175
# Broad box covering NZ, Pacific, SE Asia approach corridors for YBBN arrivals
OPENSKY_BBOX = {"lamin": -38, "lamax": -10, "lomin": 135, "lomax": 170}
OPENSKY_ENABLED      = False  # disabled — Streamlit Cloud cannot reach OpenSky (every cycle times out, never delivers data)
OPENSKY_MIN_SPEED_KT = 80
OPENSKY_MAX_ETA_MIN  = 600

AIRLINE_ICAO = {
    "QF": "QFA", "SQ": "SIA", "CX": "CPA", "VA": "VOZ", "JQ": "JST",
    "NZ": "ANZ", "FJ": "FJI", "CI": "CAL", "CZ": "CSN", "MU": "CES",
    "TG": "THA", "VN": "HVN", "MH": "MAS", "GA": "GIA", "PR": "PAL",
    "KE": "KAL", "OZ": "AAR", "JL": "JAL", "NH": "ANA", "TR": "TGW",
    "3K": "JSA", "BI": "RBA", "PX": "ANG", "SB": "ACI", "EK": "UAE",
    "QR": "QTR", "EY": "ETD", "AI": "AIC", "AK": "AXM", "5J": "CEB",
    "NF": "AVN", "S7": "SBI", "CA": "CCA", "HX": "CRK", "UO": "HKE",
    "BR": "EVA", "IT": "TTW", "MM": "APJ", "TW": "TWB", "PG": "BKP",
    "IE": "SOL", "ON": "RON", "OD": "MXD", "VJ": "VJC", "U2": "EZY",
    "UA": "UAL", "DL": "DAL", "AA": "AAL", "AC": "ACA", "BA": "BAW",
    "AF": "AFR", "KL": "KLM", "LH": "DLH", "SV": "SVA",
}

# FIX 5 — use constant in the fragment decorator (was hardcoded "60s")
UI_REFRESH_SEC           = 60
API_DATA_TTL_SEC         = 300  # 5 min cache — Ultra plan: ~264 calls/day × 2 units × 31 ≈ 16,400/month vs 60,000 limit
OPENSKY_TTL_SEC          = 60   # free source — refresh every fragment cycle for freshest radar positions

# Quiet hours — skip API calls between these times to save units. BNE international
# arrivals are minimal between ~01:00 and ~03:00 AEST, and Phillip's shift starts at
# 04:00 — nobody actually needs live data at 02:00.
QUIET_HOURS_START_H      = 1    # 01:00 AEST
QUIET_HOURS_END_H        = 3    # 03:00 AEST

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bne-board")

# Photo caches (module-level, thread-safe via single lock):
#   _photo_url_cache : reg -> photo URL (success) or "NOT_FOUND" (genuine miss, don't retry)
#   _photo_fails     : reg -> datetime of last TRANSIENT failure (retry after PHOTO_FAIL_TTL_SEC)
_photo_url_cache: dict   = {}
_photo_fails: dict       = {}
_photo_pending: set      = set()   # regs currently being fetched in the background
# Aircraft details cache (Tier 1 endpoint, 1 unit/call). reg -> info dict, or
# "NONE" when the API had nothing useful. Fetched lazily in background threads,
# cached for the life of the process — age/seats/freighter status never change.
_ac_info_cache: dict     = {}
_ac_info_pending: set    = set()
_ac_budget               = {"date": "", "n": 0}
_ac_info_lock            = threading.Lock()
# Departure-time lookups: key = "FLIGHT|YYYY-MM-DD" (arrival sched date, AEST)
_dep_cache: dict         = {}   # key -> "HH:MM" (AEST) or "NONE"
_dep_pending: set        = set()
_dep_fails: dict         = {}   # key -> fail timestamp (retry after DEP_FAIL_TTL_SEC)
_dep_budget              = {"date": "", "n": 0}
_dep_lock                = threading.Lock()

# Gate history — module-level so ALL users see the same change badges (was
# session-scoped: each browser had its own memory). Changes expire after 60 min.
_gate_state: dict        = {}   # flight_num -> last seen gate
_gate_changed: dict      = {}   # flight_num -> (old_gate, detected_at)
_gate_lock               = threading.Lock()
GATE_BADGE_TTL_SEC       = 3600
# AeroDataBox enforces a per-second rate limit on the Pro plan — space aircraft
# lookups to ~1 req/sec so they never trip it (nor collide with the FIDS call).
_adb_throttle_lock       = threading.Lock()
_adb_last_request        = [0.0]
# FIDS failure backoff — after a failed fetch, don't hammer the API on every
# 60s fragment rerun; wait this long before the next attempt.
_fids_fail_until         = [0.0]
FIDS_FAIL_BACKOFF_SEC    = 180
ADB_MIN_INTERVAL_SEC     = 1.1
_photo_lock              = threading.Lock()
# Throttle: enforce a minimum gap between outbound Planespotters requests across
# all threads so we don't burst past the free API's rate limit (was getting 429s).
_photo_throttle_lock     = threading.Lock()
_photo_last_request      = [0.0]   # mutable holder for last-request timestamp
# Cap concurrent background photo threads — without this, 30 cache-miss regs
# would spawn 30 threads at once (harmless due to the throttle, but wasteful).
_photo_semaphore         = threading.Semaphore(IMAGE_WORKERS)
PHOTO_MIN_INTERVAL_SEC   = 0.4     # ~2.5 requests/sec max

# ─────────────────────────────────────────────
#  2. THEME & STATUS CLASSIFICATION
# ─────────────────────────────────────────────
@dataclass
class ThemeParams:
    bg_main: str
    bg_card: str
    text_main: str
    text_muted: str
    text_faded: str
    border_muted: str
    gap_bg: str
    gap_active_bg: str
    gap_active_text: str
    modal_bg: str
    fallback_bg: str
    c_blue: str
    c_green: str
    c_amber: str
    c_red: str
    c_purple: str
    c_purple_bg: str
    # Theme-aware delay/surge colours — light mode needs softer variants;
    # hardcoded dark-reds look heavy/muddy on a bright background.
    c_severe_border: str   # border on 12h+ delayed cards
    c_heavy_border: str    # border on 3h+ delayed cards
    surge_bg_start: str    # surge banner gradient start
    surge_bg_end: str      # surge banner gradient end
    surge_text: str        # surge banner text
    surge_border: str      # surge banner left border


def get_theme(is_light: bool) -> ThemeParams:
    if is_light:
        # Light mode philosophy: clean, bright, airy — NOT a darkened mirror of dark mode.
        # Use a warm off-white background, pure white cards, and vivid mid-saturation
        # accent colors that pop against light bg without feeling heavy.
        return ThemeParams(
            bg_main="#F1F5F9",        # subtle cool grey — slight tint so white cards lift
            bg_card="#FFFFFF",
            text_main="#1E293B",       # dark slate, not pure black (softer to read)
            text_muted="#475569",
            text_faded="#64748B",
            border_muted="#CBD5E1",    # back to lighter borders — softer look
            gap_bg="#FFFFFF", gap_active_bg="#ECFDF5",
            gap_active_text="#059669", modal_bg="rgba(241,245,249,0.95)", fallback_bg="#E2E8F0",
            # Accent colours — vibrant mid-tones (500-level), not darkened 700+:
            c_blue="#3B82F6",          # bright blue — feels like "info"
            c_green="#10B981",         # vivid mint green
            c_amber="#F59E0B",         # warm gold/amber
            c_red="#EF4444",           # punchy red
            c_purple="#8B5CF6", c_purple_bg="#F3E8FF",
            # Light-mode delay/surge — pastel backgrounds, readable dark-red text
            c_severe_border="#FCA5A5",
            c_heavy_border="#FCD34D",
            surge_bg_start="#FEE2E2",
            surge_bg_end="#FECACA",
            surge_text="#B91C1C",
            surge_border="#EF4444",
        )
    return ThemeParams(
        bg_main="#0F172A", bg_card="#1E293B", text_main="white", text_muted="#94A3B8",
        text_faded="#CBD5E1", border_muted="#475569", gap_bg="#0F172A", gap_active_bg="#064E3B",
        gap_active_text="#A7F3D0", modal_bg="rgba(15,23,42,0.92)", fallback_bg="#1E293B",
        c_blue="#60A5FA", c_green="#34D399", c_amber="#F59E0B", c_red="#F87171",
        c_purple="#C4B5FD", c_purple_bg="#1E1B4B",
        c_severe_border="#7F1D1D",
        c_heavy_border="#92400E",
        surge_bg_start="#7F1D1D",
        surge_bg_end="#991B1B",
        surge_text="#FCA5A5",
        surge_border="#EF4444",
    )


def get_dynamic_css(t: ThemeParams, font_size_px: int = 16) -> str:
    return f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=JetBrains+Mono:wght@600&display=swap');
        #MainMenu {{visibility: hidden;}} header {{visibility: hidden;}}
        .stApp {{ background-color: {t.bg_main}; font-size: {font_size_px}px; }}
        html {{ font-size: {font_size_px}px; }}
        .block-container {{padding-top: 1rem; font-family: 'Inter', sans-serif; max-width: 700px; color: {t.text_main};}}
        .mono {{ font-family: 'JetBrains Mono', monospace; letter-spacing: -0.5px; }}

        .flip-container {{ position: relative; width: 55px; height: 55px; margin-right: 12px; flex-shrink: 0; }}
        .flip-img {{ position: absolute; top: 0; left: 0; width: 55px; height: 55px; border-radius: 8px; border: 2.5px solid {t.border_muted}; transition: opacity 1s ease-in-out; box-sizing: border-box; }}
        .img-fallback {{
            position: absolute; top: 0; left: 0; width: 55px; height: 55px; border-radius: 8px;
            border: 2.5px solid {t.border_muted}; box-sizing: border-box; z-index: 0;
            display: flex; align-items: center; justify-content: center;
            background: {t.fallback_bg}; color: {t.text_muted}; font-weight: 700; font-size: 0.75em; letter-spacing: 0.5px;
        }}

        @keyframes logoFade  {{ 0%, 40% {{ opacity: 1; }} 50%, 90% {{ opacity: 0; }} 100% {{ opacity: 1; }} }}
        @keyframes photoFade {{ 0%, 40% {{ opacity: 0; }} 50%, 90% {{ opacity: 1; }} 100% {{ opacity: 0; }} }}

        .logo-layer  {{ animation: logoFade 10s infinite;  background: #FFFFFF; padding: 4px; object-fit: contain !important; border-radius: 8px; z-index: 2; }}
        .photo-layer {{ animation: photoFade 10s infinite; object-fit: cover !important;   z-index: 1; }}

        .flight-card {{
            border-radius: 10px; padding: 10px 14px; margin-bottom: 8px; display: flex; align-items: center;
            color: {t.text_main}; box-shadow: 0 4px 10px rgba(0,0,0,0.15); border-left: 5px solid {t.c_blue}; transition: opacity 0.3s ease;
        }}
        .info-col   {{ flex-grow: 1; min-width: 0; overflow: hidden; word-wrap: break-word; }}
        .info-col .ac-line {{ font-size: 0.78em; color: {t.text_faded}; margin: 1px 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .status-col {{ text-align: right; min-width: 110px; max-width: 45%; display: flex; flex-direction: column; justify-content: center; flex-shrink: 0; }}
        .gate-num   {{ font-size: 1.85em; font-weight: 700; line-height: 1; }}
        .gate-tba   {{ font-size: 1.85em; font-weight: 700; line-height: 1; opacity: 0.35; }}

        .summary-strip {{
            display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
            background: {t.bg_card}; border: 1px solid {t.border_muted}; border-radius: 8px;
            padding: 10px 14px; margin-bottom: 10px; font-size: 0.78em; color: {t.text_muted}; gap: 4px 0;
        }}
        .summary-strip .s-item {{ text-align: center; min-width: 30%; }}
        .summary-strip .s-val  {{ font-weight: 700; font-size: 1.15em; display: block; }}

        .gap-bar {{
            background-color: {t.gap_bg}; border: 1px dashed {t.border_muted}; border-left: 5px solid transparent;
            border-radius: 8px; padding: 8px 14px; margin: 4px 0 10px 0; text-align: center; color: {t.text_muted};
            font-weight: 600; font-size: 0.85em; box-sizing: border-box;
        }}
        .gap-active {{ background-color: {t.gap_active_bg}; border-color: {t.c_green}; border-left-color: {t.c_green}; color: {t.gap_active_text}; }}

        .gap-progress-track {{ width: 100%; height: 5px; background: {t.border_muted}; border-radius: 3px; margin-top: 6px; overflow: hidden; }}
        .gap-progress-fill {{ height: 100%; border-radius: 3px; transition: width 1s linear; }}

        .surge-banner {{
            background: linear-gradient(90deg, {t.surge_bg_start} 0%, {t.surge_bg_end} 100%); border-left: 5px solid {t.surge_border}; border-radius: 8px;
            padding: 7px 14px; margin: 6px 0 8px 0; color: {t.surge_text}; font-size: 0.82em; font-weight: 700; display: flex; align-items: center; gap: 8px;
        }}
        .surge-banner .surge-icon {{ font-size: 1.1em; }}

        .img-zoom-modal {{
            display: none; position: fixed; top: 0; left: 0; width: 100vw; height: 100vh;
            background: {t.modal_bg}; z-index: 10000; flex-direction: column;
            align-items: center; justify-content: center; backdrop-filter: blur(10px);
        }}
        .img-zoom-chk:checked + .img-zoom-modal {{ display: flex !important; }}
        .img-zoom-modal img {{ max-width: 90%; max-height: 80%; border-radius: 12px; border: 2px solid {t.border_muted}; object-fit: contain; z-index: 10001; }}
        .img-zoom-close-bg {{ position: absolute; top: 0; left: 0; width: 100%; height: 100%; cursor: pointer; z-index: 10000; }}
        .close-btn {{ position: absolute; top: 20px; right: 30px; color: {t.text_main}; font-size: 3.5em; font-weight: bold; cursor: pointer; z-index: 10002; line-height: 1; }}
        .zoom-caption {{ margin-top: 12px; color: {t.text_main}; font-size: 0.95em; font-weight: 600;
                         background: {t.bg_card}; border: 1px solid {t.border_muted}; border-radius: 8px;
                         padding: 8px 16px; z-index: 10001; max-width: 90%; text-align: center; }}
        /* Hide the popover chevron arrow next to the gear icon */
        [data-testid="stPopover"] button [data-testid="stIconMaterial"]:last-of-type,
        [data-testid="stPopover"] button svg:last-of-type {{ display: none !important; }}
    </style>
    """


@dataclass
class FlightStyle:
    border_color: str
    status_color: str
    bg_color: str
    status_text: str
    card_opacity: str
    img_filter: str


def classify_flight_status(*, is_canceled, is_diverted, is_landed, landed_mins,
                            t_diff, t_type, delay_hours, s_dt, now, t: ThemeParams) -> FlightStyle:
    if is_canceled:
        archived = (now - s_dt).total_seconds() / 60 > 15
        if archived:
            return FlightStyle(t.border_muted, t.text_muted, t.bg_main, L("canceled"), "0.5", "grayscale(100%)")
        return FlightStyle(t.c_red, t.c_red, t.bg_card, L("canceled"), "0.5", "grayscale(100%)")

    if is_diverted:
        return FlightStyle(t.c_purple, t.c_purple, t.c_purple_bg, L("diverted"), "0.8", "none")

    if is_landed:
        landed_label = L("just_landed") if landed_mins == 0 else L("landed_ago", x=format_hm(landed_mins))
        # Surface heavy delay context even after landing — operationally we
        # still care that a flight arrived 2+ hours late (impacts pax flow,
        # connection misses, etc.) rather than just showing "Landed".
        if delay_hours >= HEAVY_DELAY_HOURS and landed_mins <= RECENT_LANDED_MAX:
            delay_mins = max(0, int(round(delay_hours * 60)))
            tag        = "🔴" if delay_hours >= SEVERE_DELAY_HOURS else "🟠"
            label      = f"{tag} {landed_label} (+{format_hm(delay_mins)})"
            return FlightStyle(t.c_amber, t.c_amber, t.bg_main, label, "0.75", "grayscale(20%)")
        if landed_mins <= RECENT_LANDED_MAX:
            return FlightStyle(t.c_green, t.c_green, t.bg_main,
                               landed_label, "0.75", "grayscale(40%)")
        return FlightStyle(t.border_muted, t.text_muted, t.bg_main,
                           landed_label, "0.4", "grayscale(80%)")

    m_left     = max(0, t_diff)
    delay_mins = max(0, int(round(delay_hours * 60)))

    if t_type == "scheduled" and t_diff <= 0:
        return FlightStyle(t.c_amber, t.c_amber, t.bg_card, L("no_update"), "1.0", "none")
    if m_left < IMMINENT_MINS:
        label = L("on_ground") if m_left == 0 else L("in_time", x=format_hm(m_left))
        return FlightStyle(t.c_red, t.c_red, t.bg_card, label, "1.0", "none")
    if delay_hours >= SEVERE_DELAY_HOURS:
        return FlightStyle(t.c_severe_border, t.c_red, t.bg_card, "🔴 " + L("late", x=format_hm(delay_mins)), "1.0", "none")
    if delay_hours >= HEAVY_DELAY_HOURS:
        return FlightStyle(t.c_heavy_border, t.c_amber, t.bg_card, "🟠 " + L("late", x=format_hm(delay_mins)), "1.0", "none")
    return FlightStyle(t.c_blue, t.c_blue, t.bg_card, L("in_time", x=format_hm(m_left)), "1.0", "none")


# ─────────────────────────────────────────────
#  3. CORE LOGIC
# ─────────────────────────────────────────────
def format_hm(total_minutes: int) -> str:
    h, m = divmod(total_minutes, 60)
    return f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"


def get_aircraft_pax_weight(model: str) -> int:
    """Approximate pax-load weight by aircraft size class. 3 narrowbodies
    (~450 pax) and 2 widebodies (~900 pax) are very different operational
    events; weighting by size makes surge alerts reflect real pax volume."""
    m = (model or "").upper()
    if any(x in m for x in ("777", "787", "A350", "A380", "A330", "A340", "747")):
        return 3   # widebody
    if any(x in m for x in ("737", "A319", "A320", "A321", "A220", "E190", "E195")):
        return 1   # narrowbody
    return 0       # regional/small


def extract_best_time(node: dict, tz):
    for key, label in (("actualTime", "actual"), ("revisedTime", "revised"), ("scheduledTime", "scheduled")):
        raw_obj = node.get(key)
        raw = raw_obj.get("local") if isinstance(raw_obj, dict) else node.get(key + "Local")
        if raw:
            try:
                dt = pd.to_datetime(raw).to_pydatetime()
                dt = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
                return dt, label
            except Exception as e:
                log.warning("Time parse failed for key=%s raw=%r: %s", key, raw, e)
                continue
    return None, ""


def is_strictly_international(terminal: str, country_code: str, aircraft_model: str,
                              origin_iata: str, reg: str = "") -> bool:
    t    = terminal.strip().upper()
    ac   = aircraft_model.upper()
    cc   = country_code.lower()
    iata = origin_iata.upper()
    rv   = reg.strip().upper()
    if iata == "NLK":                                    return True
    if t in DOMESTIC_TERMINALS:                          return False
    if cc == "au":                                       return False
    if rv.startswith("VH-") and not cc:                  return False
    if not cc and not iata:                              return False
    if any(k in ac for k in SMALL_AIRCRAFT_FILTER):      return False
    return True


def get_airline_logo_url(flight_number: str) -> str:
    prefix = "".join(c for c in flight_number if c.isalpha())[:2].upper()
    return f"https://pics.avs.io/200/200/{prefix}.png" if len(prefix) == 2 else ""


# ── Photo fetching with smart retry ──────────────────────────────────────────
def _fetch_photo_http(reg: str) -> str:
    """Returns a photo URL, or 'NOT_FOUND' (genuine: photo doesn't exist),
    or 'TRANSIENT_FAIL' (timeout/rate-limit/server error — should be retried,
    not cached permanently)."""
    # Global throttle — space out requests so concurrent threads don't burst
    # past Planespotters' rate limit.
    with _photo_throttle_lock:
        import time as _time
        elapsed = _time.time() - _photo_last_request[0]
        if elapsed < PHOTO_MIN_INTERVAL_SEC:
            _time.sleep(PHOTO_MIN_INTERVAL_SEC - elapsed)
        _photo_last_request[0] = _time.time()
    try:
        r = requests.get(
            f"https://api.planespotters.net/pub/photos/reg/{reg}",
            headers={"User-Agent": "BNE-Arrivals-Board/1.0 (+https://github.com/phillipyeh89/bne-flight-board)"},
            timeout=6.0,
        )
        if r.status_code == 200:
            photos = r.json().get("photos", [])
            if photos:
                return photos[0]["thumbnail_large"]["src"]
            return "NOT_FOUND"      # genuine: API responded, no photo on file
        if r.status_code == 429 or r.status_code >= 500:
            return "TRANSIENT_FAIL"  # rate limited / server error — retry later
        return "NOT_FOUND"
    except Exception as e:
        log.warning("Photo fetch failed for reg=%s: %s", reg, e)
        return "TRANSIENT_FAIL"      # timeout / connection error — retry later


def _background_fetch_photo(reg: str):
    """Worker run in a daemon thread — fetches a photo and updates the cache.
    Never blocks the UI. Photos appear on the next 60s refresh once cached."""
    with _photo_semaphore:
        url = _fetch_photo_http(reg)
    with _photo_lock:
        _photo_pending.discard(reg)
        if url not in ("NOT_FOUND", "TRANSIENT_FAIL"):
            _photo_url_cache[reg] = url            # cache success
        elif url == "NOT_FOUND":
            _photo_url_cache[reg] = "NOT_FOUND"    # genuine miss — stop retrying
        else:
            _photo_fails[reg] = datetime.now()     # transient — allow retry later


def _fetch_aircraft_info_http(reg: str):
    """Fetch aircraft details from AeroDataBox (Tier 1 = 1 unit). Returns a
    small dict of the fields we display, or "NONE" if nothing useful, or None
    on transient failure (retry naturally on a later cache miss)."""
    # Global 1 req/sec gate across all aircraft-lookup threads
    with _adb_throttle_lock:
        import time as _time
        _el = _time.time() - _adb_last_request[0]
        if _el < ADB_MIN_INTERVAL_SEC:
            _time.sleep(ADB_MIN_INTERVAL_SEC - _el)
        _adb_last_request[0] = _time.time()
    try:
        r = requests.get(
            f"https://aerodatabox.p.rapidapi.com/aircrafts/reg/{reg}/all",
            headers={
                "X-RapidAPI-Key":  st.secrets["X_RAPIDAPI_KEY"],
                "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
            },
            timeout=8,
        )
        if r.status_code != 200:
            return None if (r.status_code == 429 or r.status_code >= 500) else "NONE"
        data = r.json()
        # /all returns a list of matches; prefer the active airframe.
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list) or not data:
            return "NONE"
        ac = next((a for a in data if a.get("active")), data[0]) or {}
        info = {}
        # Defensive extraction — only keep fields that are actually present.
        if ac.get("numSeats"):
            info["seats"] = int(ac["numSeats"])
        # Age: real payloads carry build dates rather than a ready-made ageYears —
        # derive it from the first available date field.
        age = ac.get("ageYears")
        if not age:
            for _dk in ("firstFlightDate", "rolloutDate", "deliveryDate", "registrationDate"):
                _dv = ac.get(_dk)
                if _dv:
                    try:
                        _born = datetime.strptime(str(_dv)[:10], "%Y-%m-%d")
                        age = (datetime.now() - _born).days / 365.25
                        break
                    except ValueError:
                        continue
        if age:
            info["age"] = round(float(age), 1)
        if ac.get("isFreighter"):
            info["freighter"] = True
        return info or "NONE"
    except Exception as e:
        log.warning("Aircraft info fetch failed for reg=%s: %s", reg, e)
        return None


def _background_fetch_aircraft_info(reg: str):
    # Daily budget gate — the July lesson: without this, redeploy cache wipes
    # turn "one lookup per aircraft" into thousands of calls a month.
    _today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")
    with _ac_info_lock:
        if _ac_budget["date"] != _today:
            _ac_budget["date"], _ac_budget["n"] = _today, 0
        if _ac_budget["n"] >= AC_DAILY_BUDGET:
            _ac_info_pending.discard(reg)
            return
        _ac_budget["n"] += 1
    with _photo_semaphore:          # share the same politeness cap as photos
        result = _fetch_aircraft_info_http(reg)
    with _ac_info_lock:
        _ac_info_pending.discard(reg)
        if result is not None:      # None = transient; leave uncached to retry
            _ac_info_cache[reg] = result


def get_aircraft_info(reg: str):
    """NON-BLOCKING. Returns the cached info dict, or None while not yet
    fetched (a background fetch is kicked off on first miss)."""
    if not reg or not AIRCRAFT_INFO_ENABLED:
        return None
    with _ac_info_lock:
        cached = _ac_info_cache.get(reg)
        already = reg in _ac_info_pending
        if cached is None and not already:
            _ac_info_pending.add(reg)
    if cached is not None:
        return cached if cached != "NONE" else None
    if not already:
        threading.Thread(target=_background_fetch_aircraft_info, args=(reg,), daemon=True).start()
    return None


def _dep_budget_take() -> bool:
    """Consume one unit of today's departure-lookup budget. False = exhausted."""
    today = datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d")
    with _dep_lock:
        if _dep_budget["date"] != today:
            _dep_budget["date"] = today
            _dep_budget["n"] = 0
        if _dep_budget["n"] >= DEP_DAILY_BUDGET:
            return False
        _dep_budget["n"] += 1
        return True


def _fetch_dep_time_http(flight_num: str, s_dt_iso: str, key: str):
    """Look up the actual departure (wheels-up) time for one flight via the
    per-flight endpoint. Runs in a background thread. Writes result to cache."""
    try:
        if not _dep_budget_take():
            return   # budget spent — leave uncached; tomorrow's budget may retry
        # Same politeness gate as other AeroDataBox calls
        with _adb_throttle_lock:
            import time as _time
            _el = _time.time() - _adb_last_request[0]
            if _el < ADB_MIN_INTERVAL_SEC:
                _time.sleep(ADB_MIN_INTERVAL_SEC - _el)
            _adb_last_request[0] = _time.time()

        compact = flight_num.replace(" ", "")
        r = requests.get(
            f"https://aerodatabox.p.rapidapi.com/flights/number/{compact}",
            headers={
                "X-RapidAPI-Key":  st.secrets["X_RAPIDAPI_KEY"],
                "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
            },
            params={"withAircraftImage": "false", "withLocation": "false"},
            timeout=8,
        )
        if r.status_code != 200:
            if r.status_code == 429 or r.status_code >= 500:
                with _dep_lock:
                    _dep_fails[key] = datetime.now().timestamp()
            else:
                with _dep_lock:
                    _dep_cache[key] = "NONE"
            return
        legs = r.json()
        if isinstance(legs, dict):
            legs = [legs]
        if not isinstance(legs, list):
            with _dep_lock:
                _dep_cache[key] = "NONE"
            return
        # Choose the leg arriving at BNE whose scheduled arrival is closest to ours
        try:
            our_sch = pd.to_datetime(s_dt_iso)
        except Exception:
            our_sch = None
        our_date = our_sch.date() if our_sch is not None else None
        best, best_diff = None, None
        for leg in legs:
            arr = (leg or {}).get("arrival") or {}
            ap  = arr.get("airport") or {}
            if str(ap.get("icao", "")).upper() != AIRPORT_ICAO:
                continue
            sch = arr.get("scheduledTime") or {}
            raw = sch.get("utc") or sch.get("local")
            leg_local = None
            try:
                leg_local = (pd.to_datetime(raw, utc=True)
                             .tz_convert(TIMEZONE).tz_localize(None))
            except Exception:
                leg_local = None
            # Date gate: only consider legs arriving at BNE on our flight's date.
            # This is what stops a twice-daily route from matching yesterday's or
            # tomorrow's same-time leg (the CI53 wrong-reg bug).
            if our_date is not None and leg_local is not None:
                if leg_local.date() != our_date:
                    continue
            if our_sch is not None and leg_local is not None:
                base = our_sch.tz_localize(None) if our_sch.tzinfo else our_sch
                diff = abs((leg_local - base).total_seconds())
            else:
                diff = 0
            if best is None or diff < best_diff:
                best, best_diff = leg, diff
        result = {"found": False, "dep": None, "dep_actual": False, "reg": None}
        if best:
            result["found"] = True
            # Today's-leg airframe — used to cross-validate the FIDS reg, which
            # often carries the PREVIOUS day's aircraft until assignment.
            _lac = best.get("aircraft") or {}
            _leg_reg = _lac.get("reg") or _lac.get("registration")
            if _leg_reg:
                result["reg"] = str(_leg_reg).strip().upper()
            dep = best.get("departure") or {}
            def _extract(k):
                node = dep.get(k)
                raw = None
                if isinstance(node, dict):
                    raw = node.get("utc")
                elif isinstance(node, str):
                    raw = node
                if not raw:
                    raw = dep.get(k + "Utc")
                if raw:
                    try:
                        return (pd.to_datetime(raw, utc=True)
                                .tz_convert(TIMEZONE).strftime("%H:%M"))
                    except Exception:
                        return None
                return None
            # runwayTime / actualTime only exist AFTER wheels-up → "actual".
            # Otherwise fall back to revisedTime (radar estimate) → "estimated",
            # so a flight still on the ground shows its expected off-block time.
            for k in ("runwayTime", "actualTime"):
                _v = _extract(k)
                if _v:
                    result["dep"] = _v
                    result["dep_actual"] = True
                    break
            if not result["dep"]:
                _rev = _extract("revisedTime")
                if _rev:
                    result["dep"] = _rev
                    result["dep_actual"] = False
        with _dep_lock:
            _dep_cache[key] = result
    except Exception as e:
        log.warning("Dep time fetch failed for %s: %s", flight_num, e)
        with _dep_lock:
            _dep_fails[key] = datetime.now().timestamp()
    finally:
        with _dep_lock:
            _dep_pending.discard(key)


def get_flight_leg_info(flight_num: str, s_dt_iso: str):
    """NON-BLOCKING. Cached per-flight leg info dict
    {found, dep (HH:MM AEST), reg} or None while unknown. Kicks off one
    budgeted background lookup on first miss."""
    if not DEP_INFO_ENABLED or not flight_num or not s_dt_iso:
        return None
    key = f"{flight_num}|{s_dt_iso[:10]}"
    with _dep_lock:
        cached = _dep_cache.get(key)
        fail_ts = _dep_fails.get(key)
        already = key in _dep_pending
        if cached is None and not already:
            _dep_pending.add(key)
    if cached is not None:
        return cached if isinstance(cached, dict) else None
    if fail_ts and (datetime.now().timestamp() - fail_ts) < DEP_FAIL_TTL_SEC:
        with _dep_lock:
            _dep_pending.discard(key)
        return None
    if not already:
        threading.Thread(target=_fetch_dep_time_http,
                         args=(flight_num, s_dt_iso, key), daemon=True).start()
    return None


def get_dep_time(flight_num: str, s_dt_iso: str):
    info = get_flight_leg_info(flight_num, s_dt_iso)
    if info and info.get("dep"):
        return (info["dep"], info.get("dep_actual", False))
    return (None, False)


def get_photo_from_api(reg: str) -> str:
    """NON-BLOCKING. Returns a cached photo URL instantly if available, otherwise
    kicks off a background fetch and returns 'NOT_FOUND' for now. The photo will
    appear on a subsequent refresh once the background fetch completes — this keeps
    the board from freezing while waiting on Planespotters."""
    if not reg:
        return "NOT_FOUND"

    with _photo_lock:
        if reg in _photo_url_cache:
            return _photo_url_cache[reg]           # cached (URL or genuine miss)
        fail_entry = _photo_fails.get(reg)
        already_fetching = reg in _photo_pending

    # Don't retry recent transient failures yet
    if fail_entry and (datetime.now() - fail_entry).total_seconds() < PHOTO_FAIL_TTL_SEC:
        return "NOT_FOUND"

    # Kick off a background fetch (once) if not already running
    if not already_fetching:
        with _photo_lock:
            _photo_pending.add(reg)
        threading.Thread(target=_background_fetch_photo, args=(reg,), daemon=True).start()

    return "NOT_FOUND"   # not ready yet — will show on next refresh


_wx_last_good = {"data": None}   # survives transient Open-Meteo failures


@st.cache_data(ttl=1800, show_spinner=False)   # 30 min — Open-Meteo shares
def fetch_weather(anchor: str):                #   a rate-limited IP on Streamlit Cloud
    """Current conditions at BNE airport from Open-Meteo (free, no key, no
    AeroDataBox units). On failure, returns the last good reading so the strip
    doesn't vanish on a single hiccup (weather doesn't change in 5 minutes)."""
    import time as _time
    _params = {
        "latitude": -27.3842, "longitude": 153.1175,
        "current": "temperature_2m,wind_speed_10m,wind_direction_10m,weather_code",
        "hourly": "weather_code",
        "forecast_hours": 3,
        "timezone": "Australia/Brisbane",
    }
    _headers = {"User-Agent": "BNE-Arrivals-Board/1.0 (+https://github.com/phillipyeh89/bne-flight-board)"}
    try:
        r = None
        for _attempt in (1, 2, 3):
            r = requests.get("https://api.open-meteo.com/v1/forecast",
                             params=_params, headers=_headers, timeout=6)
            if r.status_code == 429:
                # Shared-IP rate limit — brief back-off then retry
                log.warning("Weather 429 (attempt %d) — backing off", _attempt)
                _time.sleep(1.5 * _attempt)
                continue
            break
        r.raise_for_status()
        body   = r.json() or {}
        cur    = body.get("current") or {}
        hourly = body.get("hourly") or {}
        if cur.get("temperature_2m") is None:
            log.warning("Weather: response had no temperature; body keys=%s", list(body.keys()))
            return _wx_last_good["data"]
        data = {
            "temp":     cur.get("temperature_2m"),
            "wind_kmh": cur.get("wind_speed_10m"),
            "wind_dir": cur.get("wind_direction_10m"),
            "code":     cur.get("weather_code"),
            "h_codes":  hourly.get("weather_code") or [],
            "h_times":  hourly.get("time") or [],
        }
        _wx_last_good["data"] = data
        return data
    except Exception as e:
        log.warning("Weather fetch failed (%s) — using last good reading", e)
        return _wx_last_good["data"]


# WMO codes we use for METAR-derived conditions, so the existing _wmo_condition
# and _wx_severity mappings apply unchanged.
def _metar_to_wmo(raw: str):
    """Approximate a WMO weather code from a raw METAR string, prioritising the
    operationally important states (fog, thunderstorm, rain). Returns None if
    nothing notable — caller then infers from cloud cover."""
    if not raw:
        return None
    s = raw.upper()
    # Present-weather groups (most severe first)
    if any(x in s for x in (" TS", "+TS", "TSRA")):        return 95   # thunderstorm
    if " FG" in s or " FZFG" in s or "MIFG" in s:           return 45   # fog
    if "BR" in s.split():                                    return 45   # mist → treat as fog-class for ops
    if any(x in s for x in ("+RA", "SHRA", " RA")):        return 63   # rain
    if any(x in s for x in ("DZ",)):                        return 53   # drizzle
    if any(x in s for x in ("SN",)):                        return 73   # snow
    # No present-weather → derive from sky condition
    if "OVC" in s or "BKN" in s:                            return 3    # cloudy
    if "SCT" in s or "FEW" in s:                            return 2    # partly cloudy
    if "CLR" in s or "SKC" in s or "CAVOK" in s or "NCD" in s: return 0 # clear
    return None


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_metar(anchor: str):
    """Current conditions at YBBN from NOAA Aviation Weather (free, no key, no
    rate-limit issues). Returns dict with temp/wind/code/visibility/flight_cat,
    or None on failure. This is REAL airport observation data — visibility and
    flight category are exactly what matters on fog days."""
    try:
        r = requests.get(
            "https://aviationweather.gov/api/data/metar",
            params={"ids": AIRPORT_ICAO, "format": "json"},
            headers={"User-Agent": "BNE-Arrivals-Board/1.0 (+https://github.com/phillipyeh89/bne-flight-board)"},
            timeout=6,
        )
        r.raise_for_status()
        arr = r.json()
        if not isinstance(arr, list) or not arr:
            log.warning("METAR: empty/unexpected response for %s", AIRPORT_ICAO)
            return None
        m = arr[0]
        raw = m.get("rawOb") or m.get("raw_text") or ""
        code = _metar_to_wmo(raw)
        # Visibility: METAR gives statute miles (float) in "visib"
        vis = m.get("visib")
        try:
            vis_km = round(float(str(vis).replace("+", "")) * 1.60934, 1) if vis is not None else None
        except (ValueError, TypeError):
            vis_km = None
        data = {
            "temp":     m.get("temp"),
            "wind_kmh": (round(m.get("wspd") * 1.852, 1) if m.get("wspd") is not None else None),  # kt→km/h
            "wind_dir": m.get("wdir") if isinstance(m.get("wdir"), (int, float)) else None,
            "code":     code if code is not None else 0,
            "vis_km":   vis_km,
            "source":   "metar",
        }
        return data if data["temp"] is not None else None
    except Exception as e:
        log.warning("METAR fetch failed (%s) — falling back to Open-Meteo", e)
        return None


def _wmo_condition(code):
    """Map WMO weather code → (emoji, i18n key). Fog gets special handling."""
    if code is None:               return ("", None)
    c = int(code)
    if c == 0:                     return ("☀️", "wx_clear")
    if c in (1, 2):                return ("⛅", "wx_pcloudy")
    if c == 3:                     return ("☁️", "wx_cloudy")
    if c in (45, 48):              return ("🌫️", "wx_fog")
    if c in (51, 53, 55):          return ("🌦️", "wx_drizzle")
    if c in (61, 63, 65, 66, 67):  return ("🌧️", "wx_rain")
    if c in (71, 73, 75, 77, 85, 86): return ("🌨️", "wx_snow")
    if c in (80, 81, 82):          return ("🌧️", "wx_showers")
    if c in (95, 96, 99):          return ("⛈️", "wx_storm")
    return ("🌡️", None)


def _wx_severity(code) -> int:
    """Operational-impact rank of a WMO code. Higher = worse for arrivals.
    Used only to decide whether the next couple of hours are getting BETTER
    or WORSE than now — not a meteorological science."""
    if code is None:                  return 0
    c = int(code)
    if c in (95, 96, 99):             return 5   # thunderstorm
    if c in (45, 48):                 return 4   # fog — the big one for BNE ops
    if c in (61, 63, 65, 66, 67, 80, 81, 82): return 3   # rain / showers
    if c in (51, 53, 55):             return 2   # drizzle
    if c == 3:                        return 1   # overcast
    return 0                                     # clear-ish


def _wx_upcoming_change(wx, now_aest):
    """Scan the next ~3 hourly forecast codes. If conditions will materially
    change (different severity class), return (emoji, key, hour_str, worsening).
    Returns None when conditions stay in the same class — the common case."""
    try:
        cur_sev = _wx_severity(wx.get("code"))
        for t_str, code in zip(wx.get("h_times") or [], wx.get("h_codes") or []):
            _hr = datetime.strptime(t_str, "%Y-%m-%dT%H:%M")
            if _hr <= now_aest.replace(tzinfo=None):
                continue
            sev = _wx_severity(code)
            if sev != cur_sev:
                emoji, key = _wmo_condition(code)
                return (emoji, key, _hr.strftime("%H:%M"), sev > cur_sev)
    except Exception as e:
        log.warning("Weather change scan failed: %s", e)
    return None


def _wx_forecast_3h(wx, now_aest, t):
    """Build a compact always-on 3-hour outlook: 'HH ☀️ · HH ⛅ · HH 🌫️'.
    Hours whose severity is worse than now are tinted amber. Returns "" if no
    hourly data is available."""
    try:
        cur_sev = _wx_severity(wx.get("code"))
        cutoff  = now_aest.replace(tzinfo=None)
        parts = []
        for t_str, code in zip(wx.get("h_times") or [], wx.get("h_codes") or []):
            _hr = datetime.strptime(t_str, "%Y-%m-%dT%H:%M")
            if _hr <= cutoff:
                continue
            emoji, _ = _wmo_condition(code)
            worse = _wx_severity(code) > cur_sev
            col   = t.c_amber if worse else t.text_muted
            parts.append(
                f'<span style="color:{col};">{_hr.strftime("%H")}{emoji}</span>'
            )
            if len(parts) >= 3:
                break
        if not parts:
            return ""
        sep = '<span style="opacity:0.4; margin:0 5px;">·</span>'
        return sep.join(parts)
    except Exception as e:
        log.warning("Weather forecast build failed: %s", e)
        return ""


@st.cache_data(ttl=API_DATA_TTL_SEC, show_spinner=False)
def fetch_flight_data(anchor: str, from_time: str, to_time: str) -> list:
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{AIRPORT_ICAO}/{from_time}/{to_time}"
    headers = {
        "X-RapidAPI-Key":  st.secrets["X_RAPIDAPI_KEY"],
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
    }
    params = {"direction": "Arrival", "withCancelled": "true", "withCodeshared": "false"}
    # Try once with 15s timeout, retry once on timeout/connection error before giving up.
    last_err = None
    for attempt in (1, 2):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=15)
            r.raise_for_status()
            st.session_state.api_last_hit = datetime.now(pytz.timezone(TIMEZONE))
            return r.json().get("arrivals", [])
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
            log.warning("AeroDataBox attempt %d failed (%s) — retrying", attempt, type(e).__name__)
            continue
        except requests.HTTPError as e:
            last_err = e
            # Do NOT retry 429. A per-second collision would clear on retry, but
            # a quota exhaustion does not — and retrying then triples our request
            # count against an already-exhausted quota, making things worse.
            # A single failed cycle is harmless; the next refresh retries anyway.
            break
        except Exception as e:
            last_err = e
            break
    log.error("AeroDataBox API error: %s", last_err)
    # Raise instead of returning [] — st.cache_data does NOT cache exceptions,
    # so a transient failure no longer blanks the board for a whole 16-min cache
    # window. The call site catches this, shows the error, and the next 60s
    # fragment rerun retries (subject to the backoff below).
    raise RuntimeError(str(last_err))


def _iata_to_callsign(flight_number: str) -> str:
    # IATA airline codes are the first 2 chars and may contain digits (3K, 5J),
    # so take a positional prefix. Then take ONLY the leading run of digits —
    # stopping at the first non-digit — so any suffix that got appended to the
    # number (leg/segment digits from codeshare or malformed data, e.g.
    # "CI 5312" instead of "CI 53") can't leak into the callsign.
    compact = flight_number.replace(" ", "").upper()
    prefix, rest = compact[:2], compact[2:]
    m = re.match(r"\d+", rest)
    digits = m.group(0) if m else ""
    return f"{AIRLINE_ICAO.get(prefix, prefix)}{digits}"


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3440.065
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


@st.cache_data(ttl=OPENSKY_TTL_SEC, show_spinner=False)
def fetch_opensky_states(anchor: str) -> dict:
    if not OPENSKY_ENABLED:
        return {}
    try:
        r = requests.get(
            "https://opensky-network.org/api/states/all",
            params=OPENSKY_BBOX,
            headers={"User-Agent": "BNE-Board-App/2.0"},
            timeout=2,
        )
        if r.status_code == 200:
            result = {}
            for s in (r.json().get("states") or []):
                callsign  = (s[1] or "").strip().upper()
                on_ground = s[8]
                velocity  = s[9]
                lat, lon = s[6], s[5]
                if callsign and not on_ground and velocity and lat is not None and lon is not None:
                    result[callsign] = {
                        "lat": lat, "lon": lon,
                        "velocity_kts": velocity * 1.94384,
                    }
            return result
        elif r.status_code == 429:
            log.warning("OpenSky rate limited — skipping this cycle")
    except Exception as e:
        log.warning("OpenSky query failed: %s", e)
    return {}


def opensky_estimate_eta(flight_number: str, opensky_data: dict, now: datetime):
    state = opensky_data.get(_iata_to_callsign(flight_number))
    if not state or state["velocity_kts"] < OPENSKY_MIN_SPEED_KT:
        return None, ""
    dist_nm = _haversine_nm(state["lat"], state["lon"], YBBN_LAT, YBBN_LON)
    eta_min = int(dist_nm / state["velocity_kts"] * 60)
    if eta_min < 1 or eta_min > OPENSKY_MAX_ETA_MIN:
        return None, ""
    return now + timedelta(minutes=eta_min), "revised"


# ─────────────────────────────────────────────
#  4. UI SETUP & FRAGMENT EXECUTION (V12.44)
# ─────────────────────────────────────────────
st.set_page_config(page_title="BNE Pro Arrivals", page_icon="✈️", layout="centered")
if "api_last_hit" not in st.session_state: st.session_state.api_last_hit = None
if "api_error"    not in st.session_state: st.session_state.api_error    = None
# Settings persist across page refreshes via URL query params
# (?theme=light&font=19&lang=zh). Each new browser session seeds its
# session_state from the URL, and every settings change writes back to it —
# so F5, browser restarts, and PWA relaunches all keep the user's choices.
def _qp_get(key: str):
    """Read a URL query param defensively. Streamlit versions differ on the
    return type (str vs list) and this must never break app startup."""
    try:
        v = st.query_params.get(key)
        if isinstance(v, (list, tuple)):
            v = v[0] if v else None
        return str(v) if v is not None else None
    except Exception:
        return None


def _qp_set(key: str, value: str):
    """Write a URL query param. Best-effort — a failure must never break a
    settings click, so swallow any error."""
    try:
        st.query_params[key] = value
    except Exception:
        pass


if "theme_light" not in st.session_state:
    st.session_state.theme_light = (_qp_get("theme") == "light")
if "font_size" not in st.session_state:
    try:
        _f = int(_qp_get("font") or 16)
    except (TypeError, ValueError):
        _f = 16
    st.session_state.font_size = min(24, max(13, _f))
if "lang" not in st.session_state:
    _l = _qp_get("lang")
    st.session_state.lang = _l if _l in LANG_OPTIONS else "en"


# FIX 5 — use UI_REFRESH_SEC constant instead of hardcoded "60s"
def _live_dashboard_impl():
    aest     = pytz.timezone(TIMEZONE)
    now_aest = datetime.now(aest)
    t        = get_theme(st.session_state.theme_light)

    # Inject dynamic CSS first so header styling is correct
    st.markdown(get_dynamic_css(t, st.session_state.font_size), unsafe_allow_html=True)

    # On narrow mobile screens, multiple text buttons stack vertically.
    # Use a single Streamlit selectbox in the sidebar-style menu instead,
    # OR collapse all controls into one popover button.
    # Header is wrapped defensively: a failure while building the controls must
    # never prevent the flight list below from rendering (V12.44 — a broken
    # header previously left the ⚙️ button full-width and no flights at all).
    # Whole-number weights only — fractional widths (e.g. 1.2) make Streamlit's
    # flexbox wrap the columns into separate rows on narrow phones, which is why
    # the gear button previously rendered full-width on its own line.
    c1, c_ctrl, c3 = st.columns([4, 1, 3])
    with c1:
        st.subheader("✈️ Arrivals")
    with c_ctrl:
        with st.popover("⚙️", use_container_width=True):
            st.markdown(f"**{L('text_size')}**")
            cA, cB = st.columns(2)
            with cA:
                if st.button("A−", help="Smaller", use_container_width=True, key="font_smaller"):
                    st.session_state.font_size = max(13, st.session_state.font_size - 3)
                    _qp_set("font", str(st.session_state.font_size))
                    st.rerun()
            with cB:
                if st.button("A+", help="Larger", use_container_width=True, key="font_larger"):
                    st.session_state.font_size = min(24, st.session_state.font_size + 3)
                    _qp_set("font", str(st.session_state.font_size))
                    st.rerun()
            st.markdown(f"**{L('theme')}**")
            toggle_icon = L("dark") if st.session_state.theme_light else L("light")
            if st.button(toggle_icon, use_container_width=True, key="theme_toggle"):
                st.session_state.theme_light = not st.session_state.theme_light
                _qp_set("theme", "light" if st.session_state.theme_light else "dark")
                st.rerun()
            st.markdown(f"**{L('language')}**")
            _lang_keys = list(LANG_OPTIONS.keys())
            _sel = st.selectbox(
                L("language"), _lang_keys,
                index=_lang_keys.index(st.session_state.lang),
                format_func=lambda k: LANG_OPTIONS[k],
                key="lang_select", label_visibility="collapsed",
            )
            if _sel != st.session_state.lang:
                st.session_state.lang = _sel
                _qp_set("lang", _sel)
                st.rerun()
    with c3:
        st.markdown(
            f'<div style="font-size:0.8em;color:{t.text_muted};text-align:right;margin-top:5px;">'
            f'🕒 <span id="bne-live-clock">{now_aest.strftime("%H:%M:%S")}</span></div>',
            unsafe_allow_html=True,
        )
        api_info_placeholder = st.empty()

    _lang = st.session_state.lang
    with st.expander("ℹ️ Guide"):
        if _lang == "zh":
            st.markdown(f"""
            **時間標籤**：<span class="mono" style="color:{t.c_blue};font-weight:bold;">Act</span> 實際降落 · <span class="mono" style="color:{t.text_faded};font-weight:bold;">Est</span> 雷達預估（已提前約10分校正）· <span class="mono" style="color:{t.text_muted};font-weight:bold;">Sch</span> 表定（無雷達）· **起飛/預計起飛** 出發時間

            **狀態**：<span style="color:{t.c_amber};">⚠️ 請看機場看板</span> 無雷達 · <span style="color:{t.c_red};">已落地滑行中</span> 過預計未確認 · <span style="color:{t.c_green};">剛降落/X前降落</span> · 🟠 誤點3h+ / 🔴 12h+ · <span style="color:{t.c_red};">⚡ 高峰</span> 15分內3班或2廣體 · <span style="color:{t.c_purple};">✈️ 轉降</span> 不到BNE · 門號下「⚠ 原XX」= 剛換門（60分後消失）

            **空檔條**：🟢 現在就是休息空檔（倒數剩餘）· 🔄 未來空檔 ·「約」= 下一班僅表定，結束時間可能變動

            **天氣列**：機場即時天況，起霧轉琥珀色。「→ 🌫️ ~06:00」= 約3小時內變化（琥珀=轉壞、綠=轉好）

            **營運異常模式**：偵測多班轉降或逾時未落地時，顯示警告橫幅、暫停自動判定，改以機場看板為準

            **點擊操作**：航班號 → Flightradar24（在飛顯示即時地圖）· 飛機照片 → 放大＋機型機齡座位＋更多照片連結

            **頂部**：更新時間 ·（+約10分延遲）AeroDataBox本身延遲 · 下次更新倒數（每5分鐘）· 休眠時段 01:00–03:00 AEST

            **設定 ⚙️**：字體大小、深淺色、語言（EN/繁中/한국어/日本語）

            *由 Phillip Yeh 開發，支援 BNE Lotte 團隊。資料：AeroDataBox + Open-Meteo。*
            """, unsafe_allow_html=True)
        elif _lang == "ko":
            st.markdown(f"""
            **시간 태그**：<span class="mono" style="color:{t.c_blue};font-weight:bold;">Act</span> 실제 착륙 · <span class="mono" style="color:{t.text_faded};font-weight:bold;">Est</span> 레이더 예상(약10분 앞당겨 보정) · <span class="mono" style="color:{t.text_muted};font-weight:bold;">Sch</span> 예정(레이더 없음) · **출발/출발예정** 출발 시간

            **상태**：<span style="color:{t.c_amber};">⚠️ 안내판 확인</span> 레이더 없음 · <span style="color:{t.c_red};">지상 이동 중</span> · <span style="color:{t.c_green};">방금/X전 착륙</span> · 🟠 3h+ / 🔴 12h+ 지연 · <span style="color:{t.c_red};">⚡ 혼잡</span> 15분내 3편 또는 대형기 2편 · <span style="color:{t.c_purple};">✈️ 회항</span> · 게이트 아래「⚠ 이전XX」= 최근 변경(60분후 사라짐)

            **공백 바**：🟢 지금 휴식 시간(남은 시간) · 🔄 예정 공백 ·「약」= 다음 편 예정 시간만, 변동 가능

            **날씨 표시줄**：공항 실시간 날씨, 안개 시 황색.「→ 🌫️ ~06:00」= 약3시간내 변화(황색=악화, 녹색=호전)

            **운영 차질 모드**：다수 회항 또는 착륙 미확인 지연 감지 시 경고 배너 표시, 자동 판정 중단, 안내판 우선

            **클릭**：항공편 번호 → Flightradar24(비행 중 실시간 지도) · 항공기 사진 → 확대＋기종·기령·좌석＋추가 사진 링크

            **상단**：업데이트 시간 ·(+약10분 지연) · 다음 새로고침(5분마다) · 대기 시간 01:00–03:00 AEST

            **설정 ⚙️**：글자 크기, 테마, 언어

            *BNE Lotte 팀을 위해 Phillip Yeh 개발. 데이터: AeroDataBox + Open-Meteo.*
            """, unsafe_allow_html=True)
        elif _lang == "ja":
            st.markdown(f"""
            **時刻タグ**：<span class="mono" style="color:{t.c_blue};font-weight:bold;">Act</span> 実際の着陸 · <span class="mono" style="color:{t.text_faded};font-weight:bold;">Est</span> レーダー推定(約10分早め補正) · <span class="mono" style="color:{t.text_muted};font-weight:bold;">Sch</span> 定刻(レーダーなし) · **出発/出発予定** 出発時刻

            **ステータス**：<span style="color:{t.c_amber};">⚠️ 案内板確認</span> レーダーなし · <span style="color:{t.c_red};">地上走行中</span> · <span style="color:{t.c_green};">着陸直後/X前に着陸</span> · 🟠 3h+ / 🔴 12h+ 遅延 · <span style="color:{t.c_red};">⚡ ピーク</span> 15分内3便または大型機2機 · <span style="color:{t.c_purple};">✈️ ダイバート</span> · ゲート下「⚠ 旧XX」= 最近変更(60分後消滅)

            **空きバー**：🟢 今が休憩時間(残り) · 🔄 今後の空き ·「約」= 次便が定刻のみ、変動あり

            **天気バー**：空港のリアルタイム天候、霧は琥珀色。「→ 🌫️ ~06:00」= 約3時間内の変化(琥珀=悪化、緑=好転)

            **運用乱れモード**：複数ダイバートまたは着陸未確認の遅延を検出時、警告バナー表示、自動判定停止、案内板優先

            **クリック**：便名 → Flightradar24(飛行中はリアルタイム地図) · 機体写真 → 拡大＋機種・機齢・座席＋追加写真リンク

            **ヘッダー**：更新時刻 ·(+約10分遅延) · 次の更新(5分ごと) · スリープ 01:00–03:00 AEST

            **設定 ⚙️**：文字サイズ、テーマ、言語

            *BNE Lotteチームのため Phillip Yeh が開発。データ: AeroDataBox + Open-Meteo.*
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            **Time tags**: <span class="mono" style="color:{t.c_blue};font-weight:bold;">Act</span> actual landing · <span class="mono" style="color:{t.text_faded};font-weight:bold;">Est</span> radar estimate (~10 min earlier-adjusted) · <span class="mono" style="color:{t.text_muted};font-weight:bold;">Sch</span> scheduled (no radar) · **Dep / Dep~** departure time

            **Status**: <span style="color:{t.c_amber};">⚠️ Check Board</span> no radar · <span style="color:{t.c_red};">On Ground</span> past ETA, unconfirmed · <span style="color:{t.c_green};">Just Landed / Landed Xm ago</span> · 🟠 3h+ / 🔴 12h+ delay · <span style="color:{t.c_red};">⚡ Surge</span> 3 flights in 15 min or 2 widebodies · <span style="color:{t.c_purple};">✈️ Diverted</span> · "⚠ was XX" under a gate = recent change (clears after 60 min)

            **Gap bars**: 🟢 break window active now (countdown) · 🔄 upcoming gap · "approx" = end based on a Sch-only flight, may shift

            **Weather strip**: live BNE conditions; fog shows amber. "→ 🌫️ ~06:00" = change within ~3h (amber worsening, green improving)

            **Disruption mode**: on multiple diversions or several unconfirmed-past-ETA flights, a banner shows, auto-landed inference pauses, trust the FIDS board

            **Tap**: flight number → Flightradar24 (live map when airborne) · aircraft photo → enlarge + type/age/seats + more-photos link

            **Header**: update time · (+~10m lag) AeroDataBox's own lag · next-refresh countdown (every 5 min) · quiet hours 01:00–03:00 AEST

            **Settings ⚙️**: text size, theme, language (EN / 繁中 / 한국어 / 日本語)

            *Built by Phillip Yeh for the BNE Lotte Team. Data: AeroDataBox + Open-Meteo.*
            """, unsafe_allow_html=True)

    # ── Fetch ──────────────────────────────────────────────────────────────────
    _epoch     = datetime(2000, 1, 1, tzinfo=aest)

    # Single quantised anchor — all cache keys and time windows derive from this
    # so the cache key is stable for the full API_DATA_TTL_SEC window.
    anchor_dt  = _epoch + timedelta(seconds=(int((now_aest - _epoch).total_seconds()) // API_DATA_TTL_SEC) * API_DATA_TTL_SEC)
    anchor     = anchor_dt.strftime("%Y-%m-%dT%H:%M")
    from_time  = (anchor_dt - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M")
    to_time    = (anchor_dt + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M")
    # Quiet hours: bail out before hitting the API at all. Saves ~120 units/month
    # by not refreshing during dead hours when nobody is using the board anyway.
    in_quiet_hours = QUIET_HOURS_START_H <= now_aest.hour < QUIET_HOURS_END_H
    if in_quiet_hours:
        st.info(L("quiet", h=f"{QUIET_HOURS_END_H:02d}"))
        return

    import time as _t
    if _t.time() < _fids_fail_until[0]:
        # Recent failure — in backoff. Render the error state without a new call.
        raw_flights = []
    else:
        try:
            raw_flights = fetch_flight_data(anchor, from_time, to_time)
        except Exception as e:
            st.session_state.api_error = str(e)
            _fids_fail_until[0] = _t.time() + FIDS_FAIL_BACKOFF_SEC
            raw_flights = []
    opensky_data = fetch_opensky_states(anchor)

    # An empty result means the API call failed or returned nothing. Surface it
    # immediately with the underlying error rather than leaving the user on an
    # indefinite "Synchronizing radar..." with no explanation. (api_error is set
    # inside a cached function, so on cache hits it may be absent — hence the
    # generic fallback message.)
    if not raw_flights:
        _err = st.session_state.get("api_error")
        if _err:
            st.error(f"⚠️ Could not load flights — {_err}")
        else:
            st.warning(
                "⚠️ No flight data returned. The AeroDataBox API is refusing "
                "requests — most often the monthly quota is exhausted, or the "
                "subscription/API key needs attention. Check the RapidAPI "
                "dashboard. Retrying automatically each refresh."
            )
        st.caption(f"Last attempt: {now_aest.strftime('%H:%M:%S')} AEST")
        st.session_state.api_error = None
        return

    # Always keep api_last_hit current — on cache hits the function body
    # doesn't run so we set it here using anchor_dt as the proxy.
    if raw_flights and (not st.session_state.api_last_hit
                        or st.session_state.api_last_hit < anchor_dt):
        st.session_state.api_last_hit = anchor_dt

    # Now fill the header placeholder — we do this AFTER the fetch so
    # api_last_hit is always populated before the countdown renders.
    api_t = st.session_state.get("api_last_hit")
    if api_t:
        next_refresh_dt  = api_t + timedelta(seconds=API_DATA_TTL_SEC)
        secs_until       = max(0, int((next_refresh_dt - now_aest).total_seconds()))
        mins_until, secs = divmod(secs_until, 60)
        refresh_txt      = f'{mins_until}m {secs:02d}s' if mins_until else f'{secs}s'

        # Make data freshness obvious to non-technical users:
        # 1. "Updated X min ago" is more intuitive than a bare HH:MM timestamp
        # 2. Show the inherent ~15min AeroDataBox lag so users know how stale
        #    the underlying radar data is, not just when we fetched it
        age_secs = max(0, int((now_aest - api_t).total_seconds()))
        age_mins = age_secs // 60
        if age_mins == 0:
            updated_txt = L("just_now")
        else:
            updated_txt = L("updated_ago", x=L("min_ago", n=age_mins))

        api_txt = (
            f'<span style="color:{t.text_faded};">{updated_txt}</span>'
            f' <span style="color:{t.c_amber}; opacity:0.8;" title="{L("lag_tip")}">'
            f'{L("lag_note")}</span><br>'
            f'<span style="color:{t.text_faded};">{L("next_refresh")}</span>'
            f'<span id="bne-refresh-countdown" '
            f'data-next="{int(next_refresh_dt.timestamp())}" '
            f'style="color:{t.c_green};">{refresh_txt}</span>'
        )
    else:
        api_txt = f'<span style="color:{t.text_faded};">{L("loading")}</span>'
    api_info_placeholder.markdown(
        f'<div style="font-size:0.7em;color:{t.text_faded};text-align:right; line-height:1.5;">{api_txt}</div>',
        unsafe_allow_html=True,
    )

    if st.session_state.api_error:
        st.error(f"⚠️ API Error — {st.session_state.api_error}")
        st.session_state.api_error = None

    if not raw_flights:
        st.info("⏳ Synchronizing radar... data will appear on next refresh.")
        return

    # ── Dedup: flight number, then same-aircraft codeshare ────────────────────
    seen = {}
    for f in raw_flights:
        num = f.get("number")
        if num and num not in seen:
            seen[num] = f

    physical_seen, deduped_flights = {}, []
    for f in seen.values():
        dep_ap  = (f.get("departure") or {}).get("airport") or {}
        arr     = f.get("arrival") or f.get("movement") or {}
        sch     = arr.get("scheduledTime")
        sch_str = sch.get("local", "") if isinstance(sch, dict) else ""
        ac_dict_dd = f.get("aircraft") or {}
        # Include airline ICAO prefix so two unrelated airlines with the same origin
        # + scheduled minute don't collapse into one card when aircraft model isn't
        # known yet (~3h pre-arrival window).
        flight_num_dd  = f.get("number") or ""
        airline_prefix = flight_num_dd.replace(" ", "").upper()[:2]
        phy_key = f"{airline_prefix}|{str(dep_ap.get('iata', ''))}|{sch_str}|{ac_dict_dd.get('model') or ''}"

        if phy_key and phy_key != "||" and phy_key in physical_seen:
            existing = physical_seen[phy_key]
            existing_ac = existing.get("aircraft") or {}
            if not (existing_ac.get("reg") or "") and (ac_dict_dd.get("reg") or ""):
                deduped_flights.remove(existing)
                physical_seen[phy_key] = f
                deduped_flights.append(f)
            continue
        physical_seen[phy_key] = f
        deduped_flights.append(f)

    # Pre-warm photo cache without blocking — get_photo_from_api spawns background
    # fetches and returns immediately, so the board renders right away and photos
    # fill in on subsequent refreshes.
    all_regs = list({(f.get("aircraft") or {}).get("reg") or ""
                     for f in deduped_flights if (f.get("aircraft") or {}).get("reg")})
    for _reg in all_regs:
        get_photo_from_api(_reg)
        get_aircraft_info(_reg)   # non-blocking Tier-1 lookup: age / seats / freighter

    # ── Process flights ───────────────────────────────────────────────────────
    processed = []
    # Disruption mode: when multiple flights in the window carry a diverted/
    # redirected status (fog, storms, runway closure), the "past ETA = must have
    # landed" inference becomes unreliable — aircraft are holding or diverting,
    # not landing. Detect it and switch the board to honest-uncertainty mode.
    _divert_count = sum(
        1 for _f in deduped_flights
        if "divert" in str(_f.get("status", "")).lower()
        or "redirect" in str(_f.get("status", "")).lower()
    )
    # Second trigger — "confirmation drought": several radar-tracked flights are
    # well past their Est with no landing confirmations at all. This catches the
    # EARLY phase of a fog event (aircraft holding, nothing diverted yet) and
    # disruption types that never produce a diverted status (ground stops,
    # runway closures).
    _stuck_count = 0
    for _f in deduped_flights:
        _bd, _tt = extract_best_time(_f.get("arrival") or {}, aest)
        if _bd is None:
            continue
        if _tt == "actual" and (now_aest - _bd).total_seconds() < 3600:
            _stuck_count = 0
            break          # a recent confirmed landing = no drought
        if _tt == "revised" and (now_aest - _bd).total_seconds() > 15 * 60:
            _stuck_count += 1
    disruption_mode = (_divert_count >= 2) or (_stuck_count >= 3)

    # Drop codeshare marketing duplicates — keep only the operating carrier's
    # record. AeroDataBox tags each with codeshareStatus: "IsOperator" (the real
    # flight) vs "IsCodeshared" (a marketing alias of another flight's aircraft).
    # Filtering these removes the swarm of duplicate cards AND many of the
    # malformed foreign-destination records (QF60-style) that ride in as aliases.
    deduped_flights = [
        _f for _f in deduped_flights
        if str(_f.get("codeshareStatus", "")).lower() != "iscodeshared"
    ]

    for f in deduped_flights:
        flight_num = f.get("number", "N/A")
        if flight_num in GHOST_FLIGHTS:
            continue

        status_raw  = f.get("status", "").lower()
        dep_node    = f.get("departure") or {}
        dep_ap      = dep_node.get("airport") or (f.get("movement") or {}).get("airport") or {}
        arr         = f.get("arrival") or f.get("movement") or {}
        ac_dict     = f.get("aircraft") or {}
        ac_m        = ac_dict.get("model") or ""
        ac_r        = ac_dict.get("reg") or ""
        origin_iata = str(dep_ap.get("iata", ""))

        # FIX 7 — filter flights that depart from BNE (i.e. are departures, not arrivals).
        # AeroDataBox occasionally includes outbound flights in the arrivals feed
        # (e.g. CZ 382 BNE→CAN, NZ 203 BNE→CHC). The most reliable cross-schema
        # check is the departure airport: if a flight originates at YBBN/BNE it
        # cannot be an arrival here, regardless of which response schema is used.
        dep_origin_icao = str(dep_ap.get("icao", "")).upper()
        dep_origin_iata = str(dep_ap.get("iata", "")).upper()
        # Diverted and canceled flights are exempt from the BNE-departure filter:
        # — A diverted flight that turns back after takeoff returns to BNE legitimately.
        # — A return-to-gate (RTG) flight (e.g. pushed back, fault found, returned)
        #   may be marked "canceled" rather than "diverted" by AeroDataBox; we still
        #   want it to appear in the canceled section rather than be silently dropped.
        _bne_origin = (dep_origin_icao == AIRPORT_ICAO or dep_origin_iata == "BNE")
        _rtg_exempt = status_raw in ("diverted", "canceled", "cancelled")
        if _bne_origin and not _rtg_exempt:
            log.info("Skipping %s — departure airport is BNE; this is an outbound flight", flight_num)
            continue
        # Secondary check for arrival-schema records: confirm destination is BNE.
        arrival_node = f.get("arrival")
        _mismatch_diverted = False
        if arrival_node:
            arr_ap   = arrival_node.get("airport") or {}
            arr_icao = str(arr_ap.get("icao", "")).upper()
            arr_iata = str(arr_ap.get("iata", "")).upper()
            _apt_mismatch = ((arr_icao and arr_icao != AIRPORT_ICAO)
                             or (arr_iata and arr_iata not in ("BNE", "")))
            if _apt_mismatch:
                # AeroDataBox may rewrite the arrival airport to the diversion
                # field. Dropping such records made diverted flights vanish from
                # the board on fog days. If the status says diverted — or we're
                # in disruption mode — keep the flight and mark it diverted.
                # Otherwise treat it as a wrong-schema outbound record and skip.
                if ("divert" in status_raw or "redirect" in status_raw
                        or disruption_mode):
                    _mismatch_diverted = True
                    log.info("Keeping %s as DIVERTED — arrival airport is %s",
                             flight_num, arr_icao or arr_iata)
                else:
                    log.info("Skipping %s — destination is %s, not %s",
                             flight_num, arr_icao or arr_iata, AIRPORT_ICAO)
                    continue

        # Diverted records often come back with wiped terminal/country fields,
        # which this heuristic filter would silently drop — exempt them so a
        # diverted flight can never vanish from the board.
        _div_candidate = ("divert" in status_raw or "redirect" in status_raw
                          or _mismatch_diverted)
        if not _div_candidate and not is_strictly_international(
                str(arr.get("terminal", "")),
                str(dep_ap.get("countryCode", "")),
                ac_m, origin_iata, ac_r):
            continue

        best_dt, t_type = extract_best_time(arr, aest)
        if not best_dt:
            continue

        sch_val = arr.get("scheduledTime")
        sch_raw = sch_val.get("local") if isinstance(sch_val, dict) else None
        if sch_raw:
            try:
                s_dt = aest.localize(pd.to_datetime(sch_raw).replace(tzinfo=None))
            except Exception as e:
                log.warning("Scheduled time parse error for %s: %s", flight_num, e)
                s_dt = best_dt
        else:
            s_dt = best_dt

        # NOTE: this FIDS endpoint ships an EMPTY departure object ({}) for every
        # flight (verified 2026-07-31 via DEP DEBUG) — so has_departed can only
        # ever come true via the status field. Departure times would need the
        # per-flight endpoint at ~2 units/flight, which the quota can't afford.
        has_departed = (dep_node.get("actualTime") is not None) or (status_raw in AIRBORNE_STATUSES)
        # If the flight hasn't departed and revisedTime is identical to scheduled
        # (within 60s), it's not real updated info — treat as scheduled. But if
        # there's a meaningful difference, the airline has updated the ETA based
        # on operational knowledge (e.g. known origin delay), so trust it.
        if t_type == "revised" and abs((best_dt - s_dt).total_seconds()) < 60 and not has_departed:
            t_type = "scheduled"

        # Compensation: AeroDataBox's live "Est" (revised) times consistently run
        # ~10 min later than the actual observed touchdown (verified against real
        # landings — VA58, JQ104, QF52). Shift Est times earlier so the board
        # predicts real arrival more closely.
        #
        # Compensation was calibrated on flights CLOSE to landing (VA58/JQ104/
        # QF52 observations) — applying it to a flight 6h out extrapolates far
        # beyond the evidence. Only compensate inside the final-approach window
        # (≤90 min to arrival); further-out flights show the raw Est.
        if (t_type == "revised"
                and (best_dt - now_aest) <= timedelta(minutes=90)):
            best_dt = best_dt - timedelta(minutes=EST_COMPENSATION_MINS)

        # Not-operating-today filter: scheduled, no reg, no departure, < 3h out
        hours_until = (best_dt - now_aest).total_seconds() / 3600
        if (0 < hours_until < 3 and not ac_r and not has_departed
                and t_type == "scheduled"
                and status_raw not in ("landed", "arrived", "canceled", "cancelled", "diverted")):
            log.info("Filtering %s — likely not operating (no reg/departure, arriving in %.1fh)",
                     flight_num, hours_until)
            continue

        # OpenSky supplement: use for scheduled-only, or close-in flights where
        # live ADS-B position beats AeroDataBox's ~15 min stale data
        if status_raw not in ("canceled", "cancelled", "diverted"):
            preliminary_mins = int((best_dt - now_aest).total_seconds() / 60)
            use_opensky = (t_type == "scheduled"
                           or (t_type == "revised" and 0 < preliminary_mins < OPENSKY_PREFER_UNDER_MIN))
            if use_opensky:
                osky_dt, osky_type = opensky_estimate_eta(flight_num, opensky_data, now_aest)
                if osky_dt:
                    best_dt, t_type = osky_dt, osky_type

        delay = (best_dt - s_dt).total_seconds() / 3600
        if delay < -2 or delay > 24:
            log.info("Skipping %s — implausible delay %.1fh", flight_num, delay)
            continue

        t_diff = int((best_dt - now_aest).total_seconds() / 60)
        is_can = status_raw in ("canceled", "cancelled")
        is_div = ("divert" in status_raw or "redirect" in status_raw
                  or _mismatch_diverted)

        # FIX 1 — only trust t_diff <= 0 for "landed" when we have a confirmed
        # actual time; "revised" (incl. OpenSky estimates) expiring past zero
        # does NOT mean the plane has landed — it means the estimate was wrong.
        is_lan = (status_raw in ("landed", "arrived")) or (t_diff <= 0 and t_type == "actual")

        # FIX 6 — time-based landed fallback.
        # Covers two cases:
        # a) Scheduled-only flights (no radar data) past the API lag window →
        #    prevents "NO UPDATE" stuck cards (e.g. JQ100).
        # b) Revised (radar) flights whose ETA has expired past the lag window
        #    but AeroDataBox hasn't confirmed landing yet → prevents "In 00m"
        #    stuck cards (e.g. KE407 showing Est 07:06 at 07:22).
        # Split by data quality (V12.44 fix for the stuck-"On Ground" bug):
        # • "revised" (radar Est exists) → the flight is genuinely being tracked
        #   and flew. AeroDataBox frequently NEVER fills departure actualTime nor
        #   flips status to airborne, so requiring has_departed left genuinely
        #   landed flights stuck "On Ground" forever (EK434/KE407/QF52 on V11.98).
        #   Fire on elapsed time alone.
        # • "scheduled" (no radar) → could be delayed at origin (NZ 205 case:
        #   Sch 08:05 but actually departing 09:00) → require departure
        #   confirmation before assuming it landed.
        if (not is_lan
                and not disruption_mode
                and t_diff < -API_LAG_MINS
                and status_raw not in AIRBORNE_STATUSES
                and (t_type == "revised"
                     or (t_type == "scheduled" and has_departed))):
            is_lan = True

        is_lan = is_lan and not is_can and not is_div

        landed_mins = max(0, -t_diff)

        style = classify_flight_status(
            is_canceled=is_can, is_diverted=is_div, is_landed=is_lan, landed_mins=landed_mins,
            t_diff=t_diff, t_type=t_type, delay_hours=delay, s_dt=s_dt, now=now_aest, t=t,
        )

        city = CITY_MAP.get(
            dep_ap.get("municipalityName") or dep_ap.get("name"),
            dep_ap.get("municipalityName") or dep_ap.get("name") or "Unknown",
        )

        processed.append({
            "num":          flight_num,
            "prev_gate":    None,  # populated below if a gate change is detected
            "origin":       city,
            "iata":         origin_iata,
            "gate":         arr.get("gate") or "TBA",
            "ac_text":      f"{ac_m} ({ac_r})" if ac_m and ac_r else ac_m or ac_r,
            "reg":          ac_r,
            "s_dt_iso":     s_dt.isoformat() if s_dt is not None else None,
            "actual_time":  best_dt.strftime("%H:%M"),
            "sch_time":     s_dt.strftime("%H:%M"),
            "is_landed":    is_lan,
            "is_canceled":  is_can,
            "is_diverted":  is_div,
            "dt":           best_dt,
            "s_dt_val":     s_dt,
            "time_type":    t_type,
            "logo_url":     get_airline_logo_url(flight_num),
            "photo_url":    get_photo_from_api(ac_r),
            "border_color": style.border_color,
            "status_color": style.status_color,
            "status_text":  style.status_text,
            "bg_color":     style.bg_color,
            "card_opacity": style.card_opacity,
            "img_filter":   style.img_filter,
            "landed_mins":  landed_mins,
        })

    # ── Gate Change Detection ─────────────────────────────────────────────────
    # Compare each flight's current gate against what we last saw. If it changed
    # (and both old/new are real gates, not TBA), flag it so the card can show
    # "was XX". History is module-level: shared by all users, survives page
    # refreshes, and badges expire after GATE_BADGE_TTL_SEC.
    # Wrapped in try/except so a detection issue can never blank the whole board.
    try:
        _now_ts = datetime.now().timestamp()
        with _gate_lock:
            for p in processed:
                if p.get("is_gap") or p.get("is_surge"):
                    continue
                fnum = p.get("num")
                cur  = p.get("gate")
                if not fnum or cur in (None, "TBA"):
                    continue
                prev = _gate_state.get(fnum)
                if prev and prev != "TBA" and prev != cur:
                    _gate_changed[fnum] = (prev, _now_ts)
                _gate_state[fnum] = cur
                chg = _gate_changed.get(fnum)
                if chg and (_now_ts - chg[1]) < GATE_BADGE_TTL_SEC:
                    p["prev_gate"] = chg[0]
                elif chg:
                    del _gate_changed[fnum]    # expired — clean up
    except Exception as e:
        log.warning("Gate change detection failed: %s", e)

    # ── Departure-time prefetch (budgeted, background, non-blocking) ─────────
    # Only radar-tracked inbound flights: they have genuinely departed, the data
    # exists, and they're the ones staff are actively tracking. Sch-only flights
    # haven't left (nothing to fetch); landed flights no longer need it.
    if DEP_INFO_ENABLED:
        for p in processed:
            if (not p.get("is_gap") and not p.get("is_surge")
                    and p.get("time_type") == "revised"
                    and not p.get("is_landed")
                    and not p.get("is_canceled") and not p.get("is_diverted")):
                get_dep_time(p.get("num", ""), p.get("s_dt_iso") or "")

    # ── Gap Detection ─────────────────────────────────────────────────────────
    gap_candidates = sorted(
        [p for p in processed
         if not p["is_canceled"] and not p["is_diverted"]
         and not (p["is_landed"] and p["landed_mins"] > RECENT_LANDED_MAX)],
        key=lambda x: x["dt"],
    )

    # Virtual "now" anchor: if the first candidate is in the future, insert a
    # synthetic entry at now_aest so the gap between RIGHT NOW and the next
    # flight is displayed — otherwise there's nothing to anchor against when
    # there are no recently-landed flights.
    if gap_candidates and gap_candidates[0]["dt"] > now_aest:
        gap_candidates.insert(0, {"dt": now_aest, "is_virtual": True})

    gap_list = []
    for i in range(len(gap_candidates) - 1):
        t1 = gap_candidates[i]["dt"]
        t2 = gap_candidates[i + 1]["dt"]
        is_virtual = gap_candidates[i].get("is_virtual", False)
        # No lag buffer applied — using the next flight's best-known time as-is.
        # For Sch-only flights we flag the uncertainty in the UI (tilde prefix)
        # rather than pretending to know a precise gap end via arbitrary subtraction.
        next_flight    = gap_candidates[i + 1]
        next_is_sch    = next_flight.get("time_type", "scheduled") == "scheduled"
        t2_safe        = t2

        gap_total = int((t2_safe - t1).total_seconds() / 60)
        # Virtual anchor gets a relaxed minimum — we always want to show how
        # long until the next flight, even if it's only 10 minutes away.
        if not is_virtual and gap_total < GAP_MIN_MINUTES:
            continue

        gap_remaining = int((t2_safe - max(t1, now_aest)).total_seconds() / 60)
        if gap_remaining < GAP_DISPLAY_MIN:
            continue

        is_active = t1 <= now_aest < t2_safe

        # FIX 2 — append to gap_list BEFORE the virtual continue so the
        # summary strip "Next Gap" field can see this gap entry
        gap_list.append({"t1": t1, "t2": t2_safe, "total": gap_total,
                         "remaining": gap_remaining, "active": is_active})

        cls = "gap-bar gap-active" if is_active else "gap-bar"
        lbl = L("active") if is_active else "🔄"

        end_str = (f"{t2_safe.strftime('%H:%M')}, {L('approx')}" if next_is_sch
                   else t2_safe.strftime("%H:%M"))

        if is_virtual:
            # Pre-shift bar: nothing has landed recently, just counting down
            # to the next arrival. Add the same progress bar as regular gaps
            # so users can see visually how much of the window remains.
            virtual_progress_html = ""
            if gap_total > 0:
                pct_left = max(0, min(100, int(gap_remaining / gap_total * 100)))
                bar_color = t.c_green if pct_left > 50 else (t.c_amber if pct_left > 25 else t.c_red)
                virtual_progress_html = (
                    f'<div class="gap-progress-track">'
                    f'<div class="gap-progress-fill" style="width:{pct_left}%; background:{bar_color};"></div>'
                    f'</div>'
                )
            processed.append({
                "is_gap":   True,
                "time_key": t1.timestamp() + 1,
                "html": (
                    f'<div class="{cls}">{lbl} {L("before_next", x=format_hm(gap_remaining))} '
                    f'<span style="opacity:0.6; font-weight:400; margin-left:8px;">'
                    f'({L("ends", x=end_str)})</span>'
                    f'{virtual_progress_html}</div>'
                ),
            })
            continue

        window_start = max(t1, now_aest) if is_active else t1
        display_min  = gap_remaining if is_active else gap_total

        progress_html = ""
        if is_active and gap_total > 0:
            pct_left = max(0, min(100, int(gap_remaining / gap_total * 100)))
            bar_color = t.c_green if pct_left > 50 else (t.c_amber if pct_left > 25 else t.c_red)
            progress_html = (
                f'<div class="gap-progress-track">'
                f'<div class="gap-progress-fill" style="width:{pct_left}%; background:{bar_color};"></div>'
                f'</div>'
            )

        processed.append({
            "is_gap":   True,
            "time_key": t1.timestamp() + 1,
            "html": (
                f'<div class="{cls}">{lbl} {L("gap_fmt", x=format_hm(display_min))} '
                f'<span style="opacity:0.6; font-weight:400; margin-left:8px;">'
                f'({window_start.strftime("%H:%M")}–{end_str})</span>'
                f'{progress_html}</div>'
            ),
        })

    # ── Surge Detection (chain-based) ─────────────────────────────────────────
    future_flights = sorted(
        [p for p in processed if not p.get("is_gap")
         and not p["is_canceled"] and not p["is_diverted"] and not p["is_landed"]],
        key=lambda x: x["dt"],
    )

    surge_used = set()
    for i, anchor_f in enumerate(future_flights):
        if i in surge_used:
            continue
        cluster, cluster_idx = [anchor_f], [i]
        for j in range(i + 1, len(future_flights)):
            if j in surge_used:
                continue
            if (future_flights[j]["dt"] - cluster[-1]["dt"]).total_seconds() / 60 <= SURGE_WINDOW_MINS:
                cluster.append(future_flights[j])
                cluster_idx.append(j)
            else:
                break
        # Trigger on either raw flight count OR pax-weight: 3+ flights of any
        # size is operationally busy, and 2 widebodies (weight 6) also qualify
        # even though it's only 2 flights.
        def _pax_weight(_p):
            # Freighters bring zero pax — exclude them from surge weight when we
            # know (aircraft-info cache). String-based weight otherwise.
            _r = _p.get("reg", "")
            if _r:
                with _ac_info_lock:
                    _info = _ac_info_cache.get(_r)
                if isinstance(_info, dict) and _info.get("freighter"):
                    return 0
            return get_aircraft_pax_weight(_p.get("ac_text", ""))

        cluster_weight = sum(_pax_weight(f) for f in cluster)
        w_start = cluster[0]["dt"]
        w_end   = cluster[-1]["dt"]
        # A surge window entirely in the past is stale noise — showing expired
        # "all hands" alerts trains people to ignore the banner.
        if w_end < now_aest:
            surge_used.update(cluster_idx)
            continue
        if len(cluster) >= SURGE_MIN_FLIGHTS or cluster_weight >= SURGE_MIN_WEIGHT:
            surge_used.update(cluster_idx)
            processed.append({
                "is_surge": True,
                "time_key": w_start.timestamp() - 1,
                "html": (
                    f'<div class="surge-banner"><span class="surge-icon">⚡</span>'
                    f'{L("surge_fmt", a=w_start.strftime("%H:%M"), b=w_end.strftime("%H:%M"), n=len(cluster))}</div>'
                ),
            })

    # ── Summary Strip ─────────────────────────────────────────────────────────
    incoming = [p for p in processed
                if not p.get("is_gap") and not p.get("is_surge")
                and not p["is_canceled"] and not p["is_diverted"] and not p["is_landed"]]

    next_gap_txt = "None"
    for g in sorted(gap_list, key=lambda x: x["t1"]):
        if g["t2"] > now_aest:
            if g["active"]:
                next_gap_txt = f'<span style="color:{t.c_green};">{L("now_fmt", m=g["remaining"])}</span>'
            else:
                next_gap_txt = f'{g["t1"].strftime("%H:%M")} ({g["total"]}m)'
            break

    busiest_txt = "—"
    if len(incoming) >= 2:
        sorted_inc = sorted(incoming, key=lambda x: x["dt"])
        best_count, best_start, best_end = 0, None, None
        for f_item in sorted_inc:
            window_end = f_item["dt"] + timedelta(minutes=30)
            count = sum(1 for o in sorted_inc if f_item["dt"] <= o["dt"] < window_end)
            if count > best_count:
                best_count = count
                best_start = f_item["dt"]
                best_end   = max((o["dt"] for o in sorted_inc
                                  if f_item["dt"] <= o["dt"] < window_end), default=f_item["dt"])
        if best_count >= 2 and best_start:
            busiest_txt = f'{best_start.strftime("%H:%M")}–{best_end.strftime("%H:%M")} ({best_count})'

    # Stale data warning — if the last successful API fetch is more than 2x the
    # normal cache TTL old, something is broken (API errors, network issues, etc).
    # Silently outdated data is dangerous because users may act on stale info
    # without realising. Show a prominent red banner.
    if api_t:
        age_minutes = int((now_aest - api_t).total_seconds() / 60)
        stale_threshold_min = int(API_DATA_TTL_SEC / 60 * 2)
        if age_minutes > stale_threshold_min:
            st.markdown(f"""
            <div style="background:{t.c_red}; color:white; padding:10px 14px;
                        border-radius:8px; margin-bottom:10px; font-weight:700;
                        font-size:0.85em; display:flex; align-items:center; gap:8px;">
                <span style="font-size:1.2em;">⚠️</span>
                <div>
                    <div>{L("stale_title", n=age_minutes)}</div>
                    <div style="font-weight:400; font-size:0.85em; opacity:0.9; margin-top:2px;">
                        {L("stale_body")}
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    if disruption_mode:
        st.markdown(f"""
        <div style="background:{t.surge_bg_start}; border-left:5px solid {t.c_amber};
                    color:{t.surge_text}; padding:10px 14px; border-radius:8px;
                    margin-bottom:10px; font-weight:700; font-size:0.85em;">
            {L("disruption")}
        </div>
        """, unsafe_allow_html=True)

    # ── Weather strip (BNE current conditions) ────────────────────────────────
    try:
        _om = fetch_weather(anchor)          # Open-Meteo: current + 3h forecast
    except Exception:
        _om = None
    try:
        _metar = fetch_metar(anchor)         # NOAA METAR: real airport observation
    except Exception:
        _metar = None
    # Prefer METAR for CURRENT conditions (real observation, has visibility);
    # borrow the hourly forecast arrays from Open-Meteo when available.
    if _metar:
        _wx = dict(_metar)
        if _om:
            _wx["h_codes"] = _om.get("h_codes", [])
            _wx["h_times"] = _om.get("h_times", [])
    else:
        _wx = _om
    if _wx and _wx.get("temp") is not None:
        _wx_emoji, _wx_key = _wmo_condition(_wx.get("code"))
        _wx_is_fog  = _wx.get("code") in (45, 48)
        _cond_col   = t.c_amber if _wx_is_fog else t.text_main
        _cond_html  = (f'<span style="color:{_cond_col}; font-weight:700;">'
                       f'{_wx_emoji} {L(_wx_key) if _wx_key else ""}</span>')
        _temp_txt   = f'{round(_wx["temp"])}&nbsp;°C'
        _wd, _ws    = _wx.get("wind_dir"), _wx.get("wind_kmh")
        if _wd is not None and _ws is not None:
            _arrow    = (f'<span style="display:inline-block; margin-right:3px; '
                         f'transform:rotate({(int(_wd) + 180) % 360}deg);">↑</span>')
            _wind_html = f'{_arrow}{int(_wd)}° · {round(_ws)}&nbsp;km/h'
        else:
            _wind_html = "—"
        # Visibility (METAR only) — the single most useful fog-day number.
        _vis_html = ""
        _vis = _wx.get("vis_km")
        if _vis is not None:
            _vis_col = t.c_amber if _vis < 5 else t.text_main   # <5km = reduced
            _vis_html = (f'<span style="opacity:0.45; margin:0 8px;">|</span>'
                         f'<span style="color:{_vis_col};">{L("wx_vis")} {_vis}&nbsp;km</span>')

        # Always-on 3-hour outlook row (amber = worsening hour)
        _fc_html = _wx_forecast_3h(_wx, now_aest, t)
        _fc_row = ""
        if _fc_html:
            _fc_row = (
                f'<div style="margin-top:4px; font-size:0.72em; opacity:0.9;">'
                f'<span style="opacity:0.6;">{L("wx_next3h")}</span> &nbsp;{_fc_html}</div>'
            )
        st.markdown(f"""
        <div style="text-align:center; font-size:0.78em; color:{t.text_muted};
                    background:{t.bg_card}; border:1px solid {t.border_muted};
                    border-radius:8px; padding:6px 12px; margin-bottom:8px;">
            {_cond_html}
            <span style="opacity:0.45; margin:0 8px;">|</span>
            <span style="color:{t.text_main}; font-weight:700;">{_temp_txt}</span>
            <span style="opacity:0.45; margin:0 8px;">|</span>
            <span style="color:{t.text_main};">{_wind_html}</span>
            {_vis_html}
            {_fc_row}
        </div>
        """, unsafe_allow_html=True)
    else:
        # Cold-start / fetch miss — show a quiet placeholder instead of vanishing,
        # so the strip's absence is never mistaken for "no weather data ever".
        st.markdown(f"""
        <div style="text-align:center; font-size:0.75em; color:{t.text_muted};
                    background:{t.bg_card}; border:1px solid {t.border_muted};
                    border-radius:8px; padding:6px 12px; margin-bottom:8px; opacity:0.7;">
            {L("wx_loading")}
        </div>
        """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="summary-strip">
        <div class="s-item"><span class="s-val" style="color:{t.c_blue};">{len(incoming)}</span>{L("incoming")}</div>
        <div class="s-item"><span class="s-val" style="color:{t.c_green};">{next_gap_txt}</span>{L("next_gap")}</div>
        <div class="s-item"><span class="s-val" style="color:{t.c_amber};">{busiest_txt}</span>{L("busiest")}</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Sort ──────────────────────────────────────────────────────────────────
    processed.sort(key=lambda p:
        (1, p["time_key"])              if p.get("is_gap") or p.get("is_surge")                     else
        (2, p["s_dt_val"].timestamp())  if p["is_canceled"] or p["is_diverted"]                     else
        (0, -p["dt"].timestamp())       if p["is_landed"] and p["landed_mins"] <= RECENT_LANDED_MAX else
        (2, -p["dt"].timestamp())       if p["is_landed"]                                           else
        (1, p["dt"].timestamp())
    )

    # ── Render Active Cards ───────────────────────────────────────────────────
    landed_divider_shown = False
    for i, pf in enumerate(processed):
        if pf.get("is_canceled") or pf.get("is_diverted"):
            continue
        if pf.get("is_gap") or pf.get("is_surge"):
            st.markdown(pf["html"], unsafe_allow_html=True)
            continue

        # Insert a visual break the first time we hit a non-recent landed card
        # so there is clear breathing room between incoming and past arrivals.
        if pf["is_landed"] and pf["landed_mins"] > RECENT_LANDED_MAX and not landed_divider_shown:
            st.markdown(
                f"<div style='margin:24px 0 8px 0; display:flex; align-items:center; gap:10px;'>"
                f"<div style='flex:1; height:1px; background:{t.border_muted};'></div>"
                f"<span style='font-size:0.72em; color:{t.text_muted}; font-weight:700; "
                f"white-space:nowrap; letter-spacing:1px; text-transform:uppercase;'>"
                f"{L("earlier")}</span>"
                f"<div style='flex:1; height:1px; background:{t.border_muted};'></div>"
                f"</div>",
                unsafe_allow_html=True,
            )
            landed_divider_shown = True

        mid       = f"z_{i}"
        # ── Registration cross-validation ────────────────────────────────────
        # FIDS regs frequently carry the previous rotation's airframe until
        # today's is assigned (verified: QR898 showing prior-day A7-BEG while
        # FR24 showed today's reg still unassigned). The per-flight leg lookup
        # (already fetched for departure times — zero extra cost) is
        # authoritative for TODAY: prefer its reg; if the leg exists but has no
        # airframe yet, hide the FIDS reg rather than show the wrong aircraft.
        display_reg   = pf.get("reg") or ""
        _ac_text_show = pf.get("ac_text") or ""
        photo_url     = pf["photo_url"]
        _leg = get_flight_leg_info(pf.get("num", ""), pf.get("s_dt_iso") or "")
        if _leg and _leg.get("found"):
            if _leg.get("reg"):
                if display_reg and _leg["reg"] != display_reg:
                    _ac_text_show = _ac_text_show.replace(display_reg, _leg["reg"])
                    display_reg   = _leg["reg"]
                elif not display_reg:
                    display_reg   = _leg["reg"]
                    _ac_text_show = (f"{_ac_text_show} ({display_reg})"
                                     if _ac_text_show else display_reg)
            else:
                if display_reg:
                    _ac_text_show = _ac_text_show.replace(f" ({display_reg})", "")
                display_reg = ""
        if display_reg != (pf.get("reg") or ""):
            photo_url = get_photo_from_api(display_reg) if display_reg else "NOT_FOUND"

        has_photo = photo_url != "NOT_FOUND"
        al_code   = "".join(c for c in pf["num"] if c.isalpha())[:2].upper()

        img_html = (
            f'<div class="flip-container" style="filter:{pf["img_filter"]};">'
            f'<div class="img-fallback" style="border-color:{pf["border_color"]};">{al_code}</div>'
            f'<label for="{mid}" style="cursor:pointer; display:block; width:100%; height:100%;">'
            f'<img src="{pf["logo_url"]}" class="flip-img logo-layer" style="border-color:{pf["border_color"]};"/>'
            f'<img src="{pf["photo_url"]}" class="flip-img photo-layer" style="border-color:{pf["border_color"]};"/>'
            f'</label></div>'
            if has_photo else
            f'<div class="flip-container" style="filter:{pf["img_filter"]};">'
            f'<div class="img-fallback" style="border-color:{pf["border_color"]};">{al_code}</div>'
            f'<img src="{pf["logo_url"]}" class="flip-img" style="border-color:{pf["border_color"]}; background:#FFF; padding:4px; object-fit:contain; border-radius:8px;"/>'
            f'</div>'
        )

        # Only show "Act" tag when we have a confirmed actual time. A flight
        # marked landed via FIX 6's time-based fallback has only scheduled or
        # estimated time — so show the original tag, not a fake "Act".
        tag        = "Act" if pf["time_type"] == "actual" else ("Est" if pf["time_type"] == "revised" else "Sch")
        time_color = t.c_blue if tag == "Act" else (t.text_faded if tag == "Est" else t.text_muted)

        # When status is Sch-only with no radar, suppress the misleading
        # "In Xh Ym" countdown — we don't actually know when it'll arrive,
        # so showing a precise countdown gives false confidence.
        suppress_countdown = (tag == "Sch" and not pf["is_landed"]
                              and not pf["is_canceled"] and not pf["is_diverted"])

        # Disruption mode: a flight past its ETA without confirmed landing may be
        # holding or diverting — "On Ground" would be a guess. Show Check Board.
        if (disruption_mode and not pf["is_landed"] and not pf["is_canceled"]
                and not pf["is_diverted"] and pf["dt"] <= now_aest):
            suppress_countdown = True

        if tag == "Sch":
            time_display = (
                f'<span class="mono" style="color:{t.text_muted};">Sch {pf["sch_time"]}</span>'
            )
        else:
            time_display = (
                f'<span class="mono" style="color:{t.text_muted}; font-size:0.85em;">Sch {pf["sch_time"]}</span>'
                f' • <span class="mono" style="color:{time_color}; font-weight:700; font-size:1.05em;">{tag} {pf["actual_time"]}</span>'
            )

        zoom_src = photo_url if has_photo else pf["logo_url"]
        gate_cls = "gate-tba" if pf["gate"] == "TBA" else "gate-num"

        # Aircraft extras (age / seats / freighter) from the background Tier-1 cache.
        bits = []
        _ai = get_aircraft_info(display_reg)
        if _ai:
            if _ai.get("age"):
                _age_val = _ai["age"]
                if _age_val < 1:
                    _months = max(1, int(round(_age_val * 12)))
                    bits.append(L("age_months", n=_months))
                else:
                    bits.append(L("age_years", n=int(round(_age_val))))
            if _ai.get("seats"):
                bits.append(L("seats", n=_ai["seats"]))
            if _ai.get("freighter"):
                bits.append(L("freighter"))
        # Card line intentionally not rendered — aircraft details show ONLY in
        # the photo zoom modal (user preference: keep cards compact).

        # Caption for the photo zoom modal: aircraft type/reg + the same extras
        zoom_caption_bits = [_ac_text_show] if _ac_text_show else []
        if _ai:
            zoom_caption_bits += bits if _ai else []
        zoom_caption = " · ".join(b for b in zoom_caption_bits if b)
        if display_reg:
            _ps_url = f"https://www.planespotters.net/search?q={display_reg}"
            zoom_caption += (
                f' <a href="{_ps_url}" target="_blank" rel="noopener" '
                f'style="color:{t.c_blue}; text-decoration:none; margin-left:6px; '
                f'font-weight:700;">📷 {L("more_photos")} ↗</a>'
            )

        # Gate-change badge — small amber "was XX" tag if the gate changed recently
        gate_change_badge = ""
        if pf.get("prev_gate"):
            gate_change_badge = (
                f'<span style="display:block; font-size:0.32em; font-weight:700; '
                f'color:{t.c_amber}; letter-spacing:0.5px; margin-top:1px;">'
                f'{L("was_gate", x=pf["prev_gate"])}</span>'
            )

        # Replace the misleading "In Xh Ym" countdown with "Check Board" when
        # we don't have radar data — keep the gate visible but don't fake an ETA.
        if suppress_countdown:
            status_col_text  = L("check_board")
            status_col_color = t.c_amber
        else:
            status_col_text  = pf["status_text"]
            status_col_color = pf["status_color"]

        # FR24 link — live-map callsign URL ONLY for flights still in the air
        # (radar Est in the future). Landed and "On Ground" flights are no longer
        # broadcasting that callsign, so the live URL fails or resolves to a
        # completely different aircraft — send those to the /data/flights/ page.
        _is_airborne = (pf["time_type"] == "revised"
                        and not pf["is_landed"]
                        and pf["dt"] > now_aest)
        if _is_airborne:
            fr24_url = f"https://www.flightradar24.com/{_iata_to_callsign(pf['num'])}"
        else:
            fr24_flight_id = pf['num'].replace(" ", "").lower()
            fr24_url       = f"https://www.flightradar24.com/data/flights/{fr24_flight_id}"
        flight_num_html = (
            f'<a href="{fr24_url}" target="_blank" rel="noopener" '
            f'style="color:inherit; text-decoration:none; border-bottom:1px dotted {t.text_muted};">'
            f'{pf["num"]}</a>'
        )

        st.markdown(f"""
        <div class="flight-card" style="border-left-color:{pf['border_color']}; background-color:{pf['bg_color']}; opacity:{pf['card_opacity']};">
            {img_html}
            <div class="info-col">
                <div style="font-size:1.1em; font-weight:700;">{flight_num_html}<span style="font-size:0.7em; color:{t.text_muted}; margin-left:8px;">{pf['origin']} [{pf['iata']}]</span></div>
                <div class="ac-line">{_ac_text_show}</div>
                <div style="font-size:0.8em; color:{t.text_muted};">{time_display}</div>
            </div>
            <div class="status-col">
                <div style="font-size:0.6em; color:{t.text_muted}; font-weight:700; letter-spacing:1px;">{L("gate")}</div>
                <div class="mono {gate_cls}">{pf['gate']}{gate_change_badge}</div>
                <div style="font-size:0.85em; font-weight:700; color:{status_col_color}; margin-top:2px;">{status_col_text}</div>
            </div>
        </div>
        <input type="checkbox" id="{mid}" class="img-zoom-chk" style="display:none;">
        <div class="img-zoom-modal">
            <label for="{mid}" class="img-zoom-close-bg"></label>
            <label for="{mid}" class="close-btn">&times;</label>
            <img src="{zoom_src}"/>
            <div class="zoom-caption">{zoom_caption}</div>
        </div>
        """, unsafe_allow_html=True)

    # ── Render Diverted ───────────────────────────────────────────────────────
    divs = sorted([p for p in processed if p.get("is_diverted")], key=lambda x: x["s_dt_val"])
    if divs:
        st.markdown(
            f"<hr style='margin:15px 0 8px 0; opacity:0.2;'>"
            f"<div style='color:{t.c_purple}; font-size:0.85em; font-weight:700; margin-bottom:5px;'>{L("diverted_hdr")}</div>",
            unsafe_allow_html=True,
        )
        for pf in divs:
            al_code = "".join(c for c in pf["num"] if c.isalpha())[:2].upper()
            st.markdown(f"""
            <div class="flight-card" style="border-left-color:{pf['border_color']}; background-color:{pf['bg_color']}; opacity:{pf['card_opacity']};">
                <div class="flip-container" style="filter:{pf['img_filter']};">
                    <div class="img-fallback" style="border-color:{pf['border_color']};">{al_code}</div>
                    <img src="{pf['logo_url']}" class="flip-img" style="border-color:{pf['border_color']}; background:#FFF; padding:4px; object-fit:contain; border-radius:8px;"/>
                </div>
                <div class="info-col">
                    <div style="font-size:1em; font-weight:700;">{pf['num']} <span style="font-size:0.75em; color:{t.text_muted};">{pf['origin']} [{pf['iata']}]</span></div>
                    <div style="font-size:0.75em; color:{t.text_muted};"><span class="mono">Sch {pf['sch_time']}</span></div>
                </div>
                <div class="status-col">
                    <div style="font-size:0.8em; font-weight:700; color:{pf['status_color']};">{pf['status_text']}</div>
                </div>
            </div>""", unsafe_allow_html=True)

    # ── Render Canceled ───────────────────────────────────────────────────────
    cans = sorted([p for p in processed if p.get("is_canceled")], key=lambda x: x["s_dt_val"])
    if cans:
        st.markdown(
            f"<hr style='margin:15px 0 8px 0; opacity:0.2;'>"
            f"<div style='color:{t.c_red}; font-size:0.85em; font-weight:700; margin-bottom:5px;'>{L("canceled_hdr")}</div>",
            unsafe_allow_html=True,
        )
        for pf in cans:
            al_code = "".join(c for c in pf["num"] if c.isalpha())[:2].upper()
            st.markdown(f"""
            <div class="flight-card" style="border-left-color:{pf['border_color']}; background-color:{pf['bg_color']}; opacity:{pf['card_opacity']};">
                <div class="flip-container" style="filter:{pf['img_filter']};">
                    <div class="img-fallback" style="border-color:{pf['border_color']};">{al_code}</div>
                    <img src="{pf['logo_url']}" class="flip-img" style="border-color:{pf['border_color']}; background:#FFF; padding:4px; object-fit:contain; border-radius:8px;"/>
                </div>
                <div class="info-col">
                    <div style="font-size:1em; font-weight:700;">{pf['num']} <span style="font-size:0.75em; color:{t.text_muted};">{pf['origin']} [{pf['iata']}]</span></div>
                    <div style="font-size:0.75em; color:{t.text_muted};"><span class="mono">Sch {pf['sch_time']}</span></div>
                </div>
                <div class="status-col">
                    <div style="font-size:0.8em; font-weight:700; color:{pf['status_color']};">{pf['status_text']}</div>
                </div>
            </div>""", unsafe_allow_html=True)

    st.markdown(
        f"<div style='text-align:center; color:{t.text_muted}; font-size:0.65em; margin-top:20px;'>Dev: Phillip Yeh | V12.44</div>",
        unsafe_allow_html=True,
    )


@st.fragment(run_every=f"{UI_REFRESH_SEC}s")
def live_dashboard():
    try:
        _live_dashboard_impl()
    except Exception as e:
        # Surface the real error instead of leaving a blank board, and log the
        # full traceback so we can diagnose. The board will retry on next refresh.
        import traceback
        log.error("Dashboard render failed: %s\n%s", e, traceback.format_exc())
        st.error(f"⚠️ Something went wrong rendering the board: {e}")
        st.caption("This will retry automatically on the next refresh. If it persists, screenshot this and send to Phillip.")


live_dashboard()

# ── Live clock & refresh countdown ──
# components.html is deprecated (removal after 2026-06-01) but st.html does not
# execute embedded <script> tags reliably in our Streamlit version. Stay with
# components.html until either Streamlit fixes st.html or we migrate to a
# different JS injection strategy.
components.html("""
<script>
    const doc = window.parent.document;
    const aestFmt = new Intl.DateTimeFormat('en-AU', {
        timeZone: 'Australia/Brisbane',
        hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
    });
    setInterval(function() {
        const clockEl = doc.getElementById('bne-live-clock');
        if (clockEl) clockEl.innerText = aestFmt.format(new Date());

        const cdEl = doc.getElementById('bne-refresh-countdown');
        if (cdEl) {
            const nextTs = parseInt(cdEl.getAttribute('data-next'), 10);
            const secsLeft = Math.max(0, nextTs - Math.floor(Date.now() / 1000));
            if (secsLeft === 0) {
                cdEl.innerText = 'Refreshing...';
            } else {
                const m = Math.floor(secsLeft / 60);
                const s = secsLeft % 60;
                cdEl.innerText = m > 0 ? m + 'm ' + String(s).padStart(2,'0') + 's' : s + 's';
            }
        }
    }, 1000);
</script>
""", height=0)
