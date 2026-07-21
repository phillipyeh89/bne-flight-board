import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import requests
import logging
import math
import threading
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
        "age_years":     "{n} years",
        "age_months":    "{n} months",
        "freighter":     "📦 Freighter",
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
        "age_years":     "機齡 {n} 年",
        "age_months":    "機齡 {n} 個月",
        "freighter":     "📦 貨機",
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
        "age_years":     "기령 {n}년",
        "age_months":    "기령 {n}개월",
        "freighter":     "📦 화물기",
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
        "age_years":     "機齢{n}年",
        "age_months":    "機齢{n}ヶ月",
        "freighter":     "📦 貨物機",
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
API_DATA_TTL_SEC         = 960  # 16 min cache — Tier 2 endpoint: 90×2×30=5,400 units/month vs 6,000 limit
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
_ac_info_lock            = threading.Lock()
# AeroDataBox enforces a per-second rate limit on the Pro plan — space aircraft
# lookups to ~1 req/sec so they never trip it (nor collide with the FIDS call).
_adb_throttle_lock       = threading.Lock()
_adb_last_request        = [0.0]
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
        .info-col .ac-extra-line {{ font-size: 0.75em; color: {t.text_main}; font-weight: 600; margin: 1px 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .status-col {{ text-align: right; min-width: 110px; max-width: 45%; display: flex; flex-direction: column; justify-content: center; flex-shrink: 0; }}
        .gate-num   {{ font-size: 1.85em; font-weight: 700; line-height: 1; }}
        .gate-tba   {{ font-size: 1.85em; font-weight: 700; line-height: 1; opacity: 0.35; }}

        .summary-
