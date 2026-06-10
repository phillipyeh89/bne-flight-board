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
IMMINENT_MINS            = 40   # red "hot" threshold — flight arriving within 40 min
API_LAG_MINS             = 10   # AeroDataBox lag observed in practice — typical 5-15 min range
EST_COMPENSATION_MINS    = 10   # AeroDataBox Est runs ~10 min later than actual touchdown (observed);
                                # subtract this from live radar estimates to better predict real arrival
OPENSKY_PREFER_UNDER_MIN = 60   # use OpenSky over AeroDataBox for flights < 60 min out
IMAGE_WORKERS            = 3    # Planespotters free API rate-limits aggressively (429s) — keep concurrency low
PHOTO_FAIL_TTL_SEC       = 180  # retry failed photo lookups after 3 min (was 10 — too long for transient failures)
SURGE_WINDOW_MINS        = 15   # cluster detection window
SURGE_MIN_FLIGHTS        = 3    # minimum flights in cluster to consider
SURGE_MIN_WEIGHT         = 4    # min total pax-weight to trigger (2 widebodies = 6 triggers; 3 narrowbodies = 3 doesn't... see logic)
DOMESTIC_TERMINALS       = ('D', 'DOM', 'D-ANC', 'GAT')
SMALL_AIRCRAFT_FILTER    = ('BEECH', 'FAIRCHILD', 'CESSNA', 'PIPER', 'PILATUS', 'KING AIR', 'METROLINER', 'SAAB')

# Add flight numbers here that appear in AeroDataBox but never actually operate to BNE
GHOST_FLIGHTS = set()

AIRBORNE_STATUSES = {"enroute", "departed", "approaching"}

CITY_MAP = {
    "Lapu-Lapu City": "Cebu", "Denpasar-Bali Island": "Bali",
    "Ho Chi Minh City": "Saigon", "Yaren District": "Nauru",
    "Guangzhou Baiyun": "Guangzhou",
}

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
        .info-col .ac-line {{ font-size: 0.7em; color: {t.text_faded}; margin: 1px 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
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
            background: {t.modal_bg}; z-index: 10000; align-items: center; justify-content: center; backdrop-filter: blur(10px);
        }}
        .img-zoom-chk:checked + .img-zoom-modal {{ display: flex !important; }}
        .img-zoom-modal img {{ max-width: 90%; max-height: 80%; border-radius: 12px; border: 2px solid {t.border_muted}; object-fit: contain; z-index: 10001; }}
        .img-zoom-close-bg {{ position: absolute; top: 0; left: 0; width: 100%; height: 100%; cursor: pointer; z-index: 10000; }}
        .close-btn {{ position: absolute; top: 20px; right: 30px; color: {t.text_main}; font-size: 3.5em; font-weight: bold; cursor: pointer; z-index: 10002; line-height: 1; }}
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
            return FlightStyle(t.border_muted, t.text_muted, t.bg_main, "CANCELED", "0.5", "grayscale(100%)")
        return FlightStyle(t.c_red, t.c_red, t.bg_card, "CANCELED", "0.5", "grayscale(100%)")

    if is_diverted:
        return FlightStyle(t.c_purple, t.c_purple, t.c_purple_bg, "✈️ DIVERTED", "0.8", "none")

    if is_landed:
        landed_label = "Just Landed" if landed_mins == 0 else f"Landed {format_hm(landed_mins)} ago"
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
        return FlightStyle(t.c_amber, t.c_amber, t.bg_card, "NO UPDATE", "1.0", "none")
    if m_left < IMMINENT_MINS:
        label = "On Ground" if m_left == 0 else f"In {format_hm(m_left)}"
        return FlightStyle(t.c_red, t.c_red, t.bg_card, label, "1.0", "none")
    if delay_hours >= SEVERE_DELAY_HOURS:
        return FlightStyle(t.c_severe_border, t.c_red, t.bg_card, f"🔴 +{format_hm(delay_mins)} Late", "1.0", "none")
    if delay_hours >= HEAVY_DELAY_HOURS:
        return FlightStyle(t.c_heavy_border, t.c_amber, t.bg_card, f"🟠 +{format_hm(delay_mins)} Late", "1.0", "none")
    return FlightStyle(t.c_blue, t.c_blue, t.bg_card, f"In {format_hm(m_left)}", "1.0", "none")


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
        except Exception as e:
            last_err = e
            break
    log.error("AeroDataBox API error: %s", last_err)
    st.session_state.api_error = str(last_err)
    return []


def _iata_to_callsign(flight_number: str) -> str:
    prefix = "".join(c for c in flight_number if c.isalpha())[:2].upper()
    digits = "".join(c for c in flight_number if c.isdigit())
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
#  4. UI SETUP & FRAGMENT EXECUTION (V11.97)
# ─────────────────────────────────────────────
st.set_page_config(page_title="BNE Pro Arrivals", page_icon="✈️", layout="centered")
if "api_last_hit" not in st.session_state: st.session_state.api_last_hit = None
if "api_error"    not in st.session_state: st.session_state.api_error    = None
if "theme_light"  not in st.session_state: st.session_state.theme_light  = False
if "font_size"    not in st.session_state: st.session_state.font_size    = 16  # base px — 14 small / 16 normal / 19 large / 22 xl
if "gate_history" not in st.session_state: st.session_state.gate_history = {}  # flight_num -> last seen gate, for change detection


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
    c1, c_ctrl, c3 = st.columns([5, 1.2, 2])
    with c1:
        st.subheader("✈️ Arrivals")
    with c_ctrl:
        with st.popover("⚙️", use_container_width=True):
            st.markdown("**Text Size**")
            cA, cB = st.columns(2)
            with cA:
                if st.button("A−", help="Smaller", use_container_width=True, key="font_smaller"):
                    st.session_state.font_size = max(13, st.session_state.font_size - 3)
                    st.rerun()
            with cB:
                if st.button("A+", help="Larger", use_container_width=True, key="font_larger"):
                    st.session_state.font_size = min(24, st.session_state.font_size + 3)
                    st.rerun()
            st.markdown("**Theme**")
            toggle_icon = "🌙 Dark" if st.session_state.theme_light else "☀️ Light"
            if st.button(toggle_icon, use_container_width=True, key="theme_toggle"):
                st.session_state.theme_light = not st.session_state.theme_light
                st.rerun()
    with c3:
        st.markdown(
            f'<div style="font-size:0.8em;color:{t.text_muted};text-align:right;margin-top:5px;">'
            f'🕒 <span id="bne-live-clock">{now_aest.strftime("%H:%M:%S")}</span></div>',
            unsafe_allow_html=True,
        )
        api_info_placeholder = st.empty()

    with st.expander("ℹ️ Guide"):
        st.markdown(f"""
        **Why use this app?**
        Built to help our team manage shifts — predict peak traffic, coordinate floor tasks, and plan break windows with confidence.

        **How to read the times:**
        * <span class="mono" style="color:{t.c_blue};font-weight:bold;">Act</span>: **Actual** landing time confirmed. Pax are heading to the floor.
        * <span class="mono" style="color:{t.text_faded};font-weight:bold;">Est</span>: **Estimated** arrival from live radar, adjusted ~9 min earlier to match observed real arrivals.
        * <span class="mono" style="color:{t.text_muted};font-weight:bold;">Sch</span>: **Scheduled** only — no radar data yet.

        **Status Indicators:**
        * <span style="color:{t.c_amber};">⚠️ **Check Board**</span>: No live radar — refer to the airport FIDS boards.
        * <span style="color:{t.c_red};">**On Ground**</span>: Flight is past its ETA but landing not yet confirmed by API (usually taxiing).
        * <span style="color:{t.c_green};">**Just Landed**</span> / **Landed Xm ago**: Plane is down — pax heading to the floor.
        * <span style="color:{t.c_amber};">🟠 **Heavy delay**</span> (3h+) / <span style="color:{t.c_red};">🔴 **Severe delay**</span> (12h+).
        * <span style="color:{t.c_red};">⚡ **Surge**</span>: 3+ flights within 15 min, or 2+ widebodies (A350/A380/777/787) close together — all hands on deck.
        * <span style="color:{t.c_purple};">✈️ **Diverted**</span>: Not arriving at BNE.

        **Gap Bars (between flights):**
        * <span style="color:{t.c_green};">🟢 ACTIVE</span>: A break window is happening right now — countdown shows time left.
        * 🔄 Upcoming gap — shows the future break window for planning.
        * `(HH:MM, approx)` means the gap end is based on a Sch-only flight — actual end may shift.

        **Earlier Arrivals:**
        Flights landed within the last 60 min stay near the top in green. Older landings move below the "Earlier Arrivals" divider and fade out.

        **Flight Numbers are Clickable:**
        Tap any flight number to open it in Flightradar24 (opens the FR24 app if installed).

        **Header Info:**
        * **Updated X min ago**: When data was last fetched from AeroDataBox.
        * **(+~15m lag)**: AeroDataBox data is inherently ~15 minutes behind real-time.
        * **Next refresh**: Live countdown to the next data fetch (every 16 min).

        **Quiet Hours (01:00–03:00 AEST):**
        The board sleeps overnight to save API quota — wakes up automatically before 04:00 shift start.

        **Settings (⚙️):**
        Tap the gear icon to adjust text size or switch between light/dark themes.

        **Data Sources:**
        Primary: AeroDataBox (~15 min lag). OpenSky live ADS-B was used as a secondary radar source but is currently disabled (Streamlit hosting can't reach it reliably).

        *Developed by Phillip Yeh to support the BNE Lotte Team.*
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
        st.info(f"🌙 Board is sleeping to save API quota. Wakes up at {QUIET_HOURS_END_H:02d}:00 AEST.")
        return

    raw_flights = fetch_flight_data(anchor, from_time, to_time)
    opensky_data = fetch_opensky_states(anchor)

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
            age_txt = "just now"
        elif age_mins == 1:
            age_txt = "1 min ago"
        else:
            age_txt = f"{age_mins} min ago"

        api_txt = (
            f'<span style="color:{t.text_faded};">Updated {age_txt}</span>'
            f' <span style="color:{t.c_amber}; opacity:0.8;" title="AeroDataBox data typically lags real-time by 5-15 min">'
            f'(+~10m lag)</span><br>'
            f'<span style="color:{t.text_faded};">Next refresh: </span>'
            f'<span id="bne-refresh-countdown" '
            f'data-next="{int(next_refresh_dt.timestamp())}" '
            f'style="color:{t.c_green};">{refresh_txt}</span>'
        )
    else:
        api_txt = f'<span style="color:{t.text_faded};">Loading data...</span>'
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
        airline_prefix = "".join(c for c in flight_num_dd if c.isalpha())[:3].upper()
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

    # ── Process flights ───────────────────────────────────────────────────────
    processed = []
    for f in deduped_flights:
        flight_num = f.get("number", "N/A")
        if flight_num in GHOST_FLIGHTS:
            continue

        status_raw  = f.get("status", "").lower()
        # TEMP DEBUG — log full raw object for any flight whose status looks unusual
        # (diverted/redirected) so we can see what AeroDataBox gives us for a
        # diverted-in flight. Remove once we've captured one.
        if "divert" in status_raw or "redirect" in status_raw:
            log.warning("DIVERT DEBUG [%s]: %s", f.get("number"), f)
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
        if arrival_node:
            arr_ap   = arrival_node.get("airport") or {}
            arr_icao = str(arr_ap.get("icao", "")).upper()
            arr_iata = str(arr_ap.get("iata", "")).upper()
            if arr_icao and arr_icao != AIRPORT_ICAO:
                log.info("Skipping %s — destination is %s, not %s", flight_num, arr_icao, AIRPORT_ICAO)
                continue
            if arr_iata and arr_iata not in ("BNE", ""):
                log.info("Skipping %s — destination IATA is %s, not BNE", flight_num, arr_iata)
                continue

        if not is_strictly_international(str(arr.get("terminal", "")),
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
        # Apply to ALL revised times (not just has_departed) — a flight with a
        # genuine revised ETA is being radar-tracked regardless of whether
        # AeroDataBox has updated its status field to "enroute". The earlier
        # has_departed gate meant flights whose status hadn't flipped to airborne
        # never got compensated, which is why some still showed ~10 min late.
        # (Pre-departure schedule-tweaks were already converted to "scheduled"
        # above, so anything still "revised" here is a real in-flight estimate.)
        if t_type == "revised":
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
        is_div = status_raw == "diverted"

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
        # CRITICAL: only fire if the flight has actually departed origin. A flight
        # that hasn't departed yet but is past its scheduled arrival is DELAYED at
        # origin, NOT landed (e.g. NZ 205 Sch 08:05 but actually leaving at 09:00 →
        # don't mark "Landed 15m ago" — it's still on the ground at origin).
        if (not is_lan
                and t_type in ("scheduled", "revised")
                and t_diff < -API_LAG_MINS
                and status_raw not in AIRBORNE_STATUSES
                and has_departed):
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
    # "was XX". History persists in session_state across the 60s fragment reruns.
    # Wrapped in try/except so a detection issue can never blank the whole board.
    try:
        gate_hist = st.session_state.get("gate_history", {})
        for p in processed:
            if p.get("is_gap") or p.get("is_surge"):
                continue
            fn   = p.get("num")
            cur  = p.get("gate")
            if not fn or cur in (None, "TBA"):
                continue
            prev = gate_hist.get(fn)
            if prev and prev != "TBA" and prev != cur:
                p["prev_gate"] = prev          # genuine change — flag for display
            gate_hist[fn] = cur                # update history to current
        st.session_state.gate_history = gate_hist
    except Exception as e:
        log.warning("Gate change detection failed: %s", e)

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
        lbl = "🟢 ACTIVE" if is_active else "🔄"

        end_str = (f"{t2_safe.strftime('%H:%M')}, approx" if next_is_sch
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
                    f'<div class="{cls}">{lbl} {format_hm(gap_remaining)} BEFORE NEXT FLIGHT '
                    f'<span style="opacity:0.6; font-weight:400; margin-left:8px;">'
                    f'(Ends {end_str})</span>'
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
                f'<div class="{cls}">{lbl} {format_hm(display_min)} GAP '
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
        cluster_weight = sum(get_aircraft_pax_weight(f.get("ac_text", "")) for f in cluster)
        if len(cluster) >= SURGE_MIN_FLIGHTS or cluster_weight >= SURGE_MIN_WEIGHT:
            surge_used.update(cluster_idx)
            w_start = cluster[0]["dt"]
            w_end   = cluster[-1]["dt"]
            processed.append({
                "is_surge": True,
                "time_key": w_start.timestamp() - 1,
                "html": (
                    f'<div class="surge-banner"><span class="surge-icon">⚡</span>'
                    f'SURGE {w_start.strftime("%H:%M")}–{w_end.strftime("%H:%M")} '
                    f'({len(cluster)} flights)</div>'
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
                next_gap_txt = f'<span style="color:{t.c_green};">NOW ({g["remaining"]}m)</span>'
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
                    <div>STALE DATA — last update was {age_minutes} min ago</div>
                    <div style="font-weight:400; font-size:0.85em; opacity:0.9; margin-top:2px;">
                        API refresh is failing. Treat all times below with caution and check the airport FIDS board.
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="summary-strip">
        <div class="s-item"><span class="s-val" style="color:{t.c_blue};">{len(incoming)}</span>Incoming</div>
        <div class="s-item"><span class="s-val" style="color:{t.c_green};">{next_gap_txt}</span>Next Gap</div>
        <div class="s-item"><span class="s-val" style="color:{t.c_amber};">{busiest_txt}</span>Busiest</div>
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
                f"Earlier Arrivals</span>"
                f"<div style='flex:1; height:1px; background:{t.border_muted};'></div>"
                f"</div>",
                unsafe_allow_html=True,
            )
            landed_divider_shown = True

        mid       = f"z_{i}"
        has_photo = pf["photo_url"] != "NOT_FOUND"
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

        if tag == "Sch":
            time_display = (
                f'<span class="mono" style="color:{t.text_muted};">Sch {pf["sch_time"]}</span>'
            )
        else:
            time_display = (
                f'<span class="mono" style="color:{t.text_muted}; font-size:0.85em;">Sch {pf["sch_time"]}</span>'
                f' • <span class="mono" style="color:{time_color}; font-weight:700; font-size:1.05em;">{tag} {pf["actual_time"]}</span>'
            )

        zoom_src = pf["photo_url"] if has_photo else pf["logo_url"]
        gate_cls = "gate-tba" if pf["gate"] == "TBA" else "gate-num"

        # Gate-change badge — small amber "was XX" tag if the gate changed recently
        gate_change_badge = ""
        if pf.get("prev_gate"):
            gate_change_badge = (
                f'<span style="display:block; font-size:0.32em; font-weight:700; '
                f'color:{t.c_amber}; letter-spacing:0.5px; margin-top:1px;">'
                f'⚠ was {pf["prev_gate"]}</span>'
            )

        # Replace the misleading "In Xh Ym" countdown with "Check Board" when
        # we don't have radar data — keep the gate visible but don't fake an ETA.
        if suppress_countdown:
            status_col_text  = "⚠️ Check Board"
            status_col_color = t.c_amber
        else:
            status_col_text  = pf["status_text"]
            status_col_color = pf["status_color"]

        # Build the Flightradar24 deep link using the IATA flight number in the
        # /data/flights/ format (lowercase, no spaces). This points to the flight's
        # data page which works whether or not the flight is currently airborne.
        # The old /<callsign> format only worked for live flights and frequently
        # resolved to the wrong aircraft when callsigns clashed or weren't airborne.
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
                <div class="ac-line">{pf['ac_text']}</div>
                <div style="font-size:0.8em; color:{t.text_muted};">{time_display}</div>
            </div>
            <div class="status-col">
                <div style="font-size:0.6em; color:{t.text_muted}; font-weight:700; letter-spacing:1px;">GATE</div>
                <div class="mono {gate_cls}">{pf['gate']}{gate_change_badge}</div>
                <div style="font-size:0.85em; font-weight:700; color:{status_col_color}; margin-top:2px;">{status_col_text}</div>
            </div>
        </div>
        <input type="checkbox" id="{mid}" class="img-zoom-chk" style="display:none;">
        <div class="img-zoom-modal">
            <label for="{mid}" class="img-zoom-close-bg"></label>
            <label for="{mid}" class="close-btn">&times;</label>
            <img src="{zoom_src}"/>
        </div>
        """, unsafe_allow_html=True)

    # ── Render Diverted ───────────────────────────────────────────────────────
    divs = sorted([p for p in processed if p.get("is_diverted")], key=lambda x: x["s_dt_val"])
    if divs:
        st.markdown(
            f"<hr style='margin:15px 0 8px 0; opacity:0.2;'>"
            f"<div style='color:{t.c_purple}; font-size:0.85em; font-weight:700; margin-bottom:5px;'>✈️ Diverted — not arriving at BNE</div>",
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
            f"<div style='color:{t.c_red}; font-size:0.85em; font-weight:700; margin-bottom:5px;'>❌ Canceled</div>",
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
        f"<div style='text-align:center; color:{t.text_muted}; font-size:0.65em; margin-top:20px;'>Dev: Phillip Yeh | V11.97</div>",
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
