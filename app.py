import streamlit as st
import pandas as pd
import requests
import logging
import math
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
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
IMMINENT_MINS            = 40   # red "hot" threshold (25 real + 15 lag compensation)
API_LAG_MINS             = 15   # AeroDataBox data is ~15 min behind real-time
OPENSKY_PREFER_UNDER_MIN = 60   # use OpenSky over AeroDataBox for flights < 60 min out
IMAGE_WORKERS            = 15
PHOTO_FAIL_TTL_SEC       = 600  # retry failed photo lookups after 10 min
SURGE_WINDOW_MINS        = 20   # cluster detection window
SURGE_MIN_FLIGHTS        = 3    # minimum flights to trigger surge alert
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
OPENSKY_ENABLED      = True
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
}

# FIX 5 — use constant in the fragment decorator (was hardcoded "60s")
UI_REFRESH_SEC           = 60
API_DATA_TTL_SEC         = 600

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bne-board")

# FIX 4 — module-level fail cache + lock replaces thread-unsafe st.session_state access
_photo_fails: dict       = {}
_photo_fails_lock        = threading.Lock()

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


def get_theme(is_light: bool) -> ThemeParams:
    if is_light:
        return ThemeParams(
            bg_main="#F8FAFC", bg_card="#FFFFFF", text_main="#0F172A", text_muted="#475569",
            text_faded="#94A3B8", border_muted="#CBD5E1", gap_bg="#FFFFFF", gap_active_bg="#ECFDF5",
            gap_active_text="#059669", modal_bg="rgba(255,255,255,0.95)", fallback_bg="#F1F5F9",
            c_blue="#2563EB", c_green="#059669", c_amber="#D97706", c_red="#DC2626",
            c_purple="#6D28D9", c_purple_bg="#F3E8FF",
        )
    return ThemeParams(
        bg_main="#0F172A", bg_card="#1E293B", text_main="white", text_muted="#94A3B8",
        text_faded="#CBD5E1", border_muted="#475569", gap_bg="#0F172A", gap_active_bg="#064E3B",
        gap_active_text="#A7F3D0", modal_bg="rgba(15,23,42,0.92)", fallback_bg="#1E293B",
        c_blue="#60A5FA", c_green="#34D399", c_amber="#F59E0B", c_red="#F87171",
        c_purple="#C4B5FD", c_purple_bg="#1E1B4B",
    )


def get_dynamic_css(t: ThemeParams) -> str:
    return f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=JetBrains+Mono:wght@600&display=swap');
        #MainMenu {{visibility: hidden;}} header {{visibility: hidden;}}
        .stApp {{ background-color: {t.bg_main}; }}
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

        @keyframes logoFade  {{ 0%, 45% {{ opacity: 1; }} 55%, 100% {{ opacity: 0; }} }}
        @keyframes photoFade {{ 0%, 45% {{ opacity: 0; }} 55%, 95%  {{ opacity: 1; }} 100% {{ opacity: 0; }} }}

        .logo-layer  {{ animation: logoFade 10s infinite;  background: #FFFFFF; padding: 4px; object-fit: contain !important; border-radius: 8px; z-index: 2; }}
        .photo-layer {{ animation: photoFade 10s infinite; object-fit: cover !important;   z-index: 1; }}

        .flight-card {{
            border-radius: 10px; padding: 10px 14px; margin-bottom: 8px; display: flex; align-items: center;
            color: {t.text_main}; box-shadow: 0 4px 10px rgba(0,0,0,0.15); border-left: 5px solid {t.c_blue}; transition: opacity 0.3s ease;
        }}
        .info-col   {{ flex-grow: 1; min-width: 0; overflow: hidden; }}
        .info-col .ac-line {{ font-size: 0.7em; color: {t.text_faded}; margin: 1px 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .status-col {{ text-align: right; min-width: 110px; display: flex; flex-direction: column; justify-content: center; }}
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
            background: linear-gradient(90deg, #7F1D1D 0%, #991B1B 100%); border-left: 5px solid #EF4444; border-radius: 8px;
            padding: 7px 14px; margin: 6px 0 8px 0; color: #FCA5A5; font-size: 0.82em; font-weight: 700; display: flex; align-items: center; gap: 8px;
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
        if landed_mins <= RECENT_LANDED_MAX:
            return FlightStyle(t.c_green, t.c_green, t.bg_main,
                               f"Landed {format_hm(landed_mins)} ago", "0.75", "grayscale(40%)")
        return FlightStyle(t.border_muted, t.text_muted, t.bg_main,
                           f"Landed {format_hm(landed_mins)} ago", "0.4", "grayscale(80%)")

    m_left     = max(0, t_diff)
    delay_mins = max(0, int(round(delay_hours * 60)))

    if t_type == "scheduled" and t_diff <= 0:
        return FlightStyle(t.c_amber, t.c_amber, t.bg_card, "NO UPDATE", "1.0", "none")
    if m_left < IMMINENT_MINS:
        return FlightStyle(t.c_red, t.c_red, t.bg_card, f"In {format_hm(m_left)}", "1.0", "none")
    if delay_hours >= SEVERE_DELAY_HOURS:
        return FlightStyle("#7F1D1D", t.c_red, t.bg_card, f"🔴 +{format_hm(delay_mins)} Late", "1.0", "none")
    if delay_hours >= HEAVY_DELAY_HOURS:
        return FlightStyle("#92400E", t.c_amber, t.bg_card, f"🟠 +{format_hm(delay_mins)} Late", "1.0", "none")
    return FlightStyle(t.c_blue, t.c_blue, t.bg_card, f"In {format_hm(m_left)}", "1.0", "none")


# ─────────────────────────────────────────────
#  3. CORE LOGIC
# ─────────────────────────────────────────────
def format_hm(total_minutes: int) -> str:
    h, m = divmod(total_minutes, 60)
    return f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"


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
@st.cache_data(show_spinner=False)
def _photo_cache_permanent(reg: str) -> str:
    return _fetch_photo_http(reg)


def _fetch_photo_http(reg: str) -> str:
    try:
        r = requests.get(
            f"https://api.planespotters.net/pub/photos/reg/{reg}",
            headers={"User-Agent": "BNE-Board-App/2.0"},
            timeout=3.0,
        )
        if r.status_code == 200:
            photos = r.json().get("photos", [])
            if photos:
                return photos[0]["thumbnail_large"]["src"]
    except Exception as e:
        log.warning("Photo fetch failed for reg=%s: %s", reg, e)
    return "NOT_FOUND"


def get_photo_from_api(reg: str) -> str:
    if not reg:
        return "NOT_FOUND"
    cached = _photo_cache_permanent(reg)
    if cached != "NOT_FOUND":
        return cached

    # FIX 4 — use module-level dict + lock instead of st.session_state (not thread-safe)
    with _photo_fails_lock:
        fail_entry = _photo_fails.get(reg)

    if fail_entry and (datetime.now() - fail_entry).total_seconds() < PHOTO_FAIL_TTL_SEC:
        return "NOT_FOUND"

    url = _fetch_photo_http(reg)
    if url != "NOT_FOUND":
        # FIX 3 — removed _photo_cache_permanent.clear() which was wiping ALL cached photos;
        # the permanent cache will naturally populate on the next call for this reg
        return url

    with _photo_fails_lock:
        _photo_fails[reg] = datetime.now()
    return "NOT_FOUND"


@st.cache_data(ttl=API_DATA_TTL_SEC, show_spinner=False)
def fetch_flight_data(anchor: str, from_time: str, to_time: str) -> list:
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{AIRPORT_ICAO}/{from_time}/{to_time}"
    headers = {
        "X-RapidAPI-Key":  st.secrets["X_RAPIDAPI_KEY"],
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
    }
    try:
        r = requests.get(
            url, headers=headers,
            params={"direction": "Arrival", "withCancelled": "true", "withCodeshared": "false"},
            timeout=10,
        )
        r.raise_for_status()
        st.session_state.api_last_hit = datetime.now(pytz.timezone(TIMEZONE))
        return r.json().get("arrivals", [])
    except Exception as e:
        log.error("AeroDataBox API error: %s", e)
        st.session_state.api_error = str(e)
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


@st.cache_data(ttl=API_DATA_TTL_SEC, show_spinner=False)
def fetch_opensky_states(anchor: str) -> dict:
    if not OPENSKY_ENABLED:
        return {}
    try:
        r = requests.get(
            "https://opensky-network.org/api/states/all",
            params=OPENSKY_BBOX,
            headers={"User-Agent": "BNE-Board-App/2.0"},
            timeout=8,
        )
        if r.status_code == 200:
            result = {}
            for s in (r.json().get("states") or []):
                callsign  = (s[1] or "").strip().upper()
                on_ground = s[8]
                velocity  = s[9]
                if callsign and not on_ground and velocity:
                    result[callsign] = {
                        "lat": s[6], "lon": s[5],
                        "velocity_kts": velocity * 1.94384,
                        "altitude_ft":  (s[7] or 0) * 3.281,
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
#  4. UI SETUP & FRAGMENT EXECUTION (V11.13)
# ─────────────────────────────────────────────
st.set_page_config(page_title="BNE Pro Arrivals", page_icon="✈️", layout="centered")
if "api_last_hit" not in st.session_state: st.session_state.api_last_hit = None
if "api_error"    not in st.session_state: st.session_state.api_error    = None
if "theme_light"  not in st.session_state: st.session_state.theme_light  = False


# FIX 5 — use UI_REFRESH_SEC constant instead of hardcoded "60s"
@st.fragment(run_every=f"{UI_REFRESH_SEC}s")
def live_dashboard():
    aest     = pytz.timezone(TIMEZONE)
    now_aest = datetime.now(aest)
    t        = get_theme(st.session_state.theme_light)

    # Inject dynamic CSS first so header styling is correct
    st.markdown(get_dynamic_css(t), unsafe_allow_html=True)

    c1, c2, c3 = st.columns([5, 1, 2])
    with c1:
        st.subheader("✈️ Arrivals")
    with c2:
        # Button shows the OPPOSITE icon — click it to switch to that mode
        toggle_icon = "🌙" if st.session_state.theme_light else "☀️"
        if st.button(toggle_icon, help="Toggle light/dark theme", use_container_width=True):
            st.session_state.theme_light = not st.session_state.theme_light
            st.rerun()
    with c3:
        st.markdown(
            f'<div style="font-size:0.8em;color:{t.text_muted};text-align:right;margin-top:5px;">'
            f'🕒 <span id="bne-live-clock">{now_aest.strftime("%H:%M:%S")}</span></div>',
            unsafe_allow_html=True,
        )
        api_t   = st.session_state.get("api_last_hit")
        api_txt = (f'API: {api_t.strftime("%H:%M")} '
                   f'<span style="color:{t.c_amber};">(~{API_LAG_MINS}m lag)</span>'
                   if api_t else "API: --:--")
        st.markdown(
            f'<div style="font-size:0.7em;color:{t.text_faded};text-align:right;">{api_txt}</div>',
            unsafe_allow_html=True,
        )

    with st.expander(" 👋👋👋 (Operational Guide)"):
        st.markdown(f"""
        **Why use this app?**
        I built this dashboard to help us manage our daily shifts more easily. Use it to predict peak traffic, coordinate floor tasks, and plan your break windows (Gaps) with confidence.

        **How to read the times:**
        * <span class="mono" style="color:{t.c_blue};font-weight:bold;">Act</span>: **Actual** landing time. The crowd is on their way!
        * <span class="mono" style="color:{t.text_faded};font-weight:bold;">Est</span>: **Estimated** arrival based on live radar. Very reliable.
        * <span class="mono" style="color:{t.text_muted};font-weight:bold;">Sch</span>: **Scheduled** time only.

        **Dual Radar:**
        This app uses **two** data sources. If the primary API (AeroDataBox) has no radar data, it falls back to **OpenSky Network** (live ADS-B transponder data).

        **Flight Status Tags:**
        * ⚠️ **Check Board**: No live radar data yet. Check physical airport FIDS boards.
        * 🟠 **Delayed**: Flight is running 3+ hours late.
        * ⚡ **Surge**: 3+ flights arriving within 20 minutes — all hands on deck.

        *Developed by Phillip Yeh to support the BNE Lotte Team.*
        """, unsafe_allow_html=True)

    # ── Fetch ──────────────────────────────────────────────────────────────────
    _epoch  = datetime(2000, 1, 1, tzinfo=aest)
    anchor  = (_epoch + timedelta(seconds=(int((now_aest - _epoch).total_seconds()) // API_DATA_TTL_SEC) * API_DATA_TTL_SEC)).strftime("%Y-%m-%dT%H:%M")

    raw_flights = fetch_flight_data(
        anchor,
        (now_aest - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M"),
        (now_aest + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M"),
    )
    opensky_data = fetch_opensky_states(anchor)

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
        phy_key = f"{str(dep_ap.get('iata', ''))}|{sch_str}|{f.get('aircraft', {}).get('model', '')}"

        if phy_key and phy_key in physical_seen:
            existing = physical_seen[phy_key]
            if not existing.get("aircraft", {}).get("reg") and f.get("aircraft", {}).get("reg"):
                deduped_flights.remove(existing)
                physical_seen[phy_key] = f
                deduped_flights.append(f)
            continue
        physical_seen[phy_key] = f
        deduped_flights.append(f)

    all_regs = list({f.get("aircraft", {}).get("reg", "")
                     for f in deduped_flights if f.get("aircraft", {}).get("reg")})
    with ThreadPoolExecutor(max_workers=IMAGE_WORKERS) as executor:
        executor.map(get_photo_from_api, all_regs)

    # ── Process flights ───────────────────────────────────────────────────────
    processed = []
    for f in deduped_flights:
        flight_num = f.get("number", "N/A")
        if flight_num in GHOST_FLIGHTS:
            continue

        status_raw  = f.get("status", "").lower()
        dep_node    = f.get("departure") or {}
        dep_ap      = dep_node.get("airport") or f.get("movement", {}).get("airport") or {}
        arr         = f.get("arrival") or f.get("movement") or {}
        ac_m        = f.get("aircraft", {}).get("model", "")
        ac_r        = f.get("aircraft", {}).get("reg", "")
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
        if t_type == "revised" and abs((best_dt - s_dt).total_seconds()) < 60 and not has_departed:
            t_type = "scheduled"

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

        # FIX 6 — time-based landed fallback for scheduled-only flights.
        # If AeroDataBox never provided radar data and the flight is more than
        # API_LAG_MINS past its scheduled time, and the API isn't reporting it
        # as still airborne, assume it has landed. Prevents flights sitting on
        # "NO UPDATE" indefinitely after they've actually arrived (e.g. JQ100).
        if (not is_lan and t_type == "scheduled"
                and t_diff < -API_LAG_MINS
                and status_raw not in AIRBORNE_STATUSES):
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
            "origin":       city,
            "iata":         origin_iata,
            "gate":         arr.get("gate", "TBA"),
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
        t2_safe = t2 - timedelta(minutes=API_LAG_MINS)

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

        if is_virtual:
            # Pre-shift bar: nothing has landed recently, just counting down
            # to the next arrival
            processed.append({
                "is_gap":   True,
                "time_key": t1.timestamp() + 1,
                "html": (
                    f'<div class="{cls}">{lbl} {format_hm(gap_remaining)} BEFORE NEXT FLIGHT '
                    f'<span style="opacity:0.6; font-weight:400; margin-left:8px;">'
                    f'(Ends {t2_safe.strftime("%H:%M")})</span></div>'
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
                f'({window_start.strftime("%H:%M")}–{t2_safe.strftime("%H:%M")})</span>'
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
        if len(cluster) >= SURGE_MIN_FLIGHTS:
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
    for i, pf in enumerate(processed):
        if pf.get("is_canceled") or pf.get("is_diverted"):
            continue
        if pf.get("is_gap") or pf.get("is_surge"):
            st.markdown(pf["html"], unsafe_allow_html=True)
            continue

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

        tag        = "Act" if (pf["is_landed"] or pf["time_type"] == "actual") else ("Est" if pf["time_type"] == "revised" else "Sch")
        time_color = t.c_blue if tag == "Act" else (t.text_faded if tag == "Est" else t.text_muted)

        if tag == "Sch":
            time_display = (
                f'<span class="mono" style="color:{t.text_muted};">Sch {pf["sch_time"]}</span>'
                f' <span style="color:{t.c_amber}; font-size:0.75em; font-weight:700; margin-left:6px;">⚠️ Check Board</span>'
            )
        else:
            time_display = (
                f'<span class="mono" style="color:{t.text_muted};">Sch {pf["sch_time"]}</span>'
                f' • <span class="mono" style="color:{time_color}; font-weight:700;">{tag} {pf["actual_time"]}</span>'
            )

        zoom_src = pf["photo_url"] if has_photo else pf["logo_url"]
        gate_cls = "gate-tba" if pf["gate"] == "TBA" else "gate-num"

        st.markdown(f"""
        <div class="flight-card" style="border-left-color:{pf['border_color']}; background-color:{pf['bg_color']}; opacity:{pf['card_opacity']};">
            {img_html}
            <div class="info-col">
                <div style="font-size:1.1em; font-weight:700;">{pf['num']}<span style="font-size:0.7em; color:{t.text_muted}; margin-left:8px;">{pf['origin']} [{pf['iata']}]</span></div>
                <div class="ac-line">{pf['ac_text']}</div>
                <div style="font-size:0.8em; color:{t.text_muted};">{time_display}</div>
            </div>
            <div class="status-col">
                <div style="font-size:0.6em; color:{t.text_muted}; font-weight:700; letter-spacing:1px;">GATE</div>
                <div class="mono {gate_cls}">{pf['gate']}</div>
                <div style="font-size:0.85em; font-weight:700; color:{pf['status_color']}; margin-top:2px;">{pf['status_text']}</div>
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
        f"<div style='text-align:center; color:{t.text_muted}; font-size:0.65em; margin-top:20px;'>Dev: Phillip Yeh | V11.20</div>",
        unsafe_allow_html=True,
    )


live_dashboard()

# ── Live clock (uses real AEST, no drift) ──
st.html("""
<script>
    const doc = window.parent.document;
    const aestFormatter = new Intl.DateTimeFormat('en-AU', {
        timeZone: 'Australia/Brisbane',
        hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
    });
    setInterval(function() {
        const clockEl = doc.getElementById('bne-live-clock');
        if (clockEl) { clockEl.innerText = aestFormatter.format(new Date()); }
    }, 1000);
</script>
""")
