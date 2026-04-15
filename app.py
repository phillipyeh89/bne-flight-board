import streamlit as st
import streamlit.components.v1 as components
import requests
import pandas as pd
import logging
import math
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
IMMINENT_MINS            = 25   # red "hot" threshold
IMAGE_WORKERS            = 15
PHOTO_FAIL_TTL_SEC       = 600  # retry failed photo lookups after 10 min
SURGE_WINDOW_MINS        = 20   # cluster detection window
SURGE_MIN_FLIGHTS        = 3    # minimum flights to trigger surge alert
DOMESTIC_TERMINALS       = ('D', 'DOM', 'D-ANC', 'GAT')
SMALL_AIRCRAFT_FILTER    = ('BEECH', 'FAIRCHILD', 'CESSNA', 'PIPER', 'PILATUS', 'KING AIR', 'METROLINER', 'SAAB')

AIRBORNE_STATUSES = {"enroute", "departed", "approaching"}

CITY_MAP = {
    "Lapu-Lapu City": "Cebu", "Denpasar-Bali Island": "Bali",
    "Ho Chi Minh City": "Saigon", "Yaren District": "Nauru",
    "Guangzhou Baiyun": "Guangzhou",
}

# ── Pax estimation (typical 3-class config) ──────────────────────────────────
# Checked in order — most specific substring first.
PAX_TABLE = [
    ("A380",    500), ("777-300", 350), ("777-200", 300), ("777",  320),
    ("A350-1",  350), ("A350",    300),
    ("787-10",  320), ("787-9",   280), ("787-8",   240), ("787",  275),
    ("A330-9",  280), ("A330-3",  280), ("A330-2",  250), ("A330", 260),
    ("A340",    270), ("767",     220),
    ("A321",    200), ("A320",    165), ("A319",    140),
    ("737 MAX", 175), ("737-9",   175), ("737-8",   170), ("737",  165),
    ("ATR",     70),  ("EMBRAER 190", 100), ("EMBRAER 195", 120),
    ("ERJ-190", 100), ("ERJ-195", 120), ("E190", 100), ("E195", 120),
]
PAX_LIGHT_THRESHOLD  = 200
PAX_HEAVY_THRESHOLD  = 300

# ── Seasonal load factors ────────────────────────────────────────────────────
# Monthly estimated occupancy rates by region.
# Conservative baseline — actual load varies by airline, day-of-week, and events
# but these give the team a useful ballpark for staffing decisions.
#                         Jan   Feb   Mar   Apr   May   Jun   Jul   Aug   Sep   Oct   Nov   Dec
SEASONAL_LOAD = {
    "east_asia": {        # China, HK, Taiwan, Japan, Korea
        1: 0.90, 2: 0.82, 3: 0.78, 4: 0.85,   # CNY Jan peak; cherry blossom Apr
        5: 0.72, 6: 0.85, 7: 0.92, 8: 0.92,   # Asian summer holidays Jul-Aug
        9: 0.78, 10: 0.88, 11: 0.78, 12: 0.90, # Golden Week Oct; Christmas Dec
    },
    "se_asia": {          # Singapore, Vietnam, Thailand, Philippines, Malaysia, Indonesia
        1: 0.85, 2: 0.75, 3: 0.72, 4: 0.78,   # Songkran travel Apr
        5: 0.68, 6: 0.80, 7: 0.88, 8: 0.88,   # Peak summer Jul-Aug
        9: 0.72, 10: 0.75, 11: 0.78, 12: 0.88, # Year-end travel Dec
    },
    "pacific": {          # NZ, Fiji, New Caledonia, PNG, Vanuatu, Samoa, Norfolk
        1: 0.90, 2: 0.78, 3: 0.75, 4: 0.82,   # AU/NZ summer peak Jan; Easter Apr
        5: 0.70, 6: 0.75, 7: 0.80, 8: 0.75,   # AU school holidays Jul
        9: 0.72, 10: 0.78, 11: 0.80, 12: 0.90, # Christmas Dec
    },
    "south_asia": {       # India, Sri Lanka, Bangladesh
        1: 0.82, 2: 0.75, 3: 0.78, 4: 0.75,
        5: 0.68, 6: 0.78, 7: 0.85, 8: 0.85,
        9: 0.75, 10: 0.82, 11: 0.80, 12: 0.88, # Diwali Oct; year-end Dec
    },
    "middle_east": {      # UAE, Qatar — mostly connecting traffic
        1: 0.82, 2: 0.75, 3: 0.78, 4: 0.78,
        5: 0.72, 6: 0.80, 7: 0.88, 8: 0.85,
        9: 0.75, 10: 0.78, 11: 0.80, 12: 0.88,
    },
}

SEASONAL_DEFAULT = {      # Fallback for unmapped origins
    1: 0.80, 2: 0.72, 3: 0.72, 4: 0.75,
    5: 0.68, 6: 0.75, 7: 0.82, 8: 0.82,
    9: 0.72, 10: 0.75, 11: 0.75, 12: 0.85,
}

COUNTRY_REGION = {
    # East Asia
    "cn": "east_asia", "hk": "east_asia", "tw": "east_asia",
    "jp": "east_asia", "kr": "east_asia", "mo": "east_asia",
    # SE Asia
    "sg": "se_asia", "vn": "se_asia", "th": "se_asia",
    "ph": "se_asia", "my": "se_asia", "id": "se_asia",
    "kh": "se_asia", "la": "se_asia", "mm": "se_asia", "bn": "se_asia",
    # Pacific
    "nz": "pacific", "fj": "pacific", "nc": "pacific",
    "pg": "pacific", "vu": "pacific", "ws": "pacific",
    "to": "pacific", "nf": "pacific", "nr": "pacific",
    # South Asia
    "in": "south_asia", "lk": "south_asia", "bd": "south_asia",
    "np": "south_asia", "pk": "south_asia",
    # Middle East
    "ae": "middle_east", "qa": "middle_east", "sa": "middle_east",
    "bh": "middle_east", "om": "middle_east", "kw": "middle_east",
}

# ── Airline-level load overrides ─────────────────────────────────────────────
# Some carriers consistently run near-full regardless of season.
# Map IATA 2-letter prefix → fixed load factor (bypasses seasonal curve).
AIRLINE_LOAD_OVERRIDE = {
    "SQ": 0.95,   # Singapore Airlines — consistently full year-round
    # Add more here as you observe patterns, e.g.:
    # "CX": 0.92,  # Cathay Pacific
}

# ── OpenSky Network — free ADS-B radar supplement ────────────────────────────
# When AeroDataBox has only scheduled time (no radar), we query OpenSky to see
# if the aircraft is actually airborne and calculate an ETA from its live position.
YBBN_LAT, YBBN_LON = -27.3842, 153.1175
OPENSKY_BBOX = {"lamin": -38, "lamax": -10, "lomin": 135, "lomax": 170}
OPENSKY_ENABLED      = True
OPENSKY_MIN_SPEED_KT = 80    # ignore ground vehicles / taxiing
OPENSKY_MAX_ETA_MIN  = 600   # sanity cap — 10 hours

# IATA → ICAO airline code mapping (for OpenSky callsign matching)
# OpenSky callsigns use ICAO format: "SIA321" not "SQ321"
AIRLINE_ICAO = {
    "QF": "QFA", "SQ": "SIA", "CX": "CPA", "VA": "VOZ", "JQ": "JST",
    "NZ": "ANZ", "FJ": "FJI", "CI": "CAL", "CZ": "CSN", "MU": "CES",
    "TG": "THA", "VN": "HVN", "MH": "MAS", "GA": "GIA", "PR": "PAL",
    "KE": "KAL", "OZ": "AAR", "JL": "JAL", "NH": "ANA", "TR": "TGW",
    "3K": "JSA", "BI": "RBA", "PX": "ANG", "SB": "ACI", "EK": "UAE",
    "QR": "QTR", "EY": "ETD", "AI": "AIC", "AK": "AXM", "5J": "CEB",
    "NF": "AVN", "S7": "SBI", "CA": "CCA", "HX": "CRK", "UO": "HKE",
    "BR": "EVA", "IT": "TTW", "MM": "APJ", "TW": "TWB", "PG": "BKP",
    "WEB": "WEB",
}

UI_REFRESH_SEC           = 60
API_DATA_TTL_SEC         = 600
STALE_DATA_THRESHOLD_MIN = 30

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bne-board")


# ─────────────────────────────────────────────
#  2. STATUS CLASSIFICATION (pure function)
# ─────────────────────────────────────────────
@dataclass
class FlightStyle:
    border_color: str
    status_color: str
    bg_color:     str
    status_text:  str
    card_opacity: str
    img_filter:   str


def classify_flight_status(
    *,
    is_canceled: bool,
    is_landed: bool,
    landed_mins: int,
    t_diff: int,
    t_type: str,
    delay_hours: float,
    s_dt: datetime,
    now: datetime,
) -> FlightStyle:
    """Pure function — given flight state, returns all visual styling."""
    if is_canceled:
        archived = (now - s_dt).total_seconds() / 60 > 15
        if archived:
            return FlightStyle("#475569", "#94A3B8", "#0F172A", "CANCELED", "0.5", "grayscale(100%)")
        return FlightStyle("#EF4444", "#F87171", "#1E293B", "CANCELED", "0.5", "grayscale(100%)")

    if is_landed:
        if landed_mins <= RECENT_LANDED_MAX:
            return FlightStyle("#059669", "#34D399", "#0F172A",
                               f"Landed {_format_hm(landed_mins)} ago", "0.75", "grayscale(40%)")
        return FlightStyle("#475569", "#94A3B8", "#0F172A",
                           f"Landed {_format_hm(landed_mins)} ago", "0.4", "grayscale(80%)")

    m_left     = max(0, t_diff)
    delay_mins = max(0, int(round(delay_hours * 60)))

    if t_type == "scheduled" and t_diff <= 0:
        return FlightStyle("#F59E0B", "#FBBF24", "#0F172A", "NO UPDATE", "1.0", "none")
    if m_left < IMMINENT_MINS:
        return FlightStyle("#EF4444", "#F87171", "#1E293B",
                           f"In {_format_hm(m_left)}", "1.0", "none")
    if delay_hours >= SEVERE_DELAY_HOURS:
        return FlightStyle("#7F1D1D", "#FCA5A5", "#1E293B",
                           f"🔴 +{_format_hm(delay_mins)} Late", "1.0", "none")
    if delay_hours >= HEAVY_DELAY_HOURS:
        return FlightStyle("#92400E", "#FBBF24", "#1E293B",
                           f"🟠 +{_format_hm(delay_mins)} Late", "1.0", "none")
    return FlightStyle("#3B82F6", "#60A5FA", "#1E293B",
                       f"In {_format_hm(m_left)}", "1.0", "none")


# ─────────────────────────────────────────────
#  3. CORE LOGIC
# ─────────────────────────────────────────────
def _format_hm(total_minutes: int) -> str:
    h, m = divmod(total_minutes, 60)
    return f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"


def estimate_pax(aircraft_model: str, country_code: str = "", month: int = 0, flight_number: str = "") -> tuple:
    """
    Returns (estimated_pax: int, load_label: str, load_color: str, load_pct: int).
    Priority: airline override → seasonal regional curve → default.
    """
    model_upper = aircraft_model.upper()
    capacity = 0
    for keyword, cap in PAX_TABLE:
        if keyword.upper() in model_upper:
            capacity = cap
            break
    if capacity == 0:
        return 0, "", "", 0

    # Check airline-level override first (e.g. SQ always full)
    airline_prefix = "".join(c for c in flight_number if c.isalpha())[:2].upper()
    if airline_prefix in AIRLINE_LOAD_OVERRIDE:
        load_factor = AIRLINE_LOAD_OVERRIDE[airline_prefix]
    else:
        region = COUNTRY_REGION.get(country_code.lower(), "")
        load_table = SEASONAL_LOAD.get(region, SEASONAL_DEFAULT)
        load_factor = load_table.get(month, 0.75)

    pax = int(capacity * load_factor)
    load_pct = int(load_factor * 100)

    if pax >= PAX_HEAVY_THRESHOLD:
        return pax, "Heavy", "#EF4444", load_pct
    elif pax >= PAX_LIGHT_THRESHOLD:
        return pax, "Medium", "#F59E0B", load_pct
    else:
        return pax, "Light", "#34D399", load_pct


def extract_best_time(node: dict, tz) -> tuple:
    for key, label in (
        ("actualTime",    "actual"),
        ("revisedTime",   "revised"),
        ("scheduledTime", "scheduled"),
    ):
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


def is_strictly_international(terminal: str, country_code: str, aircraft_model: str, origin_iata: str) -> bool:
    t    = terminal.strip().upper()
    ac   = aircraft_model.upper()
    cc   = country_code.lower()
    iata = origin_iata.upper()
    if iata == "NLK":                                    return True
    if t in DOMESTIC_TERMINALS:                          return False
    if cc == "au":                                       return False
    if any(k in ac for k in SMALL_AIRCRAFT_FILTER):     return False
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
    fail_cache = st.session_state.setdefault("_photo_fails", {})
    fail_entry = fail_cache.get(reg)
    if fail_entry:
        if (datetime.now() - fail_entry).total_seconds() < PHOTO_FAIL_TTL_SEC:
            return "NOT_FOUND"
    url = _fetch_photo_http(reg)
    if url != "NOT_FOUND":
        _photo_cache_permanent.clear()
        return url
    else:
        fail_cache[reg] = datetime.now()
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


# ── OpenSky Network — free ADS-B supplement ──────────────────────────────────
def _iata_to_callsign(flight_number: str) -> str:
    """Convert IATA flight number 'QF354' → ICAO callsign 'QFA354'."""
    prefix = "".join(c for c in flight_number if c.isalpha())[:2].upper()
    digits = "".join(c for c in flight_number if c.isdigit())
    icao = AIRLINE_ICAO.get(prefix, prefix)
    return f"{icao}{digits}"


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    R = 3440.065
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


@st.cache_data(ttl=API_DATA_TTL_SEC, show_spinner=False)
def fetch_opensky_states(anchor: str) -> dict:
    """
    Fetch all airborne aircraft in Brisbane approach area from OpenSky Network.
    Returns dict of callsign → {lat, lon, velocity_kts, altitude_ft}.
    Free API, no key needed. One call per cache window covers all flights.
    """
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
            data = r.json()
            states = data.get("states") or []
            result = {}
            for s in states:
                callsign = (s[1] or "").strip().upper()
                on_ground = s[8]
                velocity = s[9]  # m/s ground speed
                if callsign and not on_ground and velocity:
                    result[callsign] = {
                        "lat":          s[6],
                        "lon":          s[5],
                        "velocity_kts": velocity * 1.94384,  # m/s → knots
                        "altitude_ft":  (s[7] or 0) * 3.281, # metres → feet
                    }
            log.info("OpenSky: %d airborne aircraft in bbox", len(result))
            return result
        elif r.status_code == 429:
            log.warning("OpenSky rate limited — skipping this cycle")
        else:
            log.warning("OpenSky HTTP %d", r.status_code)
    except Exception as e:
        log.warning("OpenSky query failed: %s", e)
    return {}


def opensky_estimate_eta(flight_number: str, opensky_data: dict, now: datetime, tz) -> tuple:
    """
    Look up a flight in OpenSky data by callsign.
    Returns (estimated_dt, 'revised') if found airborne with sane ETA, else (None, '').
    """
    if not opensky_data:
        return None, ""
    callsign = _iata_to_callsign(flight_number)
    state = opensky_data.get(callsign)
    if not state or state["velocity_kts"] < OPENSKY_MIN_SPEED_KT:
        return None, ""

    dist_nm  = _haversine_nm(state["lat"], state["lon"], YBBN_LAT, YBBN_LON)
    eta_min  = int(dist_nm / state["velocity_kts"] * 60)

    if eta_min < 1 or eta_min > OPENSKY_MAX_ETA_MIN:
        return None, ""

    est_dt = now + timedelta(minutes=eta_min)
    log.info("OpenSky ETA: %s (%s) → %s (%.0fnm @ %.0fkts)",
             flight_number, callsign, est_dt.strftime("%H:%M"),
             dist_nm, state["velocity_kts"])
    return est_dt, "revised"


# ─────────────────────────────────────────────
#  4. UI SETUP & CSS  (V11.3)
# ─────────────────────────────────────────────
st.set_page_config(page_title="BNE Pro Arrivals", page_icon="✈️", layout="centered")
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=JetBrains+Mono:wght@600&display=swap');
    #MainMenu {{visibility: hidden;}}
    header {{visibility: hidden;}}
    .block-container {{padding-top: 1rem; font-family: 'Inter', sans-serif; max-width: 700px;}}
    .mono {{ font-family: 'JetBrains Mono', monospace; letter-spacing: -0.5px; }}

    .flip-container {{ position: relative; width: 55px; height: 55px; margin-right: 12px; flex-shrink: 0; }}
    .flip-img {{ position: absolute; top: 0; left: 0; width: 55px; height: 55px; border-radius: 8px; border: 2.5px solid #475569; transition: opacity 1s ease-in-out; box-sizing: border-box; }}

    @keyframes logoFade  {{ 0%, 45% {{ opacity: 1; }} 55%, 100% {{ opacity: 0; }} }}
    @keyframes photoFade {{ 0%, 45% {{ opacity: 0; }} 55%, 95%  {{ opacity: 1; }} 100% {{ opacity: 0; }} }}

    .logo-layer  {{ animation: logoFade 10s infinite;  background: #FFFFFF; padding: 4px; object-fit: contain !important; border-radius: 8px; z-index: 2; }}
    .photo-layer {{ animation: photoFade 10s infinite; object-fit: cover !important;   z-index: 1; }}

    .flight-card {{
        border-radius: 10px; padding: 10px 14px;
        margin-bottom: 8px; display: flex; align-items: center; color: white;
        box-shadow: 0 4px 10px rgba(0,0,0,0.2); border-left: 5px solid #3B82F6;
        transition: opacity 0.3s ease;
    }}
    .info-col   {{ flex-grow: 1; min-width: 0; overflow: hidden; }}
    .info-col .ac-line {{ font-size: 0.7em; color: #CBD5E1; margin: 1px 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .status-col {{ text-align: right; min-width: 110px; display: flex; flex-direction: column; justify-content: center; }}
    .gate-num   {{ font-size: 1.85em; font-weight: 700; line-height: 1; }}
    .gate-tba   {{ font-size: 1.85em; font-weight: 700; line-height: 1; opacity: 0.35; }}

    /* ── Summary strip ────────────────────────── */
    .summary-strip {{
        display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
        background: #0F172A; border: 1px solid #1E293B; border-radius: 8px;
        padding: 10px 14px; margin-bottom: 10px; font-size: 0.78em; color: #94A3B8;
        gap: 4px 0;
    }}
    .summary-strip .s-item {{ text-align: center; min-width: 22%; }}
    .summary-strip .s-val  {{ font-weight: 700; font-size: 1.15em; display: block; }}

    /* ── Gap bar ──────────────────────────────── */
    .gap-bar {{
        background-color: #0F172A; border: 1px dashed #475569; border-left: 5px solid transparent;
        border-radius: 8px; padding: 8px 14px; margin: 4px 0 10px 0; text-align: center; color: #94A3B8;
        font-weight: 600; font-size: 0.85em; box-sizing: border-box;
    }}
    .gap-active {{ background-color: #064E3B; border-color: #10B981; border-left-color: #10B981; color: #A7F3D0; }}

    .gap-progress-track {{
        width: 100%; height: 5px; background: #1E293B; border-radius: 3px; margin-top: 6px; overflow: hidden;
    }}
    .gap-progress-fill {{
        height: 100%; border-radius: 3px; transition: width 1s linear;
    }}

    /* ── Surge banner ─────────────────────────── */
    .surge-banner {{
        background: linear-gradient(90deg, #7F1D1D 0%, #991B1B 100%);
        border-left: 5px solid #EF4444; border-radius: 8px;
        padding: 7px 14px; margin: 6px 0 8px 0; color: #FCA5A5;
        font-size: 0.82em; font-weight: 700; display: flex;
        align-items: center; gap: 8px;
    }}
    .surge-banner .surge-icon {{ font-size: 1.1em; }}

    /* ── Pax badge ────────────────────────────── */
    .pax-badge {{
        display: inline-block; font-size: 0.6em; font-weight: 700;
        padding: 1px 6px; border-radius: 4px; margin-top: 2px;
        letter-spacing: 0.3px;
    }}

    .img-zoom-modal {{
        display: none; position: fixed; top: 0; left: 0; width: 100vw; height: 100vh;
        background: rgba(15,23,42,0.92); z-index: 10000; align-items: center;
        justify-content: center; backdrop-filter: blur(10px);
    }}
    .img-zoom-chk:checked + .img-zoom-modal {{ display: flex !important; }}
    .img-zoom-modal img {{ max-width: 90%; max-height: 80%; border-radius: 12px; border: 2px solid #475569; object-fit: contain; z-index: 10001; }}
    .img-zoom-close-bg {{ position: absolute; top: 0; left: 0; width: 100%; height: 100%; cursor: pointer; z-index: 10000; }}
    .close-btn {{ position: absolute; top: 20px; right: 30px; color: white; font-size: 3.5em; font-weight: bold; cursor: pointer; z-index: 10002; line-height: 1; }}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  5. EXECUTION
# ─────────────────────────────────────────────
if "api_last_hit" not in st.session_state: st.session_state.api_last_hit = None
if "api_error"    not in st.session_state: st.session_state.api_error    = None

aest     = pytz.timezone(TIMEZONE)
now_aest = datetime.now(aest)

# ── Header ────────────────────────────────────
c1, c2 = st.columns([2, 1])
with c1:
    st.subheader("✈️ Arrivals")
with c2:
    st.markdown(
        f'<div style="font-size:0.8em;color:#94A3B8;text-align:right;margin-top:5px;">'
        f'🕒 Live: {now_aest.strftime("%H:%M:%S")}</div>',
        unsafe_allow_html=True,
    )
    api_t   = st.session_state.get("api_last_hit")
    api_txt = f'API: {api_t.strftime("%H:%M")}' if api_t else "API: --:--"
    st.markdown(
        f'<div style="font-size:0.7em;color:#64748B;text-align:right;">{api_txt}</div>',
        unsafe_allow_html=True,
    )

with st.expander(" 👋👋👋 (Operational Guide)"):
    st.markdown("""
    **Why use this app?**
    I built this dashboard to help us manage our daily shifts more easily. Use it to predict peak traffic, coordinate floor tasks, and plan your break windows (Gaps) with confidence.

    **How to read the times:**
    * <span class="mono" style="color:#7DD3FC;font-weight:bold;">Act</span>: **Actual** landing time. The crowd is on their way!
    * <span class="mono" style="color:#E2E8F0;font-weight:bold;">Est</span>: **Estimated** arrival based on live radar. Very reliable.
    * <span class="mono" style="color:#94A3B8;font-weight:bold;">Sch</span>: **Scheduled** time only.

    **Dual Radar:**
    This app uses **two** data sources. If the primary API (AeroDataBox) has no radar data, it falls back to **OpenSky Network** (live ADS-B transponder data) to estimate arrival times from the aircraft's actual position and speed. This reduces the number of ⚠️ Check Board warnings.

    **Flight Status Tags:**
    * ⚠️ **Check Board**: No live radar data yet. Check physical airport FIDS boards.
    * 🟠 **Delayed**: Flight is running 3+ hours late.
    * ⚡ **Surge**: 3+ flights arriving within 20 minutes — all hands on deck.

    **Passenger Load Badges:**
    * <span style="color:#34D399;">Light</span> (< 200 pax) · <span style="color:#F59E0B;">Medium</span> (200–300 pax) · <span style="color:#EF4444;">Heavy</span> (300+ pax)
    * Pax counts are conservative estimates adjusted by **seasonal demand** — e.g. East Asia routes run ~92% full in Jul–Aug but ~72% in May.
    * The **%** shown is the load factor used for that flight's origin region this month.

    *Developed by Phillip Yeh to support the BNE Lotte Team.*
    """, unsafe_allow_html=True)

# ── Fetch ──────────────────────────────────────────────────────────────────────
_epoch  = datetime(2000, 1, 1, tzinfo=aest)
anchor  = (_epoch + timedelta(seconds=(int((now_aest - _epoch).total_seconds()) // API_DATA_TTL_SEC) * API_DATA_TTL_SEC)).strftime("%Y-%m-%dT%H:%M")

raw_flights = fetch_flight_data(
    anchor,
    (now_aest - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M"),
    (now_aest + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M"),
)

# OpenSky — one call gets all airborne aircraft in the Brisbane area.
# Used to upgrade "scheduled only" flights with live ADS-B position data.
opensky_data = fetch_opensky_states(anchor)

if st.session_state.api_error:
    st.error(f"⚠️ API Error — {st.session_state.api_error}")
    st.session_state.api_error = None

if not raw_flights:
    st.info("⏳ Synchronizing radar... data will appear on next refresh.")
    st.stop()

# ── Deduplicate ───────────────────────────────────────────────────────────────
seen: dict = {}
for f in raw_flights:
    num = f.get("number")
    if num and num not in seen:
        seen[num] = f
unique_flights = list(seen.values())

# ── Prefetch reg photos concurrently ──────────────────────────────────────────
all_regs = list({
    f.get("aircraft", {}).get("reg", "")
    for f in unique_flights
    if f.get("aircraft", {}).get("reg")
})
with ThreadPoolExecutor(max_workers=IMAGE_WORKERS) as executor:
    executor.map(get_photo_from_api, all_regs)

# ── Process ───────────────────────────────────────────────────────────────────
processed = []

for f in unique_flights:
    flight_num  = f.get("number", "N/A")
    status_raw  = f.get("status", "").lower()
    dep_node    = f.get("departure") or {}
    dep_ap      = dep_node.get("airport") or f.get("movement", {}).get("airport") or {}
    arr         = f.get("arrival") or f.get("movement") or {}
    ac_m        = f.get("aircraft", {}).get("model", "")
    ac_r        = f.get("aircraft", {}).get("reg", "")
    origin_iata = str(dep_ap.get("iata", ""))

    if not is_strictly_international(str(arr.get("terminal", "")), str(dep_ap.get("countryCode", "")), ac_m, origin_iata):
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

    # ── OpenSky fallback — upgrade scheduled-only flights with live ADS-B ─────
    # If AeroDataBox has no radar data, check if OpenSky can see the aircraft.
    if t_type == "scheduled" and status_raw not in ("canceled", "cancelled"):
        osky_dt, osky_type = opensky_estimate_eta(flight_num, opensky_data, now_aest, aest)
        if osky_dt:
            best_dt = osky_dt
            t_type  = osky_type  # "revised" — removes ⚠️ Check Board

    delay = (best_dt - s_dt).total_seconds() / 3600
    if delay < -2 or delay > 24:
        log.info("Skipping %s — implausible delay %.1fh", flight_num, delay)
        continue

    t_diff = int((best_dt - now_aest).total_seconds() / 60)
    is_can = status_raw in ("canceled", "cancelled")

    is_lan = False
    if status_raw in ("landed", "arrived"):
        is_lan = True
    elif t_diff <= 0 and t_type in ("actual", "revised"):
        is_lan = True
    is_lan = is_lan and not is_can

    landed_mins = max(0, -t_diff)

    style = classify_flight_status(
        is_canceled=is_can, is_landed=is_lan, landed_mins=landed_mins,
        t_diff=t_diff, t_type=t_type, delay_hours=delay, s_dt=s_dt, now=now_aest,
    )

    city = CITY_MAP.get(
        dep_ap.get("municipalityName") or dep_ap.get("name"),
        dep_ap.get("municipalityName") or dep_ap.get("name") or "Unknown",
    )

    pax_count, pax_label, pax_color, load_pct = estimate_pax(
        ac_m, str(dep_ap.get("countryCode", "")), now_aest.month, flight_num
    )

    processed.append({
        "num":          flight_num,
        "origin":       city,
        "iata":         origin_iata,
        "gate":         arr.get("gate", "TBA"),
        "ac_model":     ac_m,
        "ac_text":      f"{ac_m} ({ac_r})" if ac_m and ac_r else ac_m or ac_r,
        "actual_time":  best_dt.strftime("%H:%M"),
        "sch_time":     s_dt.strftime("%H:%M"),
        "is_landed":    is_lan,
        "is_canceled":  is_can,
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
        "pax_count":    pax_count,
        "pax_label":    pax_label,
        "pax_color":    pax_color,
        "load_pct":     load_pct,
    })

# ── Gap Detection ─────────────────────────────────────────────────────────────
gap_candidates = sorted(
    [p for p in processed
     if not p["is_canceled"]
     and not (p["is_landed"] and p["landed_mins"] > RECENT_LANDED_MAX)],
    key=lambda x: x["dt"],
)

gap_list = []
if gap_candidates:
    for i in range(len(gap_candidates) - 1):
        t1 = gap_candidates[i]["dt"]
        t2 = gap_candidates[i + 1]["dt"]

        gap_total = int((t2 - t1).total_seconds() / 60)
        if gap_total < GAP_MIN_MINUTES:
            continue

        gap_remaining = int((t2 - max(t1, now_aest)).total_seconds() / 60)
        if gap_remaining < GAP_DISPLAY_MIN:
            continue

        is_active = t1 <= now_aest < t2
        gap_list.append({"t1": t1, "t2": t2, "total": gap_total, "remaining": gap_remaining, "active": is_active})

        cls = "gap-bar gap-active" if is_active else "gap-bar"
        lbl = "🟢 ACTIVE" if is_active else "🔄"
        window_start = max(t1, now_aest) if is_active else t1
        display_min  = gap_remaining if is_active else gap_total

        # ── Progress bar for active gaps ──────────────────────────────────────
        progress_html = ""
        if is_active and gap_total > 0:
            pct_left = max(0, min(100, int(gap_remaining / gap_total * 100)))
            if pct_left > 50:
                bar_color = "#10B981"
            elif pct_left > 25:
                bar_color = "#F59E0B"
            else:
                bar_color = "#EF4444"
            progress_html = (
                f'<div class="gap-progress-track">'
                f'<div class="gap-progress-fill" style="width:{pct_left}%; background:{bar_color};"></div>'
                f'</div>'
            )

        processed.append({
            "is_gap":   True,
            "html":     (
                f'<div class="{cls}">{lbl} {_format_hm(display_min)} GAP '
                f'<span style="opacity:0.6; font-weight:400; margin-left:8px;">'
                f'({window_start.strftime("%H:%M")}–{t2.strftime("%H:%M")})</span>'
                f'{progress_html}</div>'
            ),
            "time_key": t1.timestamp() + 1,
        })

# ── Surge Detection ───────────────────────────────────────────────────────────
future_flights = sorted(
    [p for p in processed if not p.get("is_gap") and not p["is_canceled"] and not p["is_landed"]],
    key=lambda x: x["dt"],
)

surge_used = set()  # indices of flights already claimed by a surge
surge_windows = []
for i, anchor_f in enumerate(future_flights):
    if i in surge_used:
        continue
    cluster = [anchor_f]
    cluster_idx = [i]
    for j in range(i + 1, len(future_flights)):
        if j in surge_used:
            continue
        if (future_flights[j]["dt"] - anchor_f["dt"]).total_seconds() / 60 <= SURGE_WINDOW_MINS:
            cluster.append(future_flights[j])
            cluster_idx.append(j)
        else:
            break
    if len(cluster) >= SURGE_MIN_FLIGHTS:
        surge_used.update(cluster_idx)
        w_start   = cluster[0]["dt"]
        w_end     = cluster[-1]["dt"]
        total_pax = sum(c["pax_count"] for c in cluster)
        pax_note  = f" · ~{total_pax} pax" if total_pax > 0 else ""
        surge_windows.append({
            "start": w_start, "end": w_end,
            "count": len(cluster), "pax": total_pax,
        })
        processed.append({
            "is_surge": True,
            "html": (
                f'<div class="surge-banner">'
                f'<span class="surge-icon">⚡</span>'
                f'SURGE {w_start.strftime("%H:%M")}–{w_end.strftime("%H:%M")} '
                f'({len(cluster)} flights{pax_note})'
                f'</div>'
            ),
            "time_key": w_start.timestamp() - 1,
        })

# ── Summary Strip ─────────────────────────────────────────────────────────────
incoming       = [p for p in processed if not p.get("is_gap") and not p.get("is_surge") and not p["is_canceled"] and not p["is_landed"]]
incoming_count = len(incoming)
total_pax_sum  = sum(p["pax_count"] for p in incoming)

# Next gap
next_gap_txt = "None"
for g in sorted(gap_list, key=lambda x: x["t1"]):
    if g["t2"] > now_aest:
        if g["active"]:
            next_gap_txt = f'<span style="color:#34D399;">NOW ({g["remaining"]}m)</span>'
        else:
            next_gap_txt = f'{g["t1"].strftime("%H:%M")} ({g["total"]}m)'
        break

# Busiest window: slide a 30-min window across incoming flights
busiest_txt = "—"
if len(incoming) >= 2:
    sorted_inc = sorted(incoming, key=lambda x: x["dt"])
    best_count, best_start, best_end = 0, None, None
    for f in sorted_inc:
        window_end = f["dt"] + timedelta(minutes=30)
        count = sum(1 for o in sorted_inc if f["dt"] <= o["dt"] < window_end)
        if count > best_count:
            best_count = count
            best_start = f["dt"]
            # Find the last flight in this window
            last_in_window = max((o["dt"] for o in sorted_inc if f["dt"] <= o["dt"] < window_end), default=f["dt"])
            best_end = last_in_window
    if best_count >= 2 and best_start:
        busiest_txt = f'{best_start.strftime("%H:%M")}–{best_end.strftime("%H:%M")} ({best_count})'

pax_txt = f"~{total_pax_sum}" if total_pax_sum > 0 else "—"

st.markdown(f"""
<div class="summary-strip">
    <div class="s-item"><span class="s-val" style="color:#60A5FA;">{incoming_count}</span>Incoming</div>
    <div class="s-item"><span class="s-val" style="color:#94A3B8;">{pax_txt}</span>Est. Pax</div>
    <div class="s-item"><span class="s-val" style="color:#34D399;">{next_gap_txt}</span>Next Gap</div>
    <div class="s-item"><span class="s-val" style="color:#F59E0B;">{busiest_txt}</span>Busiest</div>
</div>
""", unsafe_allow_html=True)

# ── Sort ──────────────────────────────────────────────────────────────────────
processed.sort(key=lambda p:
    (1, p["time_key"])              if p.get("is_gap") or p.get("is_surge")                     else
    (2, p["s_dt_val"].timestamp())  if p["is_canceled"]                                         else
    (0, -p["dt"].timestamp())       if p["is_landed"] and p["landed_mins"] <= RECENT_LANDED_MAX else
    (2, -p["dt"].timestamp())       if p["is_landed"]                                           else
    (1, p["dt"].timestamp())
)

# ── Render Active Cards ────────────────────────────────────────────────────────
for i, pf in enumerate(processed):
    if pf.get("is_canceled"):
        continue
    if pf.get("is_gap") or pf.get("is_surge"):
        st.markdown(pf["html"], unsafe_allow_html=True)
        continue

    mid       = f"z_{i}"
    has_photo = pf["photo_url"] != "NOT_FOUND"

    img_html = (
        f'<div class="flip-container" style="filter:{pf["img_filter"]};">'
        f'<label for="{mid}" style="cursor:pointer; display:block; width:100%; height:100%;">'
        f'<img src="{pf["logo_url"]}" class="flip-img logo-layer" style="border-color:{pf["border_color"]};"/>'
        f'<img src="{pf["photo_url"]}" class="flip-img photo-layer" style="border-color:{pf["border_color"]};"/>'
        f'</label></div>'
        if has_photo else
        f'<div class="flip-container" style="filter:{pf["img_filter"]};">'
        f'<img src="{pf["logo_url"]}" class="flip-img" style="border-color:{pf["border_color"]}; background:#FFF; padding:4px; object-fit:contain; border-radius:8px;"/>'
        f'</div>'
    )

    tag        = "Act" if (pf["is_landed"] or pf["time_type"] == "actual") else ("Est" if pf["time_type"] == "revised" else "Sch")
    time_color = "#7DD3FC" if tag == "Act" else ("#E2E8F0" if tag == "Est" else "#94A3B8")

    if tag == "Sch" and not pf["is_canceled"]:
        time_display = (
            f'<span class="mono" style="color:#94A3B8;">Sch {pf["sch_time"]}</span>'
            f' <span style="color:#FBBF24; font-size:0.75em; font-weight:700; margin-left:6px;">⚠️ Check Board</span>'
        )
    else:
        time_display = (
            f'<span class="mono" style="color:#94A3B8;">Sch {pf["sch_time"]}</span>'
            f' • <span class="mono" style="color:{time_color}; font-weight:700;">{tag} {pf["actual_time"]}</span>'
        )

    # ── Pax badge ─────────────────────────────────────────────────────────────
    pax_html = ""
    if pf["pax_label"]:
        pax_html = (
            f'<div><span class="pax-badge" style="background:{pf["pax_color"]}22; color:{pf["pax_color"]}; '
            f'border: 1px solid {pf["pax_color"]}44;">'
            f'~{pf["pax_count"]} pax · {pf["load_pct"]}%</span></div>'
        )

    zoom_src = pf["photo_url"] if has_photo else pf["logo_url"]
    gate_cls = "gate-tba" if pf["gate"] == "TBA" else "gate-num"

    st.markdown(f"""
    <div class="flight-card" style="border-left-color:{pf['border_color']}; background-color:{pf['bg_color']}; opacity:{pf['card_opacity']};">
        {img_html}
        <div class="info-col">
            <div style="font-size:1.1em; font-weight:700;">{pf['num']}<span style="font-size:0.7em; color:#94A3B8; margin-left:8px;">{pf['origin']}</span></div>
            <div class="ac-line">{pf['ac_text']}</div>{pax_html}
            <div style="font-size:0.8em; color:#94A3B8;">{time_display}</div>
        </div>
        <div class="status-col">
            <div style="font-size:0.6em; color:#94A3B8; font-weight:700; letter-spacing:1px;">GATE</div>
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

# ── Render Canceled ───────────────────────────────────────────────────────────
cans = sorted([p for p in processed if p.get("is_canceled")], key=lambda x: x["s_dt_val"])
if cans:
    st.markdown(
        "<hr style='margin:15px 0 8px 0; opacity:0.2;'>"
        "<div style='color:#F87171; font-size:0.85em; font-weight:700; margin-bottom:5px;'>❌ Canceled</div>",
        unsafe_allow_html=True,
    )
    for pf in cans:
        img_html = (
            f'<div class="flip-container" style="filter:{pf["img_filter"]};">'
            f'<img src="{pf["logo_url"]}" class="flip-img" style="border-color:{pf["border_color"]}; '
            f'background:#FFF; padding:4px; object-fit:contain; border-radius:8px;"/>'
            f'</div>'
        )
        st.markdown(f"""
        <div class="flight-card" style="border-left-color:{pf['border_color']}; background-color:{pf['bg_color']}; opacity:{pf['card_opacity']};">
            {img_html}
            <div class="info-col">
                <div style="font-size:1em; font-weight:700;">{pf['num']} <span style="font-size:0.75em; color:#94A3B8;">{pf['origin']}</span></div>
                <div style="font-size:0.75em; color:#94A3B8;"><span class="mono">Sch {pf['sch_time']}</span></div>
            </div>
            <div class="status-col">
                <div style="font-size:0.8em; font-weight:700; color:{pf['status_color']};">{pf['status_text']}</div>
            </div>
        </div>""", unsafe_allow_html=True)

st.markdown(
    "<div style='text-align:center; color:#475569; font-size:0.65em; margin-top:20px;'>Dev: Phillip Yeh | V11.3</div>",
    unsafe_allow_html=True,
)

# ── Auto-refresh via JavaScript (reliable unlike <meta refresh>) ──────────────
# components.html() runs in an iframe that actually executes <script> tags.
# window.parent targets the main Streamlit page from inside the iframe.
components.html(f"""
<script>
    setTimeout(function() {{
        window.parent.location.reload();
    }}, {UI_REFRESH_SEC * 1000});
</script>
""", height=0)
