import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import requests
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
IMMINENT_MINS            = 30   # red "hot" threshold (25 + 5 lag compensation)
API_LAG_MINS             = 5    # AeroDataBox data is ~5 min behind real-time
OPENSKY_PREFER_UNDER_MIN = 60   # use OpenSky over AeroDataBox for flights < 60 min out
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

# ── OpenSky Network — free ADS-B radar supplement ────────────────────────────
YBBN_LAT, YBBN_LON = -27.3842, 153.1175
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
    "WEB": "WEB",
}

UI_REFRESH_SEC           = 60
API_DATA_TTL_SEC         = 600
STALE_DATA_THRESHOLD_MIN = 30

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bne-board")

# ─────────────────────────────────────────────
#  2. STATUS CLASSIFICATION
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
    if is_canceled:
        archived = (now - s_dt).total_seconds() / 60 > 15
        if archived:
            return FlightStyle("#475569", "#94A3B8", "#0F172A", "CANCELED", "0.5", "grayscale(100%)")
        return FlightStyle("#EF4444", "#F87171", "#1E293B", "CANCELED", "0.5", "grayscale(100%)")

    if is_landed:
        if landed_mins <= RECENT_LANDED_MAX:
            return FlightStyle("#059669", "#34D399", "#0F172A",
                               f"Landed {format_hm(landed_mins)} ago", "0.75", "grayscale(40%)")
        return FlightStyle("#475569", "#94A3B8", "#0F172A",
                           f"Landed {format_hm(landed_mins)} ago", "0.4", "grayscale(80%)")

    m_left     = max(0, t_diff)
    delay_mins = max(0, int(round(delay_hours * 60)))

    if t_type == "scheduled" and t_diff <= 0:
        return FlightStyle("#F59E0B", "#FBBF24", "#0F172A", "NO UPDATE", "1.0", "none")
    if m_left < IMMINENT_MINS:
        return FlightStyle("#EF4444", "#F87171", "#1E293B",
                           f"In {format_hm(m_left)}", "1.0", "none")
    if delay_hours >= SEVERE_DELAY_HOURS:
        return FlightStyle("#7F1D1D", "#FCA5A5", "#1E293B",
                           f"🔴 +{format_hm(delay_mins)} Late", "1.0", "none")
    if delay_hours >= HEAVY_DELAY_HOURS:
        return FlightStyle("#92400E", "#FBBF24", "#1E293B",
                           f"🟠 +{format_hm(delay_mins)} Late", "1.0", "none")
    return FlightStyle("#3B82F6", "#60A5FA", "#1E293B",
                       f"In {format_hm(m_left)}", "1.0", "none")

# ─────────────────────────────────────────────
#  3. CORE LOGIC
# ─────────────────────────────────────────────
def format_hm(total_minutes: int) -> str:
    h, m = divmod(total_minutes, 60)
    return f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"

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

def is_strictly_international(terminal: str, country_code: str, aircraft_model: str, origin_iata: str, reg: str = "") -> bool:
    t    = terminal.strip().upper()
    ac   = aircraft_model.upper()
    cc   = country_code.lower()
    iata = origin_iata.upper()
    rv   = reg.strip().upper()

    if iata == "NLK":                                    return True
    if t in DOMESTIC_TERMINALS:                          return False
    if cc == "au":                                       return False
    # VH- registration = Australian-registered aircraft — strong domestic signal
    if rv.startswith("VH-") and not cc:                  return False
    # No origin data at all = unproven, treat as domestic
    if not cc and not iata:                              return False
    if any(k in ac for k in SMALL_AIRCRAFT_FILTER):      return False
    return True

def get_airline_logo_url(flight_number: str) -> str:
    prefix = "".join(c for c in flight_number if c.isalpha())[:2].upper()
    return f"https://pics.avs.io/200/200/{prefix}.png" if len(prefix) == 2 else ""

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
    if not reg: return "NOT_FOUND"
    cached = _photo_cache_permanent(reg)
    if cached != "NOT_FOUND": return cached

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

def _iata_to_callsign(flight_number: str) -> str:
    prefix = "".join(c for c in flight_number if c.isalpha())[:2].upper()
    digits = "".join(c for c in flight_number if c.isdigit())
    icao = AIRLINE_ICAO.get(prefix, prefix)
    return f"{icao}{digits}"

def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3440.065
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))

@st.cache_data(ttl=API_DATA_TTL_SEC, show_spinner=False)
def fetch_opensky_states(anchor: str) -> dict:
    if not OPENSKY_ENABLED: return {}
    try:
        r = requests.get("https://opensky-network.org/api/states/all", params=OPENSKY_BBOX,
                         headers={"User-Agent": "BNE-Board-App/2.0"}, timeout=8)
        if r.status_code == 200:
            states = r.json().get("states") or []
            result = {}
            for s in states:
                callsign = (s[1] or "").strip().upper()
                on_ground = s[8]
                velocity = s[9]
                if callsign and not on_ground and velocity:
                    result[callsign] = {
                        "lat": s[6], "lon": s[5],
                        "velocity_kts": velocity * 1.94384,
                        "altitude_ft": (s[7] or 0) * 3.281,
                    }
            return result
    except Exception as e:
        log.warning("OpenSky query failed: %s", e)
    return {}

def opensky_estimate_eta(flight_number: str, opensky_data: dict, now: datetime, tz) -> tuple:
    if not opensky_data: return None, ""
    callsign = _iata_to_callsign(flight_number)
    state = opensky_data.get(callsign)
    if not state or state["velocity_kts"] < OPENSKY_MIN_SPEED_KT: return None, ""

    dist_nm  = _haversine_nm(state["lat"], state["lon"], YBBN_LAT, YBBN_LON)
    eta_min  = int(dist_nm / state["velocity_kts"] * 60)

    if eta_min < 1 or eta_min > OPENSKY_MAX_ETA_MIN: return None, ""
    return now + timedelta(minutes=eta_min), "revised"

# ─────────────────────────────────────────────
#  4. UI SETUP & CSS  (V11.8)
# ─────────────────────────────────────────────
st.set_page_config(page_title="BNE Pro Arrivals", page_icon="✈️", layout="centered")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=JetBrains+Mono:wght@600&display=swap');
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    .block-container {padding-top: 1rem; font-family: 'Inter', sans-serif; max-width: 700px;}
    .mono { font-family: 'JetBrains Mono', monospace; letter-spacing: -0.5px; }

    .flip-container { position: relative; width: 55px; height: 55px; margin-right: 12px; flex-shrink: 0; }
    .flip-img { position: absolute; top: 0; left: 0; width: 55px; height: 55px; border-radius: 8px; border: 2.5px solid #475569; transition: opacity 1s ease-in-out; box-sizing: border-box; }

    @keyframes logoFade  { 0%, 45% { opacity: 1; } 55%, 100% { opacity: 0; } }
    @keyframes photoFade { 0%, 45% { opacity: 0; } 55%, 95%  { opacity: 1; } 100% { opacity: 0; } }

    .logo-layer  { animation: logoFade 10s infinite;  background: #FFFFFF; padding: 4px; object-fit: contain !important; border-radius: 8px; z-index: 2; }
    .photo-layer { animation: photoFade 10s infinite; object-fit: cover !important;   z-index: 1; }

    .flight-card {
        border-radius: 10px; padding: 10px 14px;
        margin-bottom: 8px; display: flex; align-items: center; color: white;
        box-shadow: 0 4px 10px rgba(0,0,0,0.2); border-left: 5px solid #3B82F6;
        transition: opacity 0.3s ease;
    }
    .info-col   { flex-grow: 1; min-width: 0; overflow: hidden; }
    .info-col .ac-line { font-size: 0.7em; color: #CBD5E1; margin: 1px 0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .status-col { text-align: right; min-width: 110px; display: flex; flex-direction: column; justify-content: center; }
    .gate-num   { font-size: 1.85em; font-weight: 700; line-height: 1; }
    .gate-tba   { font-size: 1.85em; font-weight: 700; line-height: 1; opacity: 0.35; }

    .summary-strip {
        display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
        background: #0F172A; border: 1px solid #1E293B; border-radius: 8px;
        padding: 10px 14px; margin-bottom: 10px; font-size: 0.78em; color: #94A3B8;
        gap: 4px 0;
    }
    .summary-strip .s-item { text-align: center; min-width: 30%; }
    .summary-strip .s-val  { font-weight: 700; font-size: 1.15em; display: block; }

    .gap-bar {
        background-color: #0F172A; border: 1px dashed #475569; border-left: 5px solid transparent;
        border-radius: 8px; padding: 8px 14px; margin: 4px 0 10px 0; text-align: center; color: #94A3B8;
        font-weight: 600; font-size: 0.85em; box-sizing: border-box;
    }
    .gap-active { background-color: #064E3B; border-color: #10B981; border-left-color: #10B981; color: #A7F3D0; }

    .gap-progress-track {
        width: 100%; height: 5px; background: #1E293B; border-radius: 3px; margin-top: 6px; overflow: hidden;
    }
    .gap-progress-fill {
        height: 100%; border-radius: 3px; transition: width 1s linear;
    }

    .surge-banner {
        background: linear-gradient(90deg, #7F1D1D 0%, #991B1B 100%);
        border-left: 5px solid #EF4444; border-radius: 8px;
        padding: 7px 14px; margin: 6px 0 8px 0; color: #FCA5A5;
        font-size: 0.82em; font-weight: 700; display: flex;
        align-items: center; gap: 8px;
    }
    .surge-banner .surge-icon { font-size: 1.1em; }

    .img-zoom-modal {
        display: none; position: fixed; top: 0; left: 0; width: 100vw; height: 100vh;
        background: rgba(15,23,42,0.92); z-index: 10000; align-items: center;
        justify-content: center; backdrop-filter: blur(10px);
    }
    .img-zoom-chk:checked + .img-zoom-modal { display: flex !important; }
    .img-zoom-modal img { max-width: 90%; max-height: 80%; border-radius: 12px; border: 2px solid #475569; object-fit: contain; z-index: 10001; }
    .img-zoom-close-bg { position: absolute; top: 0; left: 0; width: 100%; height: 100%; cursor: pointer; z-index: 10000; }
    .close-btn { position: absolute; top: 20px; right: 30px; color: white; font-size: 3.5em; font-weight: bold; cursor: pointer; z-index: 10002; line-height: 1; }
</style>
""", unsafe_allow_html=True)

if "api_last_hit" not in st.session_state: st.session_state.api_last_hit = None
if "api_error"    not in st.session_state: st.session_state.api_error    = None

# ─────────────────────────────────────────────
#  5. FRAGMENT EXECUTION (SEAMLESS REFRESH)
# ─────────────────────────────────────────────
@st.fragment(run_every="60s")
def live_dashboard():
    aest     = pytz.timezone(TIMEZONE)
    now_aest = datetime.now(aest)

    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader("✈️ Arrivals")
    with c2:
        st.markdown(
            f'<div style="font-size:0.8em;color:#94A3B8;text-align:right;margin-top:5px;">'
            f'🕒 Live: <span id="bne-live-clock">{now_aest.strftime("%H:%M:%S")}</span></div>',
            unsafe_allow_html=True,
        )
        api_t   = st.session_state.get("api_last_hit")
        api_txt = f'API: {api_t.strftime("%H:%M")} <span style="color:#F59E0B;">(~{API_LAG_MINS}m lag)</span>' if api_t else "API: --:--"
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

    opensky_data = fetch_opensky_states(anchor)

    if st.session_state.api_error:
        st.error(f"⚠️ API Error — {st.session_state.api_error}")
        st.session_state.api_error = None

    if not raw_flights:
        st.info("⏳ Synchronizing radar... data will appear on next refresh.")
        return

    seen: dict = {}
    for f in raw_flights:
        num = f.get("number")
        if num and num not in seen:
            seen[num] = f
    unique_flights = list(seen.values())

    all_regs = list({
        f.get("aircraft", {}).get("reg", "")
        for f in unique_flights
        if f.get("aircraft", {}).get("reg")
    })

    with ThreadPoolExecutor(max_workers=IMAGE_WORKERS) as executor:
        executor.map(get_photo_from_api, all_regs)

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

        if not is_strictly_international(str(arr.get("terminal", "")), str(dep_ap.get("countryCode", "")), ac_m, origin_iata, ac_r):
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

        # ── OpenSky radar supplement ───────────────────────────────────────────
        # Two cases where OpenSky's live ADS-B position beats AeroDataBox:
        # 1. Scheduled-only: AeroDataBox has no radar at all → use OpenSky
        # 2. Close-in flights (< 60 min): AeroDataBox data is ~5 min stale,
        #    but OpenSky updates every ~10 sec → prefer live position math
        if status_raw not in ("canceled", "cancelled"):
            preliminary_mins = int((best_dt - now_aest).total_seconds() / 60)
            use_opensky = (
                t_type == "scheduled"
                or (t_type == "revised" and 0 < preliminary_mins < OPENSKY_PREFER_UNDER_MIN)
            )
            if use_opensky:
                osky_dt, osky_type = opensky_estimate_eta(flight_num, opensky_data, now_aest, aest)
                if osky_dt:
                    best_dt = osky_dt
                    t_type  = osky_type

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

            # Subtract API lag from remaining time — flight could arrive ~5 min
            # earlier than shown, so a "20m gap" is really ~15m usable.
            safe_remaining = max(0, gap_remaining - API_LAG_MINS)
            gap_list.append({"t1": t1, "t2": t2, "total": gap_total, "remaining": safe_remaining, "active": is_active})

            cls = "gap-bar gap-active" if is_active else "gap-bar"
            lbl = "🟢 ACTIVE" if is_active else "🔄"
            window_start = max(t1, now_aest) if is_active else t1
            display_min  = safe_remaining if is_active else max(0, gap_total - API_LAG_MINS)

            progress_html = ""
            if is_active and gap_total > 0:
                pct_left = max(0, min(100, int(safe_remaining / gap_total * 100)))
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
                    f'<div class="{cls}">{lbl} {format_hm(display_min)} GAP '
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

    surge_used = set()
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
            surge_windows.append({
                "start": w_start, "end": w_end,
                "count": len(cluster),
            })
            processed.append({
                "is_surge": True,
                "html": (
                    f'<div class="surge-banner">'
                    f'<span class="surge-icon">⚡</span>'
                    f'SURGE {w_start.strftime("%H:%M")}–{w_end.strftime("%H:%M")} '
                    f'({len(cluster)} flights)'
                    f'</div>'
                ),
                "time_key": w_start.timestamp() - 1,
            })

    # ── Summary Strip ─────────────────────────────────────────────────────────────
    incoming       = [p for p in processed if not p.get("is_gap") and not p.get("is_surge") and not p["is_canceled"] and not p["is_landed"]]
    incoming_count = len(incoming)

    next_gap_txt = "None"
    for g in sorted(gap_list, key=lambda x: x["t1"]):
        if g["t2"] > now_aest:
            if g["active"]:
                next_gap_txt = f'<span style="color:#34D399;">NOW ({g["remaining"]}m)</span>'
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
                last_in_window = max((o["dt"] for o in sorted_inc if f_item["dt"] <= o["dt"] < window_end), default=f_item["dt"])
                best_end = last_in_window
        if best_count >= 2 and best_start:
            busiest_txt = f'{best_start.strftime("%H:%M")}–{best_end.strftime("%H:%M")} ({best_count})'

    st.markdown(f"""
    <div class="summary-strip">
        <div class="s-item"><span class="s-val" style="color:#60A5FA;">{incoming_count}</span>Incoming</div>
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

        zoom_src = pf["photo_url"] if has_photo else pf["logo_url"]
        gate_cls = "gate-tba" if pf["gate"] == "TBA" else "gate-num"

        st.markdown(f"""
        <div class="flight-card" style="border-left-color:{pf['border_color']}; background-color:{pf['bg_color']}; opacity:{pf['card_opacity']};">
            {img_html}
            <div class="info-col">
                <div style="font-size:1.1em; font-weight:700;">{pf['num']}<span style="font-size:0.7em; color:#94A3B8; margin-left:8px;">{pf['origin']}</span></div>
                <div class="ac-line">{pf['ac_text']}</div>
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
        "<div style='text-align:center; color:#475569; font-size:0.65em; margin-top:20px;'>Dev: Phillip Yeh | V11.8</div>",
        unsafe_allow_html=True,
    )

# ── Call the fragment ──
live_dashboard()

# ── JavaScript Live Clock (V11.7 — uses real AEST, no drift) ──
# The V11.6 clock incremented from the last displayed value, which drifts on
# mobile browsers where setInterval(1000) is throttled. This version uses the
# browser's real Date() converted to AEST via Intl.DateTimeFormat — it's always
# correct regardless of the user's timezone or timer drift.
components.html("""
<script>
    const doc = window.parent.document;
    const aestFormatter = new Intl.DateTimeFormat('en-AU', {
        timeZone: 'Australia/Brisbane',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
        hour12: false
    });
    setInterval(function() {
        const clockEl = doc.getElementById('bne-live-clock');
        if (clockEl) {
            clockEl.innerText = aestFormatter.format(new Date());
        }
    }, 1000);
</script>
""", height=0)
