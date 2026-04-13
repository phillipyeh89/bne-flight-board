import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pytz

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
AIRPORT_ICAO       = "YBBN"
TIMEZONE           = "Australia/Brisbane"
LOOKBACK_HOURS     = 4
LOOKAHEAD_HOURS    = 8
RECENT_LANDED_MAX  = 60
GAP_MIN_MINUTES    = 20
GAP_DISPLAY_MIN    = 5
IMAGE_WORKERS      = 10
DOMESTIC_TERMINALS = ('D', 'DOM', 'D-ANC', 'GAT')
SMALL_AIRCRAFT_FILTER = ('BEECH', 'FAIRCHILD', 'CESSNA', 'PIPER', 'PILATUS', 'KING AIR', 'METROLINER')

CITY_MAP = {
    "Lapu-Lapu City": "Cebu", "Denpasar-Bali Island": "Bali",
    "Ho Chi Minh City": "Saigon", "Yaren District": "Nauru",
    "Guangzhou Baiyun": "Guangzhou"
}

UI_REFRESH_SEC          = 60
API_DATA_TTL_SEC        = 600
STALE_DATA_THRESHOLD_MIN = 30

# ─────────────────────────────────────────────
#  PAGE CONFIG & CSS
# ─────────────────────────────────────────────
st.set_page_config(page_title="BNE Pro Arrivals", page_icon="✈️", layout="centered")
st.markdown(f"""
<meta http-equiv="refresh" content="{UI_REFRESH_SEC}">
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=JetBrains+Mono:wght@600&display=swap');
    #MainMenu {{visibility: hidden;}}
    header {{visibility: hidden;}}
    .block-container {{padding-top: 1.5rem; font-family: 'Inter', sans-serif;}}
    .mono {{ font-family: 'JetBrains Mono', monospace; letter-spacing: -0.5px; }}
    @keyframes blink {{ 50% {{ opacity: 0; }} }}
    .stale-warning {{ color: #EF4444 !important; font-weight: 700 !important; animation: blink 1.2s linear infinite; }}
    .avatar-btn {{ cursor: pointer; margin-right: 18px; flex-shrink: 0; display: block; transition: transform 0.2s ease; border-radius: 35px; }}
    .avatar-btn:hover {{ transform: scale(1.08); }}
    .img-zoom-chk:checked + .img-zoom-modal {{ display: flex; }}
    .img-zoom-modal {{ display: none; position: fixed; top:0; left:0; right:0; bottom:0; background: rgba(15,23,42,0.92); z-index: 9999; align-items: center; justify-content: center; backdrop-filter: blur(5px); }}
    .img-zoom-modal img {{ max-width: 90vw; max-height: 80vh; border-radius: 12px; border: 2px solid #475569; object-fit: contain; }}
    .img-zoom-close {{ position: absolute; top:0; left:0; right:0; bottom:0; cursor: pointer; }}
    .close-btn-text {{ position: absolute; top: 20px; right: 30px; color: #F8FAFC; font-size: 3em; cursor: pointer; }}
</style>
""", unsafe_allow_html=True)

if "api_last_hit" not in st.session_state:
    st.session_state.api_last_hit = None


# ─────────────────────────────────────────────
#  IMAGE FETCHING  (reg photo → airline logo → emoji)
# ─────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _get_photo_from_api(reg: str):
    """Fetch registration-specific photo from Planespotters. Cached indefinitely per reg."""
    try:
        url = f"https://api.planespotters.net/pub/photos/reg/{reg}"
        r = requests.get(url, headers={"User-Agent": "BNE-Board-App/1.0"}, timeout=3.0)
        if r.status_code == 200:
            photos = r.json().get("photos", [])
            if photos:
                return {"url": photos[0]["thumbnail_large"]["src"]}
    except:
        pass
    return None


def get_airline_logo_url(flight_number: str) -> str | None:
    """
    Derive IATA airline prefix from flight number and return an avs.io logo URL.
    Handles mixed prefixes like QF, EK, CX etc. Returns None if unparseable.
    """
    if not flight_number:
        return None
    prefix = "".join(c for c in flight_number if c.isalpha())[:2].upper()
    if len(prefix) == 2:
        return f"https://pics.avs.io/200/200/{prefix}.png"
    return None


def fetch_aircraft_image(reg: str, flight_number: str = "") -> str:
    """
    Two-tier lookup:
      1. Registration photo (Planespotters)
      2. Airline logo (avs.io via IATA prefix)
      3. Fallback → "NOT_FOUND" (emoji rendered in card)
    """
    if reg:
        result = _get_photo_from_api(reg)
        if result:
            return result["url"]
    logo = get_airline_logo_url(flight_number)
    return logo if logo else "NOT_FOUND"


def prefetch_images(flights: list):
    """Warm image cache concurrently for all flights in current window."""
    args = [
        (f.get("aircraft", {}).get("reg", ""), f.get("number", ""))
        for f in flights
    ]
    with ThreadPoolExecutor(max_workers=IMAGE_WORKERS) as ex:
        list(ex.map(lambda a: fetch_aircraft_image(*a), args))


# ─────────────────────────────────────────────
#  API DATA  (FIX: stable cache key so TTL actually works)
# ─────────────────────────────────────────────
def _get_cache_anchor(now_aest) -> str:
    """
    Round down to the nearest API_DATA_TTL_SEC interval so the cache key is
    stable for the full TTL window. Without this, from_t/to_t change every
    minute and @st.cache_data never gets a cache hit.
    """
    epoch = datetime(2000, 1, 1, tzinfo=now_aest.tzinfo)
    elapsed_seconds = int((now_aest - epoch).total_seconds())
    floored_seconds = (elapsed_seconds // API_DATA_TTL_SEC) * API_DATA_TTL_SEC
    floored_dt = epoch + timedelta(seconds=floored_seconds)
    return floored_dt.strftime("%Y-%m-%dT%H:%M")


@st.cache_data(ttl=API_DATA_TTL_SEC, show_spinner=False)
def fetch_flight_data(cache_anchor: str, from_time: str, to_time: str) -> list:
    """
    cache_anchor  — stable rounded key; drives TTL correctly.
    from_time/to_time — real window passed to the API.
    """
    url = (
        f"https://aerodatabox.p.rapidapi.com/flights/airports/icao"
        f"/{AIRPORT_ICAO}/{from_time}/{to_time}"
    )
    params = {
        "direction": "Arrival",
        "withCancelled": "true",
        "withCodeshared": "false",
    }
    headers = {
        "X-RapidAPI-Key": st.secrets["X_RAPIDAPI_KEY"],
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        st.session_state.api_last_hit = datetime.now(pytz.timezone(TIMEZONE))
        return r.json().get("arrivals", [])
    except Exception as e:
        st.error(f"API Request Failed: {e}")
        return []


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def format_hm(total_minutes: int) -> str:
    h, m = divmod(total_minutes, 60)
    return f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"


def _parse_local_dt(raw: str | None, tz) -> datetime | None:
    if not raw:
        return None
    try:
        dt = pd.to_datetime(raw).to_pydatetime()
        return tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
    except:
        return None


def extract_best_time(node: dict, tz) -> tuple:
    for key, label in (
        ("actualTime", "actual"),
        ("revisedTime", "revised"),
        ("scheduledTime", "scheduled"),
    ):
        raw = (
            node.get(key).get("local")
            if isinstance(node.get(key), dict)
            else node.get(key + "Local")
        )
        if raw:
            dt = _parse_local_dt(raw, tz)
            if dt:
                return dt, label
    return None, ""


def is_strictly_international(terminal: str, country_code: str, aircraft_model: str) -> bool:
    """
    Returns True only for confirmed international arrivals.

    Terminal notes:
    - Empty terminal ("") means not yet assigned. We let it through to the
      country/aircraft checks — AeroDataBox often omits terminal for pre-arrival
      international flights. This is intentional, not a bug.
    - Norfolk Island (NF) flights: AeroDataBox may return countryCode "au" or "nf".
      If NF flights are ever incorrectly filtered, add "nf" to FORCED_INTERNATIONAL
      below and short-circuit before the country_code == "au" check.
    """
    t  = terminal.strip().upper()
    ac = aircraft_model.upper()

    if t in DOMESTIC_TERMINALS:
        return False
    if country_code == "au":
        return False
    if any(k in ac for k in SMALL_AIRCRAFT_FILTER):
        return False
    return True


def get_card_style(is_canceled, is_archived, is_landed, landed_mins, delay_hours, mins_left):
    if is_canceled:
        return (
            ("#475569", "#94A3B8", "#0F172A", "❌ CANCELED")
            if is_archived
            else ("#EF4444", "#F87171", "#1E293B", "❌ CANCELED")
        )
    if is_landed:
        return (
            ("#10B981", "#34D399", "#1E293B", f"Landed {format_hm(landed_mins)} ago")
            if landed_mins <= RECENT_LANDED_MAX
            else ("#475569", "#94A3B8", "#0F172A", f"Landed {format_hm(landed_mins)} ago")
        )
    bg = "#1E293B"
    if delay_hours >= 12:
        return "#7F1D1D", "#FCA5A5", bg, f"🚨 SEVERE DELAY In {format_hm(mins_left)}"
    if mins_left < 25:
        return "#EF4444", "#F87171", bg, f"🔥 In {format_hm(mins_left)}"
    if delay_hours >= 3:
        return "#EF4444", "#F87171", bg, f"⚠️ HEAVY DELAY In {format_hm(mins_left)}"
    if mins_left <= 60:
        return "#F59E0B", "#FBBF24", bg, f"In {format_hm(mins_left)}"
    return "#3B82F6", "#60A5FA", bg, f"In {format_hm(mins_left)}"


# ─────────────────────────────────────────────
#  CARD RENDERER
# ─────────────────────────────────────────────
def render_flight_card(pf: dict, index: int):
    img_url    = "" if pf["image_url"] == "NOT_FOUND" else pf["image_url"]
    border_col = pf["border_color"]
    is_logo    = "avs.io" in img_url if img_url else False

    if img_url:
        # Reg photos → round (35px); airline logos → square-ish (8px)
        radius = "8px" if is_logo else "35px"
        mid = f"modal_{index}"
        image_element = (
            f'<label for="{mid}" class="avatar-btn">'
            f'<img src="{img_url}" style="width:70px;height:70px;border-radius:{radius};'
            f'object-fit:cover;border:2px solid {border_col};"/></label>'
            f'<input type="checkbox" id="{mid}" class="img-zoom-chk" style="display:none;">'
            f'<div class="img-zoom-modal">'
            f'<label for="{mid}" class="img-zoom-close"></label>'
            f'<label for="{mid}" class="close-btn-text">&times;</label>'
            f'<img src="{img_url}" /></div>'
        )
    else:
        image_element = (
            f'<div style="width:70px;height:70px;border-radius:35px;background:#334155;'
            f'display:flex;align-items:center;justify-content:center;margin-right:18px;'
            f'font-size:1.6em;border:2px solid {border_col};">✈️</div>'
        )

    sch_str = f'<span class="mono">Sch {pf["sch_display"]}</span> • ' if pf["sch_display"] else ""
    check_board_tag = (
        ' <span style="color:#FBBF24; font-size:0.85em; font-weight:700;">⚠️ Check Board</span>'
        if (not pf["is_landed"] and not pf["is_canceled"] and pf["time_type"] == "scheduled")
        else ""
    )

    if pf["is_landed"] or pf["time_type"] == "actual":
        act_html = (
            f'<span class="mono" style="color:#7DD3FC;font-weight:bold;'
            f'background:rgba(14,165,233,0.15);padding:2px 6px;border-radius:4px;">'
            f'Act {pf["actual_time"]}</span>'
        )
    elif pf["time_type"] == "revised":
        act_html = (
            f'<span class="mono" style="color:#E2E8F0;font-weight:bold;'
            f'background:rgba(226,232,240,0.15);padding:2px 6px;border-radius:4px;">'
            f'Est {pf["actual_time"]}</span>{check_board_tag}'
        )
    else:
        act_html = (
            f'<span class="mono" style="color:#94A3B8;font-weight:bold;'
            f'background:rgba(148,163,184,0.15);padding:2px 6px;border-radius:4px;">'
            f'Sch {pf["actual_time"]}</span>{check_board_tag}'
        )
        sch_str = ""

    origin_display = (
        f"{pf['origin']} <span class='mono' style='font-size:0.85em; opacity:0.8;'>({pf['iata']})</span>"
        if pf["iata"]
        else pf["origin"]
    )

    st.markdown(
        f"""<div style="background-color:{pf['bg_color']};border-left:6px solid {border_col};
        border-radius:8px;padding:16px 20px;margin-bottom:12px;display:flex;align-items:center;
        color:white;box-shadow:0 4px 6px rgba(0,0,0,0.15);">
        {image_element}
        <div style="flex-grow:1;">
            <div style="font-size:1.4em;font-weight:700;">{pf['num']}
                <span style="font-size:0.75em;color:#94A3B8;margin-left:8px;">{origin_display}</span>
            </div>
            <div style="font-size:0.85em;color:#CBD5E1;">{pf['ac_text']}</div>
            <div style="font-size:0.85em;color:#CBD5E1;">{sch_str}{act_html}</div>
        </div>
        <div style="text-align:right;min-width:110px;">
            <div style="font-size:0.8em;color:#94A3B8;font-weight:700;">GATE</div>
            <div class="mono" style="font-size:2.6em;font-weight:700;line-height:1;">{pf['gate']}</div>
            <div style="font-size:1.05em;font-weight:700;color:{pf['status_color']};">{pf['status_text']}</div>
        </div></div>""",
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
aest      = pytz.timezone(TIMEZONE)
now_aest  = datetime.now(aest)
from_t    = (now_aest - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M")
to_t      = (now_aest + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M")
anchor    = _get_cache_anchor(now_aest)   # stable key → cache actually works

# ── Header ────────────────────────────────────
c1, c2 = st.columns([2, 1])
with c1:
    st.title("✈️ Arrivals")
with c2:
    st.markdown(
        f'<div style="font-size:0.85em;color:#94A3B8;text-align:center;margin-top:10px;">'
        f'🕒 Live: {now_aest.strftime("%H:%M:%S")}</div>',
        unsafe_allow_html=True,
    )
    api_t    = st.session_state.get("api_last_hit")
    is_stale = api_t and (now_aest - api_t).total_seconds() / 60 > STALE_DATA_THRESHOLD_MIN
    api_html = (
        f'<span class="stale-warning">API: {api_t.strftime("%H:%M")} (STALE)</span>'
        if is_stale
        else f'API: {api_t.strftime("%H:%M") if api_t else "--:--"}'
    )
    st.markdown(
        f'<div style="font-size:0.75em;color:#64748B;text-align:center;">{api_html}</div>',
        unsafe_allow_html=True,
    )

# ── Fetch ─────────────────────────────────────
flights = fetch_flight_data(anchor, from_t, to_t)
if not flights:
    st.info("No data available. Re-checking...")
    st.stop()

# ── Deduplicate: keep FIRST occurrence per flight number (FIX) ──────────────
seen: dict = {}
for f in flights:
    num = f.get("number")
    if num and num not in seen:
        seen[num] = f
unique_flights = list(seen.values())

prefetch_images(unique_flights)

# ── Process ───────────────────────────────────
processed_flights = []

for f in unique_flights:
    flight_num = f.get("number", "N/A")
    status     = f.get("status", "").lower()

    dep  = f.get("departure", {})
    ai   = dep.get("airport") or f.get("movement", {}).get("airport") or {}
    city = CITY_MAP.get(
        ai.get("municipalityName") or ai.get("name") or "Unknown",
        ai.get("municipalityName") or ai.get("name") or "Unknown",
    )
    iata    = ai.get("iata", "")
    country = str(ai.get("countryCode", "")).strip().lower()

    arr_n = f.get("arrival") or f.get("movement") or {}
    term  = str(arr_n.get("terminal", "")).strip().upper()
    gate  = arr_n.get("gate", "TBA")

    ac   = f.get("aircraft", {})
    ac_m = ac.get("model", "")
    ac_r = ac.get("reg", "")

    if not is_strictly_international(term, country, ac_m):
        continue

    best_dt, t_type = extract_best_time(arr_n, aest)
    if not best_dt:
        continue

    s_dt = (
        _parse_local_dt((arr_n.get("scheduledTime", {}) or {}).get("local"), aest)
        or best_dt
    )
    sch_disp    = s_dt.strftime("%H:%M") if (arr_n.get("scheduledTime", {}) or {}).get("local") else ""
    delay_hours = (best_dt - s_dt).total_seconds() / 3600 if s_dt else 0

    if delay_hours < -2 or delay_hours > 24:
        continue

    t_diff  = int((best_dt - now_aest).total_seconds() / 60)
    is_can  = status in ("canceled", "cancelled")
    is_lan  = (status in ("landed", "arrived") or t_diff <= 0) and not is_can
    l_min   = max(0, -t_diff) if is_lan else 0
    m_left  = max(0, t_diff) if not is_lan else 0

    bc, sc, bg, st_txt = get_card_style(
        is_can,
        (is_can and bool(s_dt) and (now_aest - s_dt).total_seconds() / 60 > 15),
        is_lan,
        l_min,
        delay_hours,
        m_left,
    )

    # Pass flight_number to image fetcher for logo fallback
    img_url = fetch_aircraft_image(ac_r, flight_num)

    processed_flights.append({
        "num":         flight_num,
        "origin":      city,
        "iata":        iata,
        "sch_display": sch_disp,
        "ac_text":     f"{ac_m} ({ac_r})" if ac_m and ac_r else ac_m or ac_r,
        "gate":        gate,
        "actual_time": best_dt.strftime("%H:%M"),
        "is_landed":   is_lan,
        "is_canceled": is_can,
        "landed_mins": l_min,
        "dt":          best_dt,
        "s_dt_val":    s_dt,
        "time_type":   t_type,
        "image_url":   img_url,
        "border_color": bc,
        "status_color": sc,
        "status_text":  st_txt,
        "bg_color":     bg,
        "is_next_day":  best_dt.date() > now_aest.date(),
    })

# ── Gap Detection ─────────────────────────────
future_f = sorted(
    [p for p in processed_flights if not p["is_landed"] and not p["is_canceled"]],
    key=lambda x: x["dt"],
)

if future_f:
    windows = [(now_aest, future_f[0]["dt"])]
    for i in range(len(future_f) - 1):
        windows.append((future_f[i]["dt"], future_f[i + 1]["dt"]))

    for t1, t2 in windows:
        if t2 <= now_aest:
            continue
        ds    = max(t1, now_aest)
        g_min = int((t2 - ds).total_seconds() / 60)
        if (t2 - t1).total_seconds() / 60 < GAP_MIN_MINUTES or g_min < GAP_DISPLAY_MIN:
            continue
        act = t1 <= now_aest
        tit = (
            f"🟢 ACTIVE OFF-FLOOR ({format_hm(g_min)} left)"
            if act
            else f"🔄 {format_hm(g_min)} OFF-FLOOR WINDOW"
        )
        gb, gbo, gc = (
            ("#064E3B", "#10B981", "#A7F3D0") if act else ("#0F172A", "#475569", "#94A3B8")
        )
        processed_flights.append({
            "is_gap":   True,
            "html":     (
                f'<div style="background-color:{gb};border:1px dashed {gbo};border-radius:8px;'
                f'padding:10px;margin-bottom:12px;text-align:center;color:{gc};font-weight:bold;">'
                f'{tit} <span style="opacity:0.7;font-weight:normal;">'
                f'({ds.strftime("%H:%M")}–{t2.strftime("%H:%M")})</span></div>'
            ),
            "time_key": t1.timestamp() + 1,
        })

# ── Sort ──────────────────────────────────────
def s_key(p):
    if p.get("is_gap"):
        return (1, p["time_key"])
    if p["is_canceled"]:
        return (2, p["s_dt_val"].timestamp())
    if p.get("is_landed") and p["landed_mins"] <= RECENT_LANDED_MAX:
        return (0, -p["dt"].timestamp())
    if p.get("is_landed"):
        return (2, -p["dt"].timestamp())
    return (1, p["dt"].timestamp())


processed_flights.sort(key=s_key)
active_f   = [f for f in processed_flights if not f.get("is_canceled")]
canceled_f = sorted(
    [f for f in processed_flights if f.get("is_canceled")],
    key=lambda x: x["s_dt_val"],
)

# ── Render ────────────────────────────────────
for i, pf in enumerate(active_f):
    if pf.get("is_gap"):
        st.markdown(pf["html"], unsafe_allow_html=True)
    else:
        render_flight_card(pf, i)

if canceled_f:
    st.markdown(
        "<hr style='margin:40px 0 20px 0; opacity:0.3;'>"
        "<h4 style='color:#F87171;'>❌ Canceled (Current Window)</h4>",
        unsafe_allow_html=True,
    )
    offset = len(active_f)
    for i, pf in enumerate(canceled_f):
        render_flight_card(pf, offset + i)

st.markdown(
    "<div style='text-align:center; color:#475569; font-size:0.8em; margin-top:50px;'>"
    "Developer: Phillip Yeh | V8.0</div>",
    unsafe_allow_html=True,
)
