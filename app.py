import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, time
from concurrent.futures import ThreadPoolExecutor
import pytz

# ─────────────────────────────────────────────
#  Constants  (no more magic numbers)
# ─────────────────────────────────────────────
AIRPORT_ICAO      = "YBBN"
TIMEZONE          = "Australia/Brisbane"
LOOKBACK_HOURS    = 1
LOOKAHEAD_HOURS   = 11
GAP_MIN_MINUTES   = 20      # minimum gap length to display
GAP_DISPLAY_MIN   = 5       # suppress gap card if remaining time < this
RECENT_LANDED_MAX = 60      # minutes: landed cards stay "active" (green)
IMMINENT_MINUTES  = 25      # < 25 min → red "fire" status
SOON_MINUTES      = 60      # < 60 min → amber status
OLD_FLIGHT_HOURS  = 8       # hide scheduled flights whose sch time is > 8 h ago
PREP_START        = time(2, 30)   # early-morning prep window start
PREP_END          = time(4, 10)   # early-morning prep window end
IMAGE_WORKERS     = 8             # concurrent threads for photo prefetch
DOMESTIC_TERMINALS = ('D', 'DOM')

# ─────────────────────────────────────────────
#  Page config & CSS
# ─────────────────────────────────────────────
st.set_page_config(page_title="BNE Flight Board", page_icon="✈️", layout="centered")
st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    header    {visibility: hidden;}
    .block-container {padding-top: 2rem;}

    .avatar-btn {
        cursor: pointer; margin-right: 18px; flex-shrink: 0;
        display: block; transition: transform 0.2s ease, box-shadow 0.2s ease;
        border-radius: 35px;
    }
    .avatar-btn:hover { transform: scale(1.08); box-shadow: 0 0 15px rgba(255,255,255,0.3); }

    .img-zoom-chk:checked + .img-zoom-modal { display: flex; }
    .img-zoom-modal {
        display: none; position: fixed; top:0; left:0; right:0; bottom:0;
        background: rgba(15,23,42,0.92); z-index: 999999;
        align-items: center; justify-content: center; backdrop-filter: blur(5px);
    }
    .img-zoom-modal img {
        max-width: 90vw; max-height: 80vh; border-radius: 12px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.6); border: 2px solid #475569;
        object-fit: contain;
    }
    .img-zoom-close { position: absolute; top:0; left:0; right:0; bottom:0; cursor: pointer; }
    .close-btn-text {
        position: absolute; top: 20px; right: 30px;
        color: #F8FAFC; font-size: 3em; font-weight: bold;
        cursor: pointer; z-index: 1000000; line-height: 1;
        text-shadow: 0 2px 4px rgba(0,0,0,0.5);
    }
    .close-btn-text:hover { color: #EF4444; }
</style>
""", unsafe_allow_html=True)

if "last_update_time" not in st.session_state:
    st.session_state.last_update_time = None

# ─────────────────────────────────────────────
#  Data helpers
# ─────────────────────────────────────────────
@st.cache_data(ttl=86400, show_spinner=False)
def fetch_aircraft_image(reg: str) -> str:
    """Return thumbnail URL for a registration, or '' on any failure."""
    if not reg:
        return ""
    try:
        url = f"https://api.planespotters.net/pub/photos/reg/{reg}"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=3)
        r.raise_for_status()
        photos = r.json().get("photos", [])
        if photos:
            return photos[0]["thumbnail_large"]["src"]
    except Exception:
        pass
    return ""


def prefetch_images(flights: list):
    """Warm the image cache for all registrations in parallel."""
    regs = [f.get("aircraft", {}).get("reg", "") for f in flights]
    with ThreadPoolExecutor(max_workers=IMAGE_WORKERS) as ex:
        list(ex.map(fetch_aircraft_image, regs))


@st.cache_data(ttl=60)
def fetch_flight_data(from_time: str, to_time: str) -> list:
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{AIRPORT_ICAO}/{from_time}/{to_time}"
    params  = {"direction": "Arrival", "withCancelled": "true", "withCodeshared": "false"}
    headers = {
        "X-RapidAPI-Key":  st.secrets["X_RAPIDAPI_KEY"],
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        st.session_state.last_update_time = datetime.now(pytz.timezone(TIMEZONE))
        return r.json().get("arrivals", [])
    except Exception as e:
        st.error(f"API Request Failed: {e}")
        return []


# ─────────────────────────────────────────────
#  Parsing helpers
# ─────────────────────────────────────────────
def format_hm(total_minutes: int) -> str:
    h, m = divmod(total_minutes, 60)
    return f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"


def _parse_local_dt(raw: str | None, tz) -> datetime | None:
    """Parse a local datetime string and attach timezone. Returns None on failure."""
    if not raw:
        return None
    try:
        dt = pd.to_datetime(raw).to_pydatetime()
        return tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
    except Exception:
        return None


def extract_best_time(node: dict, tz) -> datetime | None:
    """
    Return the most accurate time from a flight node.
    Priority: actualTime > revisedTime > scheduledTime
    """
    for key in ("actualTime", "revisedTime", "scheduledTime"):
        raw = node.get(key)
        if isinstance(raw, dict):
            raw = raw.get("local")
        if raw:
            dt = _parse_local_dt(raw, tz)
            if dt:
                return dt
    return None


def is_domestic(terminal: str, country_code: str) -> bool:
    """
    Terminal is the authoritative source; country code is a fallback
    only when there is no terminal string at all.
    """
    t = terminal.strip().upper()
    if t in DOMESTIC_TERMINALS:
        return True
    if not t and country_code == "au":
        return True
    return False


def get_card_style(is_canceled, is_archived_canceled, is_landed,
                   landed_mins, is_delayed, is_early_prep, minutes_left):
    """Return (border_color, status_color, bg_color, status_text)."""
    if is_canceled:
        if is_archived_canceled:
            return "#475569", "#94A3B8", "#0F172A", "❌ CANCELED"
        return "#EF4444", "#F87171", "#1E293B", "❌ CANCELED"

    if is_landed:
        if landed_mins <= RECENT_LANDED_MAX:
            return "#10B981", "#34D399", "#1E293B", f"Landed {format_hm(landed_mins)} ago"
        return "#475569", "#94A3B8", "#0F172A", f"Landed {format_hm(landed_mins)} ago"

    delay_icon = "⚠️ " if is_delayed else ""
    bg = "#1E293B"
    if is_early_prep:
        return "#8B5CF6", "#A78BFA", bg, f"⏰ {delay_icon}In {format_hm(minutes_left)} (Prep)"
    if minutes_left < IMMINENT_MINUTES:
        return "#EF4444", "#F87171", bg, f"🔥 {delay_icon}In {format_hm(minutes_left)}"
    if minutes_left <= SOON_MINUTES:
        return "#F59E0B", "#FBBF24", bg, f"{delay_icon}In {format_hm(minutes_left)}"
    return "#3B82F6", "#60A5FA", bg, f"{delay_icon}In {format_hm(minutes_left)}"


# ─────────────────────────────────────────────
#  Flight card & gap renderers
# ─────────────────────────────────────────────
def render_flight_card(pf: dict, index: int):
    img_url     = pf["image_url"]
    border_col  = pf["border_color"]

    if img_url:
        mid = f"modal_{index}"
        image_element = f"""<label for="{mid}" class="avatar-btn">
<img src="{img_url}" style="width:70px;height:70px;border-radius:35px;
     object-fit:cover;border:2px solid {border_col};display:block;" />
</label>
<input type="checkbox" id="{mid}" class="img-zoom-chk" style="display:none;">
<div class="img-zoom-modal">
  <label for="{mid}" class="img-zoom-close"></label>
  <label for="{mid}" class="close-btn-text">&times;</label>
  <img src="{img_url}" />
</div>"""
    else:
        image_element = (
            f'<div style="width:70px;height:70px;border-radius:35px;background:#334155;'
            f'display:flex;align-items:center;justify-content:center;margin-right:18px;'
            f'font-size:1.6em;border:2px solid {border_col};flex-shrink:0;">✈️</div>'
        )

    sch_str  = f"Sch {pf['sch_display']} • " if pf["sch_display"] else ""
    act_html = (
        "" if pf["is_canceled"] else
        f'<span style="color:#7DD3FC;font-weight:bold;background:rgba(14,165,233,0.15);'
        f'padding:2px 6px;border-radius:4px;border:1px solid rgba(14,165,233,0.3);">'
        f'Act {pf["actual_time"]}</span>'
    )

    card_html = f"""<div style="background-color:{pf['bg_color']};border-left:6px solid {border_col};
border-radius:8px;padding:16px 20px;margin-bottom:12px;display:flex;
align-items:center;color:white;font-family:sans-serif;box-shadow:0 4px 6px rgba(0,0,0,0.15);">
  {image_element}
  <div style="flex-grow:1;">
    <div style="font-size:1.4em;font-weight:bold;margin-bottom:4px;">
      {pf['num']}
      <span style="font-size:0.75em;color:#94A3B8;font-weight:normal;margin-left:6px;">{pf['origin']}</span>
    </div>
    <div style="font-size:0.85em;color:#CBD5E1;margin-bottom:6px;">{pf['ac_text']}</div>
    <div style="font-size:0.85em;color:#CBD5E1;">{sch_str}{act_html}</div>
  </div>
  <div style="text-align:right;min-width:110px;">
    <div style="font-size:0.8em;color:#94A3B8;text-transform:uppercase;font-weight:bold;letter-spacing:0.05em;">Gate</div>
    <div style="font-size:2.6em;font-weight:bold;line-height:1;margin-top:4px;">{pf['gate']}</div>
    <div style="font-size:1.05em;font-weight:bold;color:{pf['status_color']};margin-top:6px;">{pf['status_text']}</div>
  </div>
</div>"""
    st.markdown(card_html, unsafe_allow_html=True)


def render_gap_card(pf: dict):
    st.markdown(pf["html"], unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────
aest        = pytz.timezone(TIMEZONE)
now_aest    = datetime.now(aest)
from_time   = (now_aest - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M")
to_time     = (now_aest + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M")

# ── Header ──────────────────────────────────
col1, col2 = st.columns([2, 1])
with col1:
    st.title("✈️ Arrivals")
with col2:
    upd = st.session_state.last_update_time
    upd_str = upd.strftime("%H:%M:%S") if upd else "Just Now"
    st.markdown(
        f'<div style="font-size:0.85em;color:#94A3B8;text-align:center;margin-bottom:8px;">'
        f'🕒 Last Updated: {upd_str}</div>',
        unsafe_allow_html=True,
    )
    if st.button("🔄 Refresh", use_container_width=True):
        fetch_flight_data.clear()
        st.rerun()

# ── Fetch & prefetch ─────────────────────────
flights = fetch_flight_data(from_time, to_time)
if not flights:
    st.warning("No flight data available. Auto-refreshing in 60 seconds.")
    st.markdown('<meta http-equiv="refresh" content="60">', unsafe_allow_html=True)
    st.stop()

prefetch_images(flights)   # parallel image cache warm-up

# ── Parse flights ────────────────────────────
processed_flights = []

for f in flights:
    flight_num   = f.get("number", "N/A")
    status       = f.get("status", "").lower()

    # ── Airport / origin ──
    dep          = f.get("departure") or {}
    mv           = f.get("movement") or {}
    airport_info = dep.get("airport") or mv.get("airport") or {}
    city         = (airport_info.get("municipalityName")
                    or airport_info.get("name")
                    or airport_info.get("iata")
                    or "Unknown")
    country_code = str(airport_info.get("countryCode", "")).strip().lower()

    # ── Arrival node ──
    arr_node = f.get("arrival") or f.get("movement") or {}
    terminal = str(arr_node.get("terminal", "")).strip().upper()
    gate     = arr_node.get("gate", "TBA")

    # Skip domestic
    if is_domestic(terminal, country_code):
        continue

    # ── Aircraft ──
    ac           = f.get("aircraft") or {}
    ac_model     = ac.get("model", "")
    ac_reg       = ac.get("reg", "")
    ac_text      = (f"{ac_model} ({ac_reg})" if ac_model and ac_reg
                    else ac_model or ac_reg)
    image_url    = fetch_aircraft_image(ac_reg)

    # ── Best arrival time (actual > revised > scheduled) ──
    best_dt = extract_best_time(arr_node, aest)
    if best_dt is None:
        continue

    # ── Scheduled time (for delay display & sorting) ──
    sch_raw = (arr_node.get("scheduledTime", {}) or {}).get("local")
    s_dt    = _parse_local_dt(sch_raw, aest) or best_dt
    sch_display = s_dt.strftime("%H:%M") if sch_raw else ""

    # ── Sanity: ignore implausible time offsets ──
    if sch_raw:
        diff_hours = (best_dt - s_dt).total_seconds() / 3600
        if diff_hours < -2 or diff_hours > 12:
            continue
        is_canceled = status in ("canceled", "cancelled")
        if not is_canceled and (now_aest - s_dt).total_seconds() > OLD_FLIGHT_HOURS * 3600:
            continue

    # ── Status flags ──
    time_diff_min    = int((best_dt - now_aest).total_seconds() / 60)
    is_canceled      = status in ("canceled", "cancelled")
    is_delayed       = status == "delayed"
    is_landed        = (status in ("landed", "arrived") or time_diff_min <= 0) and not is_canceled
    landed_mins      = max(0, -time_diff_min) if is_landed else 0
    minutes_left     = max(0, time_diff_min)  if not is_landed else 0
    mins_past_sch    = (now_aest - s_dt).total_seconds() / 60
    is_archived_can  = is_canceled and mins_past_sch > 15
    is_early_prep    = (not is_landed and not is_canceled
                        and PREP_START <= best_dt.time() <= PREP_END)

    border_color, status_color, bg_color, status_text = get_card_style(
        is_canceled, is_archived_can, is_landed,
        landed_mins, is_delayed, is_early_prep, minutes_left,
    )

    processed_flights.append({
        "is_gap":             False,
        "num":                flight_num,
        "origin":             city,
        "sch_display":        sch_display,
        "ac_text":            ac_text,
        "gate":               gate,
        "actual_time":        best_dt.strftime("%H:%M"),
        "is_landed":          is_landed,
        "is_canceled":        is_canceled,
        "is_archived_canceled": is_archived_can,
        "landed_mins":        landed_mins,
        "dt":                 best_dt,
        "s_dt_val":           s_dt,
        "image_url":          image_url,
        "border_color":       border_color,
        "status_color":       status_color,
        "status_text":        status_text,
        "bg_color":           bg_color,
    })

# ── Gap detection ─────────────────────────────
future_flights = sorted(
    [pf for pf in processed_flights if not pf["is_landed"] and not pf["is_canceled"]],
    key=lambda x: x["dt"],
)

if future_flights:
    # Gaps are measured from NOW (not from the last landed flight)
    windows = [(now_aest, future_flights[0]["dt"])]
    for i in range(len(future_flights) - 1):
        windows.append((future_flights[i]["dt"], future_flights[i + 1]["dt"]))

    for t_start, t_end in windows:
        if t_end <= now_aest:
            continue
        display_start = max(t_start, now_aest)
        gap_mins = int((t_end - display_start).total_seconds() / 60)
        if (t_end - t_start).total_seconds() / 60 < GAP_MIN_MINUTES:
            continue
        if gap_mins < GAP_DISPLAY_MIN:
            continue

        is_active  = t_start <= now_aest
        title_text = (f"🟢 ACTIVE OFF-FLOOR ({format_hm(gap_mins)} left)"
                      if is_active else f"🔄 {format_hm(gap_mins)} OFF-FLOOR WINDOW")
        time_text  = f"{display_start.strftime('%H:%M')} – {t_end.strftime('%H:%M')}"
        gap_bg     = "#064E3B" if is_active else "#0F172A"
        gap_border = "#10B981" if is_active else "#475569"
        gap_color  = "#A7F3D0" if is_active else "#94A3B8"

        gap_html = (
            f'<div style="background-color:{gap_bg};border:1px dashed {gap_border};'
            f'border-radius:8px;padding:12px;margin-bottom:12px;text-align:center;'
            f'color:{gap_color};font-family:sans-serif;font-weight:bold;'
            f'box-shadow:0 2px 4px rgba(0,0,0,0.1);">'
            f'{title_text} <span style="opacity:0.7;font-weight:normal;margin-left:8px;">({time_text})</span>'
            f"</div>"
        )
        processed_flights.append({
            "is_gap":    True,
            "html":      gap_html,
            "time_key":  t_start.timestamp() + 1,
        })

# ── Sort ──────────────────────────────────────
def sort_key(pf):
    if pf["is_gap"]:
        return (1, pf["time_key"])
    if pf.get("is_canceled"):
        # archived (>15 min past sch) → bottom tier; fresh cancel → future timeline
        return (2, -pf["s_dt_val"].timestamp()) if pf.get("is_archived_canceled") else (1, pf["dt"].timestamp())
    if pf["is_landed"]:
        return (0, -pf["dt"].timestamp()) if pf["landed_mins"] <= RECENT_LANDED_MAX else (2, -pf["dt"].timestamp())
    return (1, pf["dt"].timestamp())

processed_flights.sort(key=sort_key)

# ── Render ────────────────────────────────────
for i, pf in enumerate(processed_flights):
    if pf["is_gap"]:
        render_gap_card(pf)
    else:
        render_flight_card(pf, i)
