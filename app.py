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

UI_REFRESH_SEC           = 60
API_DATA_TTL_SEC        = 600
STALE_DATA_THRESHOLD_MIN = 30

# ─────────────────────────────────────────────
#  PAGE CONFIG & DYNAMIC CSS (FLIP ANIMATION)
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
    
    /* Flip Container Logic */
    .flip-container {{
        position: relative; width: 70px; height: 70px; margin-right: 18px; flex-shrink: 0;
    }}
    .flip-img {{
        position: absolute; top: 0; left: 0; width: 70px; height: 70px;
        transition: opacity 1s ease-in-out; border-radius: 35px; border: 2px solid #475569;
    }}
    
    /* Animation Cycle: 10 seconds total (5s each) */
    @keyframes logoFade {{
        0%, 45% {{ opacity: 1; }}
        55%, 100% {{ opacity: 0; }}
    }}
    @keyframes photoFade {{
        0%, 45% {{ opacity: 0; }}
        55%, 95% {{ opacity: 1; }}
        100% {{ opacity: 0; }}
    }}
    
    .logo-layer {{
        animation: logoFade 10s infinite; background: #FFFFFF; padding: 6px; 
        object-fit: contain; border-radius: 8px; box-sizing: border-box; z-index: 2;
    }}
    .photo-layer {{
        animation: photoFade 10s infinite; object-fit: cover; z-index: 1;
    }}
    
    /* Zoom Modal */
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
#  IMAGE FETCHING LOGIC
# ─────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _get_photo_from_api(reg: str):
    try:
        url = f"https://api.planespotters.net/pub/photos/reg/{reg}"
        r = requests.get(url, headers={"User-Agent": "BNE-Board-App/1.0"}, timeout=3.0)
        if r.status_code == 200:
            photos = r.json().get("photos", [])
            if photos: return photos[0]["thumbnail_large"]["src"]
    except: pass
    return None

def get_airline_logo_url(flight_number: str) -> str | None:
    if not flight_number: return None
    prefix = "".join(c for c in flight_number if c.isalpha())[:2].upper()
    return f"https://pics.avs.io/200/200/{prefix}.png" if len(prefix) == 2 else None

# ─────────────────────────────────────────────
#  DATA PROCESSING
# ─────────────────────────────────────────────
def _get_cache_anchor(now_aest) -> str:
    epoch = datetime(2000, 1, 1, tzinfo=now_aest.tzinfo)
    elapsed = int((now_aest - epoch).total_seconds())
    floored = (elapsed // API_DATA_TTL_SEC) * API_DATA_TTL_SEC
    return (epoch + timedelta(seconds=floored)).strftime("%Y-%m-%dT%H:%M")

@st.cache_data(ttl=API_DATA_TTL_SEC, show_spinner=False)
def fetch_flight_data(cache_anchor: str, from_time: str, to_time: str) -> list:
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{AIRPORT_ICAO}/{from_time}/{to_time}"
    headers = {"X-RapidAPI-Key": st.secrets["X_RAPIDAPI_KEY"], "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"}
    try:
        r = requests.get(url, headers=headers, params={"direction": "Arrival", "withCancelled": "true", "withCodeshared": "false"}, timeout=10)
        r.raise_for_status()
        st.session_state.api_last_hit = datetime.now(pytz.timezone(TIMEZONE))
        return r.json().get("arrivals", [])
    except Exception as e:
        st.error(f"API Failed: {e}"); return []

def format_hm(total_minutes: int) -> str:
    h, m = divmod(total_minutes, 60)
    return f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"

def extract_best_time(node: dict, tz) -> tuple:
    for key, label in (("actualTime", "actual"), ("revisedTime", "revised"), ("scheduledTime", "scheduled")):
        raw = node.get(key).get("local") if isinstance(node.get(key), dict) else node.get(key + "Local")
        if raw:
            dt = pd.to_datetime(raw).to_pydatetime()
            dt = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
            return dt, label
    return None, ""

def get_card_style(is_canceled, is_archived, is_landed, landed_mins, delay_hours, mins_left):
    if is_canceled:
        return ("#475569", "#94A3B8", "#0F172A", "❌ CANCELED") if is_archived else ("#EF4444", "#F87171", "#1E293B", "❌ CANCELED")
    if is_landed:
        return ("#10B981", "#34D399", "#1E293B", f"Landed {format_hm(landed_mins)} ago") if landed_mins <= RECENT_LANDED_MAX else ("#475569", "#94A3B8", "#0F172A", f"Landed {format_hm(landed_mins)} ago")
    bg = "#1E293B"
    if delay_hours >= 12: return "#7F1D1D", "#FCA5A5", bg, f"🚨 SEVERE DELAY In {format_hm(mins_left)}"
    if mins_left < 25: return "#EF4444", "#F87171", bg, f"🔥 In {format_hm(mins_left)}"
    if delay_hours >= 3: return "#EF4444", "#F87171", bg, f"⚠️ HEAVY DELAY In {format_hm(mins_left)}"
    if mins_left <= 60: return "#F59E0B", "#FBBF24", bg, f"In {format_hm(mins_left)}"
    return "#3B82F6", "#60A5FA", bg, f"In {format_hm(mins_left)}"

# ─────────────────────────────────────────────
#  DYNAMICS RENDERER
# ─────────────────────────────────────────────
def render_flight_card(pf: dict, index: int):
    logo_url  = pf["logo_url"]
    photo_url = pf["photo_url"]
    border_col = pf["border_color"]
    mid = f"modal_{index}"
    
    # Logic: If both exist, we flip. If only logo exists, we show logo only.
    if logo_url and photo_url != "NOT_FOUND":
        image_html = f"""
        <div class="flip-container">
            <label for="{mid}" class="avatar-btn">
                <img src="{logo_url}" class="flip-img logo-layer" style="border-color:{border_col};" />
                <img src="{photo_url}" class="flip-img photo-layer" style="border-color:{border_col};" />
            </label>
        </div>
        """
    elif logo_url:
        image_html = f"""
        <div class="flip-container">
            <label for="{mid}" class="avatar-btn">
                <img src="{logo_url}" class="flip-img" style="border-color:{border_col}; background:#FFF; padding:6px; object-fit:contain; border-radius:8px;" />
            </label>
        </div>
        """
    else:
        image_html = f'<div style="width:70px;height:70px;border-radius:35px;background:#334155;display:flex;align-items:center;justify-content:center;margin-right:18px;font-size:1.6em;border:2px solid {border_col};flex-shrink:0;">✈️</div>'

    # Tooltip / Modal (Shows the specific Photo if available, otherwise Logo)
    zoom_img = photo_url if photo_url != "NOT_FOUND" else logo_url
    modal_html = f"""
    <input type="checkbox" id="{mid}" class="img-zoom-chk" style="display:none;">
    <div class="img-zoom-modal">
        <label for="{mid}" class="img-zoom-close"></label>
        <label for="{mid}" class="close-btn-text">&times;</label>
        <img src="{zoom_img}" />
    </div>
    """

    sch_str = f'<span class="mono">Sch {pf["sch_display"]}</span> • ' if pf["sch_display"] else ""
    check_board_tag = ' <span style="color:#FBBF24; font-size:0.85em; font-weight:700;">⚠️ Check Board</span>' if (not pf["is_landed"] and not pf["is_canceled"] and pf["time_type"] == "scheduled") else ""
    
    if pf["is_landed"] or pf["time_type"] == "actual": act_html = f'<span class="mono" style="color:#7DD3FC;font-weight:bold;background:rgba(14,165,233,0.15);padding:2px 6px;border-radius:4px;">Act {pf["actual_time"]}</span>'
    elif pf["time_type"] == "revised": act_html = f'<span class="mono" style="color:#E2E8F0;font-weight:bold;background:rgba(226,232,240,0.15);padding:2px 6px;border-radius:4px;">Est {pf["actual_time"]}</span>{check_board_tag}'
    else: act_html = f'<span class="mono" style="color:#94A3B8;font-weight:bold;background:rgba(148,163,184,0.15);padding:2px 6px;border-radius:4px;">Sch {pf["actual_time"]}</span>{check_board_tag}'; sch_str = ""

    origin_display = f"{pf['origin']} <span class='mono' style='font-size:0.85em; opacity:0.8;'>({pf['iata']})</span>" if pf["iata"] else pf["origin"]
    
    st.markdown(f"""
    <div style="background-color:{pf['bg_color']};border-left:6px solid {border_col};border-radius:8px;padding:16px 20px;margin-bottom:12px;display:flex;align-items:center;color:white;box-shadow:0 4px 6px rgba(0,0,0,0.15);">
        {image_html}
        <div style="flex-grow:1;">
            <div style="font-size:1.4em;font-weight:700;">{pf['num']}<span style="font-size:0.75em;color:#94A3B8;margin-left:8px;">{origin_display}</span></div>
            <div style="font-size:0.85em;color:#CBD5E1;">{pf['ac_text']}</div>
            <div style="font-size:0.85em;color:#CBD5E1;">{sch_str}{act_html}</div>
        </div>
        <div style="text-align:right;min-width:110px;">
            <div style="font-size:0.8em;color:#94A3B8;font-weight:700;">GATE</div>
            <div class="mono" style="font-size:2.6em;font-weight:700;line-height:1;">{pf['gate']}</div>
            <div style="font-size:1.05em;font-weight:700;color:{pf['status_color']};">{pf['status_text']}</div>
        </div>
    </div>
    {modal_html}
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  EXECUTION
# ─────────────────────────────────────────────
aest = pytz.timezone(TIMEZONE); now_aest = datetime.now(aest)
from_t = (now_aest - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M")
to_t = (now_aest + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M")
anchor = _get_cache_anchor(now_aest)

c1, c2 = st.columns([2, 1])
with c1: st.title("✈️ Arrivals")
with c2:
    st.markdown(f'<div style="font-size:0.85em;color:#94A3B8;text-align:center;margin-top:10px;">🕒 Live: {now_aest.strftime("%H:%M:%S")}</div>', unsafe_allow_html=True)
    api_t = st.session_state.get("api_last_hit")
    api_html = f'<span class="stale-warning">API: {api_t.strftime("%H:%M")} (STALE)</span>' if (api_t and (now_aest - api_t).total_seconds()/60 > STALE_DATA_THRESHOLD_MIN) else f'API: {api_t.strftime("%H:%M") if api_t else "--:--"}'
    st.markdown(f'<div style="font-size:0.75em;color:#64748B;text-align:center;">{api_html}</div>', unsafe_allow_html=True)

flights = fetch_flight_data(anchor, from_t, to_t)
if not flights: st.info("No data available..."); st.stop()

# Deduplicate
seen = {}
for f in flights:
    num = f.get("number")
    if num and num not in seen: seen[num] = f
unique_flights = list(seen.values())

processed_flights = []
for f in unique_flights:
    flight_num = f.get("number", "N/A")
    ai = f.get("departure", {}).get("airport") or f.get("movement", {}).get("airport") or {}
    arr_n = f.get("arrival") or f.get("movement") or {}
    ac = f.get("aircraft", {}); ac_m, ac_r = ac.get("model", ""), ac.get("reg", "")
    if not is_strictly_international(str(arr_n.get("terminal", "")), str(ai.get("countryCode", "")), ac_m): continue
    best_dt, t_type = extract_best_time(arr_n, aest)
    if not best_dt: continue
    s_dt = _parse_local_dt((arr_n.get("scheduledTime", {}) or {}).get("local"), aest) or best_dt
    delay_hours = (best_dt - s_dt).total_seconds() / 3600 if s_dt else 0
    if delay_hours < -2 or delay_hours > 24: continue
    t_diff = int((best_dt - now_aest).total_seconds() / 60)
    is_can = f.get("status", "").lower() in ("canceled", "cancelled")
    is_lan = (f.get("status", "").lower() in ("landed", "arrived") or t_diff <= 0) and not is_can
    bc, sc, bg, st_txt = get_card_style(is_can, (is_can and (now_aest-s_dt).total_seconds()/60 > 15), is_lan, max(0,-t_diff), delay_hours, max(0,t_diff))
    
    processed_flights.append({
        "num": flight_num, "origin": CITY_MAP.get(ai.get("municipalityName") or ai.get("name") or "Unknown", ai.get("municipalityName") or ai.get("name") or "Unknown"),
        "iata": ai.get("iata", ""), "sch_display": s_dt.strftime("%H:%M") if (arr_n.get("scheduledTime", {}) or {}).get("local") else "",
        "ac_text": f"{ac_m} ({ac_r})" if ac_m and ac_r else ac_m or ac_r, "gate": arr_n.get("gate", "TBA"),
        "actual_time": best_dt.strftime("%H:%M"), "is_landed": is_lan, "is_canceled": is_can, "landed_mins": max(0,-t_diff),
        "dt": best_dt, "s_dt_val": s_dt, "time_type": t_type, "logo_url": get_airline_logo_url(flight_num),
        "photo_url": _get_photo_from_api(ac_r) if ac_r else "NOT_FOUND", "border_color": bc, "status_color": sc, "status_text": st_txt, "bg_color": bg, "is_next_day": best_dt.date() > now_aest.date()
    })

# Gap Detection
future_f = sorted([p for p in processed_flights if not p["is_landed"] and not p["is_canceled"]], key=lambda x: x["dt"])
if future_f:
    windows = [(now_aest, future_f[0]["dt"])]
    for i in range(len(future_f)-1): windows.append((future_f[i]["dt"], future_f[i+1]["dt"]))
    for t1, t2 in windows:
        if t2 <= now_aest: continue
        ds = max(t1, now_aest); g_min = int((t2 - ds).total_seconds() / 60)
        if (t2-t1).total_seconds()/60 < GAP_MIN_MINUTES or g_min < GAP_DISPLAY_MIN: continue
        act = t1 <= now_aest; tit = f"🟢 ACTIVE OFF-FLOOR ({format_hm(g_min)} left)" if act else f"🔄 {format_hm(g_min)} OFF-FLOOR WINDOW"
        gb, gbo, gc = ("#064E3B", "#10B981", "#A7F3D0") if act else ("#0F172A", "#475569", "#94A3B8")
        processed_flights.append({"is_gap": True, "html": f'<div style="background-color:{gb};border:1px dashed {gbo};border-radius:8px;padding:10px;margin-bottom:12px;text-align:center;color:{gc};font-weight:bold;">{tit} <span style="opacity:0.7;font-weight:normal;">({ds.strftime("%H:%M")}–{t2.strftime("%H:%M")})</span></div>', "time_key": t1.timestamp() + 1})

# Sorting & Render
processed_flights.sort(key=lambda p: (1, p["time_key"]) if p.get("is_gap") else ((2, p["s_dt_val"].timestamp()) if p["is_canceled"] else ((0, -p["dt"].timestamp()) if p["is_landed"] and p["landed_mins"] <= RECENT_LANDED_MAX else ((2, -p["dt"].timestamp()) if p["is_landed"] else (1, p["dt"].timestamp())))))
for i, pf in enumerate([f for f in processed_flights if not f.get("is_canceled")]):
    if pf.get("is_gap"): st.markdown(pf["html"], unsafe_allow_html=True)
    else: render_flight_card(pf, i)

if any(f["is_canceled"] for f in processed_flights):
    st.markdown("<hr style='margin:40px 0 20px 0; opacity:0.3;'><h4 style='color:#F87171;'>❌ Canceled</h4>", unsafe_allow_html=True)
    for i, pf in enumerate(sorted([f for f in processed_flights if f.get("is_canceled")], key=lambda x: x["s_dt_val"])): render_flight_card(pf, 999 + i)

st.markdown(f"<div style='text-align:center; color:#475569; font-size:0.8em; margin-top:50px;'>Developer: Phillip Yeh | V8.1</div>", unsafe_allow_html=True)
