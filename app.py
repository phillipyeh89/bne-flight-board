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
#  PAGE CONFIG & DYNAMIC CSS
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
    
    /* Flip Container Logic */
    .flip-container {{ position: relative; width: 70px; height: 70px; margin-right: 18px; flex-shrink: 0; }}
    .flip-img {{ position: absolute; top: 0; left: 0; width: 70px; height: 70px; transition: opacity 1s ease-in-out; border-radius: 35px; border: 2px solid #475569; }}
    
    @keyframes logoFade {{ 0%, 45% {{ opacity: 1; }} 55%, 100% {{ opacity: 0; }} }}
    @keyframes photoFade {{ 0%, 45% {{ opacity: 0; }} 55%, 95% {{ opacity: 1; }} 100% {{ opacity: 0; }} }}
    
    .logo-layer {{ animation: logoFade 10s infinite; background: #FFFFFF; padding: 6px; object-fit: contain; border-radius: 8px; box-sizing: border-box; z-index: 2; }}
    .photo-layer {{ animation: photoFade 10s infinite; object-fit: cover; z-index: 1; }}
    
    .avatar-btn {{ cursor: pointer; display: block; }}
    .img-zoom-chk:checked + .img-zoom-modal {{ display: flex; }}
    .img-zoom-modal {{ display: none; position: fixed; top:0; left:0; right:0; bottom:0; background: rgba(15,23,42,0.92); z-index: 9999; align-items: center; justify-content: center; backdrop-filter: blur(5px); }}
    .img-zoom-modal img {{ max-width: 90vw; max-height: 80vh; border-radius: 12px; border: 2px solid #475569; object-fit: contain; }}
    .img-zoom-close {{ position: absolute; top:0; left:0; right:0; bottom:0; cursor: pointer; }}
    .close-btn-text {{ position: absolute; top: 20px; right: 30px; color: #F8FAFC; font-size: 3em; cursor: pointer; }}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  CORE FUNCTIONS (Defined Global to prevent NameError)
# ─────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def get_photo_from_api(reg: str):
    if not reg: return "NOT_FOUND"
    try:
        url = f"https://api.planespotters.net/pub/photos/reg/{reg}"
        r = requests.get(url, headers={"User-Agent": "BNE-Board-App/1.0"}, timeout=3.0)
        if r.status_code == 200:
            photos = r.json().get("photos", [])
            if photos: return photos[0]["thumbnail_large"]["src"]
    except: pass
    return "NOT_FOUND"

def get_airline_logo_url(flight_number: str) -> str:
    prefix = "".join(c for c in flight_number if c.isalpha())[:2].upper()
    return f"https://pics.avs.io/200/200/{prefix}.png" if len(prefix) == 2 else ""

def is_strictly_international(terminal: str, country_code: str, aircraft_model: str) -> bool:
    t = terminal.strip().upper()
    ac = aircraft_model.upper()
    if t in DOMESTIC_TERMINALS: return False
    if country_code.lower() == "au": return False
    if any(k in ac for k in SMALL_AIRCRAFT_FILTER): return False
    return True

def extract_best_time(node: dict, tz) -> tuple:
    for key, label in (("actualTime", "actual"), ("revisedTime", "revised"), ("scheduledTime", "scheduled")):
        raw = node.get(key).get("local") if isinstance(node.get(key), dict) else node.get(key + "Local")
        if raw:
            dt = pd.to_datetime(raw).to_pydatetime()
            dt = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
            return dt, label
    return None, ""

@st.cache_data(ttl=API_DATA_TTL_SEC, show_spinner=False)
def fetch_flight_data(anchor: str, from_time: str, to_time: str) -> list:
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{AIRPORT_ICAO}/{from_time}/{to_time}"
    headers = {"X-RapidAPI-Key": st.secrets["X_RAPIDAPI_KEY"], "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"}
    try:
        r = requests.get(url, headers=headers, params={"direction": "Arrival", "withCancelled": "true", "withCodeshared": "false"}, timeout=10)
        r.raise_for_status()
        st.session_state.api_last_hit = datetime.now(pytz.timezone(TIMEZONE))
        return r.json().get("arrivals", [])
    except Exception as e:
        st.error(f"API Request Failed: {e}"); return []

def get_card_style(is_canceled, is_archived, is_landed, landed_mins, delay_hours, mins_left):
    if is_canceled:
        return ("#475569", "#94A3B8", "#0F172A", "❌ CANCELED") if is_archived else ("#EF4444", "#F87171", "#1E293B", "❌ CANCELED")
    if is_landed:
        h, m = divmod(landed_mins, 60)
        time_str = f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"
        return ("#10B981", "#34D399", "#1E293B", f"Landed {time_str} ago") if landed_mins <= RECENT_LANDED_MAX else ("#475569", "#94A3B8", "#0F172A", f"Landed {time_str} ago")
    bg = "#1E293B"
    if delay_hours >= 12: return "#7F1D1D", "#FCA5A5", bg, "🚨 SEVERE DELAY"
    if mins_left < 25: return "#EF4444", "#F87171", bg, f"🔥 In {mins_left}m"
    if delay_hours >= 3: return "#EF4444", "#F87171", bg, "⚠️ HEAVY DELAY"
    return "#3B82F6", "#60A5FA", bg, f"In {mins_left}m"

# ─────────────────────────────────────────────
#  EXECUTION
# ─────────────────────────────────────────────
aest = pytz.timezone(TIMEZONE); now_aest = datetime.now(aest)
anchor = (datetime(2000, 1, 1, tzinfo=aest) + timedelta(seconds=(int((now_aest - datetime(2000, 1, 1, tzinfo=aest)).total_seconds()) // API_DATA_TTL_SEC) * API_DATA_TTL_SEC)).strftime("%Y-%m-%dT%H:%M")
from_t = (now_aest - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M")
to_t = (now_aest + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M")

c1, c2 = st.columns([2, 1])
with c1: st.title("✈️ Arrivals")
with c2:
    st.markdown(f'<div style="font-size:0.85em;color:#94A3B8;text-align:center;margin-top:10px;">🕒 Live: {now_aest.strftime("%H:%M:%S")}</div>', unsafe_allow_html=True)
    api_t = st.session_state.get("api_last_hit")
    api_html = f'<span class="stale-warning">API: {api_t.strftime("%H:%M")} (STALE)</span>' if (api_t and (now_aest - api_t).total_seconds()/60 > STALE_DATA_THRESHOLD_MIN) else f'API: {api_t.strftime("%H:%M") if api_t else "--:--"}'
    st.markdown(f'<div style="font-size:0.75em;color:#64748B;text-align:center;">{api_html}</div>', unsafe_allow_html=True)

flights = fetch_flight_data(anchor, from_t, to_t)
if not flights: st.info("No data available..."); st.stop()

# Process & Render
unique_flights = {f.get("number"): f for f in flights}.values()
processed_flights = []

for f in unique_flights:
    flight_num = f.get("number", "N/A")
    dep_ap = f.get("departure", {}).get("airport") or f.get("movement", {}).get("airport") or {}
    arr_info = f.get("arrival") or f.get("movement") or {}
    ac = f.get("aircraft", {}); ac_m, ac_r = ac.get("model", ""), ac.get("reg", "")
    
    if not is_strictly_international(str(arr_info.get("terminal", "")), str(dep_ap.get("countryCode", "")), ac_m): continue
    
    best_dt, t_type = extract_best_time(arr_info, aest)
    if not best_dt: continue
    s_dt = pd.to_datetime(arr_info.get("scheduledTime", {}).get("local") if isinstance(arr_info.get("scheduledTime"), dict) else best_dt)
    
    t_diff = int((best_dt - now_aest).total_seconds() / 60)
    is_can = f.get("status", "").lower() in ("canceled", "cancelled")
    is_lan = (f.get("status", "").lower() in ("landed", "arrived") or t_diff <= 0) and not is_can
    
    bc, sc, bg, st_txt = get_card_style(is_can, (is_can and (now_aest-best_dt).total_seconds()/60 > 15), is_lan, max(0,-t_diff), (best_dt-s_dt).total_seconds()/3600, max(0,t_diff))
    
    pf = {
        "num": flight_num, "origin": CITY_MAP.get(dep_ap.get("municipalityName") or dep_ap.get("name"), dep_ap.get("municipalityName") or dep_ap.get("name") or "Unknown"),
        "iata": dep_ap.get("iata", ""), "gate": arr_info.get("gate", "TBA"), "ac_text": f"{ac_m} ({ac_r})" if ac_m and ac_r else ac_m or ac_r,
        "actual_time": best_dt.strftime("%H:%M"), "is_landed": is_lan, "is_canceled": is_can, "landed_mins": max(0,-t_diff),
        "dt": best_dt, "s_dt_val": s_dt, "time_type": t_type, "logo_url": get_airline_logo_url(flight_num),
        "photo_url": get_photo_from_api(ac_r), "border_color": bc, "status_color": sc, "status_text": st_txt, "bg_color": bg
    }
    processed_flights.append(pf)

# Sort and Render Cards
processed_flights.sort(key=lambda p: (2, p["s_dt_val"].timestamp()) if p["is_canceled"] else ((0, -p["dt"].timestamp()) if p["is_landed"] and p["landed_mins"] <= RECENT_LANDED_MAX else ((2, -p["dt"].timestamp()) if p["is_landed"] else (1, p["dt"].timestamp()))))

for i, pf in enumerate(processed_flights):
    if pf["is_canceled"]: continue
    mid = f"modal_{i}"
    image_html = f'<div class="flip-container"><label for="{mid}" class="avatar-btn"><img src="{pf["logo_url"]}" class="flip-img logo-layer" style="border-color:{pf["border_color"]};" /><img src="{pf["photo_url"]}" class="flip-img photo-layer" style="border-color:{pf["border_color"]};" /></label></div>' if pf["photo_url"] != "NOT_FOUND" else f'<div class="flip-container"><img src="{pf["logo_url"]}" class="flip-img" style="border-color:{pf["border_color"]}; background:#FFF; padding:6px; object-fit:contain; border-radius:8px;" /></div>'
    
    st.markdown(f"""
    <div style="background-color:{pf['bg_color']};border-left:6px solid {pf['border_color']};border-radius:8px;padding:16px 20px;margin-bottom:12px;display:flex;align-items:center;color:white;box-shadow:0 4px 6px rgba(0,0,0,0.15);">
        {image_html}
        <div style="flex-grow:1;">
            <div style="font-size:1.4em;font-weight:700;">{pf['num']}<span style="font-size:0.75em;color:#94A3B8;margin-left:8px;">{pf['origin']} ({pf['iata']})</span></div>
            <div style="font-size:0.85em;color:#CBD5E1;">{pf['ac_text']}</div>
            <div style="font-size:0.85em;color:#CBD5E1;"><span class="mono">Sch {pf['s_dt_val'].strftime('%H:%M')}</span> • <span class="mono" style="font-weight:bold;color:#7DD3FC;">{pf['actual_time']}</span></div>
        </div>
        <div style="text-align:right;min-width:110px;">
            <div style="font-size:0.8em;color:#94A3B8;font-weight:700;">GATE</div>
            <div class="mono" style="font-size:2.6em;font-weight:700;line-height:1;">{pf['gate']}</div>
            <div style="font-size:1.05em;font-weight:700;color:{pf['status_color']};">{pf['status_text']}</div>
        </div>
    </div>
    <input type="checkbox" id="{mid}" class="img-zoom-chk" style="display:none;"><div class="img-zoom-modal"><label for="{mid}" class="img-zoom-close"></label><label for="{mid}" class="close-btn-text">&times;</label><img src="{pf['photo_url'] if pf['photo_url'] != 'NOT_FOUND' else pf['logo_url']}" /></div>
    """, unsafe_allow_html=True)

st.markdown(f"<div style='text-align:center; color:#475569; font-size:0.8em; margin-top:50px;'>Developer: Phillip Yeh | V8.2</div>", unsafe_allow_html=True)
