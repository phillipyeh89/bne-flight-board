import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pytz

# ─────────────────────────────────────────────
#  1. GLOBAL CONFIGURATION
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
#  2. HELPER FUNCTIONS (DEFINED FIRST)
# ─────────────────────────────────────────────
def extract_best_time(node: dict, tz) -> tuple:
    """Extracts the most accurate time from API node."""
    for key, label in (("actualTime", "actual"), ("revisedTime", "revised"), ("scheduledTime", "scheduled")):
        raw = node.get(key).get("local") if isinstance(node.get(key), dict) else node.get(key + "Local")
        if raw:
            try:
                dt = pd.to_datetime(raw).to_pydatetime()
                dt = tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
                return dt, label
            except: continue
    return None, ""

def is_strictly_international(terminal: str, country_code: str, aircraft_model: str) -> bool:
    """Filters out non-international or small flights."""
    t, ac, cc = terminal.strip().upper(), aircraft_model.upper(), country_code.lower()
    if t in DOMESTIC_TERMINALS: return False
    if cc == "au": return False
    if any(k in ac for k in SMALL_AIRCRAFT_FILTER): return False
    return True

@st.cache_data(show_spinner=False)
def get_photo_from_api(reg: str):
    if not reg: return "NOT_FOUND"
    try:
        r = requests.get(f"https://api.planespotters.net/pub/photos/reg/{reg}", headers={"User-Agent": "BNE-Board-App/1.0"}, timeout=3.0)
        if r.status_code == 200:
            photos = r.json().get("photos", [])
            if photos: return photos[0]["thumbnail_large"]["src"]
    except: pass
    return "NOT_FOUND"

def get_airline_logo_url(flight_number: str) -> str:
    prefix = "".join(c for c in flight_number if c.isalpha())[:2].upper()
    return f"https://pics.avs.io/200/200/{prefix}.png" if len(prefix) == 2 else ""

@st.cache_data(ttl=API_DATA_TTL_SEC, show_spinner=False)
def fetch_flight_data(anchor: str, from_time: str, to_time: str) -> list:
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{AIRPORT_ICAO}/{from_time}/{to_time}"
    headers = {"X-RapidAPI-Key": st.secrets["X_RAPIDAPI_KEY"], "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"}
    try:
        r = requests.get(url, headers=headers, params={"direction": "Arrival", "withCancelled": "true", "withCodeshared": "false"}, timeout=10)
        r.raise_for_status()
        st.session_state.api_last_hit = datetime.now(pytz.timezone(TIMEZONE))
        return r.json().get("arrivals", [])
    except: return []

# ─────────────────────────────────────────────
#  3. UI & CSS (GRID ALIGNMENT)
# ─────────────────────────────────────────────
st.set_page_config(page_title="BNE Pro Arrivals", page_icon="✈️", layout="centered")
st.markdown(f"""
<meta http-equiv="refresh" content="{UI_REFRESH_SEC}">
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=JetBrains+Mono:wght@600&display=swap');
    #MainMenu {{visibility: hidden;}}
    header {{visibility: hidden;}}
    .block-container {{padding-top: 1.5rem; font-family: 'Inter', sans-serif; max-width: 850px;}}
    .mono {{ font-family: 'JetBrains Mono', monospace; letter-spacing: -0.5px; }}
    
    /* Flip Animation */
    .flip-container {{ position: relative; width: 70px; height: 70px; margin-right: 20px; flex-shrink: 0; }}
    .flip-img {{ position: absolute; top: 0; left: 0; width: 70px; height: 70px; border-radius: 35px; border: 2.5px solid #475569; transition: opacity 1s ease-in-out; }}
    @keyframes logoFade {{ 0%, 45% {{ opacity: 1; }} 55%, 100% {{ opacity: 0; }} }}
    @keyframes photoFade {{ 0%, 45% {{ opacity: 0; }} 55%, 95% {{ opacity: 1; }} 100% {{ opacity: 0; }} }}
    .logo-layer {{ animation: logoFade 10s infinite; background: #FFFFFF; padding: 7px; object-fit: contain; border-radius: 10px; z-index: 2; }}
    .photo-layer {{ animation: photoFade 10s infinite; object-fit: cover; z-index: 1; }}
    
    /* Card Grid Alignment */
    .flight-card {{
        background-color: #1E293B; border-radius: 12px; padding: 16px 20px; 
        margin-bottom: 12px; display: flex; align-items: center; color: white;
        box-shadow: 0 4px 10px rgba(0,0,0,0.2); border-left: 6px solid #3B82F6;
    }}
    .info-col {{ flex-grow: 1; min-width: 0; }}
    .status-col {{ text-align: right; min-width: 135px; display: flex; flex-direction: column; justify-content: center; }}
    
    /* Gap Bar Alignment (Added 6px transparent border to match card alignment) */
    .gap-bar {{
        background-color: #0F172A; border: 1.5px dashed #475569; border-left: 6px solid transparent;
        border-radius: 10px; padding: 12px; margin: 10px 0 22px 0; text-align: center; color: #94A3B8;
        font-weight: 600; font-size: 0.95em;
    }}
    .gap-active {{ background-color: #064E3B; border-color: #10B981; border-left-color: #10B981; color: #A7F3D0; }}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  4. EXECUTION ENGINE
# ─────────────────────────────────────────────
if "api_last_hit" not in st.session_state: st.session_state.api_last_hit = None
aest = pytz.timezone(TIMEZONE); now_aest = datetime.now(aest)
anchor = (datetime(2000, 1, 1, tzinfo=aest) + timedelta(seconds=(int((now_aest - datetime(2000, 1, 1, tzinfo=aest)).total_seconds()) // API_DATA_TTL_SEC) * API_DATA_TTL_SEC)).strftime("%Y-%m-%dT%H:%M")

c1, c2 = st.columns([2, 1])
with c1: st.title("✈️ Arrivals")
with c2:
    st.markdown(f'<div style="font-size:0.85em;color:#94A3B8;text-align:right;margin-top:10px;">🕒 Live: {now_aest.strftime("%H:%M:%S")}</div>', unsafe_allow_html=True)
    api_t = st.session_state.get("api_last_hit")
    api_txt = f'API: {api_t.strftime("%H:%M")}' if api_t else 'API: --:--'
    st.markdown(f'<div style="font-size:0.75em;color:#64748B;text-align:right;">{api_txt}</div>', unsafe_allow_html=True)

with st.expander("ℹ️ System Info & Labels"):
    st.markdown(f"""
    **Time Labels:**
    * <span class="mono" style="color:#7DD3FC;font-weight:bold;">Act</span>: **Actual** (Landed)
    * <span class="mono" style="color:#E2E8F0;font-weight:bold;">Est</span>: **Estimated** (Radar ETA)
    * <span class="mono" style="color:#94A3B8;font-weight:bold;">Sch</span>: **Scheduled** (No radar data)
    
    **Window:** -{LOOKBACK_HOURS}h to +{LOOKAHEAD_HOURS}h | Developer: Phillip Yeh | V8.5
    """, unsafe_allow_html=True)

# Fetch and Process
raw_flights = fetch_flight_data(anchor, (now_aest - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M"), (now_aest + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M"))
if not raw_flights: st.info("Waiting for API sync..."); st.stop()

processed = []
for f in {f.get("number"): f for f in raw_flights}.values():
    flight_num = f.get("number", "N/A")
    ai = f.get("departure", {}).get("airport") or f.get("movement", {}).get("airport") or {}
    arr = f.get("arrival") or f.get("movement") or {}
    ac_m = f.get("aircraft", {}).get("model", "")
    ac_r = f.get("aircraft", {}).get("reg", "")
    
    if not is_strictly_international(str(arr.get("terminal", "")), str(ai.get("countryCode", "")), ac_m): continue
    
    best_dt, t_type = extract_best_time(arr, aest)
    if not best_dt: continue
    s_dt_raw = arr.get("scheduledTime", {}).get("local") if isinstance(arr.get("scheduledTime"), dict) else best_dt
    s_dt = pd.to_datetime(s_dt_raw).replace(tzinfo=None)
    s_dt = aest.localize(s_dt)
    
    t_diff = int((best_dt - now_aest).total_seconds() / 60)
    is_can = f.get("status", "").lower() in ("canceled", "cancelled")
    is_lan = (f.get("status", "").lower() in ("landed", "arrived") or t_diff <= 0) and not is_can
    
    # Styles
    delay = (best_dt - s_dt).total_seconds() / 3600
    if is_can: bc, sc, bg, st_txt = ("#475569", "#94A3B8", "#0F172A", "CANCELED") if (now_aest-s_dt).total_seconds()/60 > 15 else ("#EF4444", "#F87171", "#1E293B", "CANCELED")
    elif is_lan:
        l_min = max(0, -t_diff)
        bc, sc, bg, st_txt = ("#10B981", "#34D399", "#1E293B", f"Landed {l_min}m ago") if l_min <= RECENT_LANDED_MAX else ("#475569", "#94A3B8", "#0F172A", f"Landed {l_min}m ago")
    else:
        m_left = max(0, t_diff)
        if delay >= 12: bc, sc, bg, st_txt = "#7F1D1D", "#FCA5A5", "#1E293B", "SEVERE DELAY"
        elif m_left < 25: bc, sc, bg, st_txt = "#EF4444", "#F87171", "#1E293B", f"In {m_left}m"
        else: bc, sc, bg, st_txt = "#3B82F6", "#60A5FA", "#1E293B", f"In {m_left}m"

    processed.append({
        "num": flight_num, "origin": CITY_MAP.get(ai.get("municipalityName") or ai.get("name"), ai.get("municipalityName") or ai.get("name") or "Unknown"),
        "iata": ai.get("iata", ""), "gate": arr.get("gate", "TBA"), "ac_text": f"{ac_m} ({ac_r})" if ac_m and ac_r else ac_m or ac_r,
        "actual_time": best_dt.strftime("%H:%M"), "sch_time": s_dt.strftime("%H:%M"), "is_landed": is_lan, "is_canceled": is_can, 
        "dt": best_dt, "s_dt_val": s_dt, "time_type": t_type, "logo_url": get_airline_logo_url(flight_num), "photo_url": get_photo_from_api(ac_r),
        "border_color": bc, "status_color": sc, "status_text": st_txt, "bg_color": bg, "landed_mins": max(0,-t_diff)
    })

# Gap detection
future = sorted([p for p in processed if not p["is_landed"] and not p["is_canceled"]], key=lambda x: x["dt"])
if future:
    wins = [(now_aest, future[0]["dt"])] + [(future[i]["dt"], future[i+1]["dt"]) for i in range(len(future)-1)]
    for t1, t2 in wins:
        if t2 <= now_aest: continue
        g_min = int((t2 - max(t1, now_aest)).total_seconds() / 60)
        if (t2-t1).total_seconds()/60 < GAP_MIN_MINUTES or g_min < GAP_DISPLAY_MIN: continue
        act = t1 <= now_aest; cls = "gap-bar gap-active" if act else "gap-bar"
        processed.append({"is_gap": True, "html": f'<div class="{cls}">{"🟢 ACTIVE" if act else "🔄"} {g_min}m OFF-FLOOR WINDOW <span style="opacity:0.6; font-weight:400; margin-left:8px;">({max(t1, now_aest).strftime("%H:%M")}–{t2.strftime("%H:%M")})</span></div>', "time_key": t1.timestamp() + 1})

# Render Engine
processed.sort(key=lambda p: (1, p["time_key"]) if p.get("is_gap") else ((2, p["s_dt_val"].timestamp()) if p["is_canceled"] else ((0, -p["dt"].timestamp()) if p["is_landed"] and p["landed_mins"] <= RECENT_LANDED_MAX else ((2, -p["dt"].timestamp()) if p["is_landed"] else (1, p["dt"].timestamp())))))

for i, pf in enumerate(processed):
    if pf.get("is_canceled"): continue
    if pf.get("is_gap"): st.markdown(pf["html"], unsafe_allow_html=True); continue
    
    mid = f"modal_{i}"
    img_html = f'<div class="flip-container"><label for="{mid}" class="avatar-btn"><img src="{pf["logo_url"]}" class="flip-img logo-layer" style="border-color:{pf["border_color"]};"/><img src="{pf["photo_url"]}" class="flip-img photo-layer" style="border-color:{pf["border_color"]};"/></label></div>' if pf["photo_url"] != "NOT_FOUND" else f'<div class="flip-container"><img src="{pf["logo_url"]}" class="flip-img" style="border-color:{pf["border_color"]}; background:#FFF; padding:7px; object-fit:contain; border-radius:10px;"/></div>'
    tag = "Act" if pf["is_landed"] or pf["time_type"] == "actual" else ("Est" if pf["time_type"] == "revised" else "Sch")
    time_color = "#7DD3FC" if tag == "Act" else ("#E2E8F0" if tag == "Est" else "#94A3B8")
    cb = ' <span style="color:#FBBF24; font-size:0.8em;">⚠️ Check Board</span>' if (tag == "Sch" and not pf["is_canceled"]) else ""

    st.markdown(f"""
    <div class="flight-card" style="border-left-color:{pf['border_color']}; background-color:{pf['bg_color']};">
        {img_html}
        <div class="info-col">
            <div style="font-size:1.4em; font-weight:700;">{pf['num']}<span style="font-size:0.7em; color:#94A3B8; margin-left:10px;">{pf['origin']} ({pf['iata']})</span></div>
            <div style="font-size:0.85em; color:#CBD5E1; margin: 2px 0;">{pf['ac_text']}</div>
            <div style="font-size:0.85em; color:#94A3B8;"><span class="mono">Sch {pf['sch_time']}</span> • <span class="mono" style="color:{time_color}; font-weight:700;">{tag} {pf['actual_time']}</span>{cb}</div>
        </div>
        <div class="status-col">
            <div style="font-size:0.75em; color:#94A3B8; font-weight:700; letter-spacing:1px;">GATE</div>
            <div class="mono" style="font-size:2.6em; font-weight:700; line-height:1;">{pf['gate']}</div>
            <div style="font-size:1em; font-weight:700; color:{pf['status_color']}; margin-top:4px;">{pf['status_text']}</div>
        </div>
    </div>
    <input type="checkbox" id="{mid}" class="img-zoom-chk" style="display:none;"><div class="img-zoom-modal"><label for="{mid}" class="img-zoom-close"></label><img src="{pf['photo_url'] if pf['photo_url'] != 'NOT_FOUND' else pf['logo_url']}"/></div>
    """, unsafe_allow_html=True)

# Canceled
cans = sorted([f for f in processed if f.get("is_canceled")], key=lambda x: x["s_dt_val"])
if cans:
    st.markdown("<hr style='margin:40px 0 20px 0; opacity:0.2;'><h4 style='color:#F87171;'>❌ Canceled</h4>", unsafe_allow_html=True)
    for i, pf in enumerate(cans):
        mid = f"can_{i}"
        img_html = f'<div class="flip-container"><img src="{pf["logo_url"]}" class="flip-img" style="border-color:{pf["border_color"]}; background:#FFF; padding:7px; object-fit:contain; border-radius:10px;"/></div>'
        st.markdown(f"""<div class="flight-card" style="border-left-color:{pf['border_color']}; background-color:{pf['bg_color']};">{img_html}<div class="info-col"><div style="font-size:1.4em; font-weight:700;">{pf['num']} <span style="font-size:0.7em; color:#94A3B8;">{pf['origin']}</span></div><div style="font-size:0.85em; color:#94A3B8;"><span class="mono">Sch {pf['sch_time']}</span></div></div><div class="status-col"><div style="font-size:1em; font-weight:700; color:{pf['status_color']};">{pf['status_text']}</div></div></div>""", unsafe_allow_html=True)

st.markdown(f"<div style='text-align:center; color:#475569; font-size:0.8em; margin-top:50px;'>Developer: Phillip Yeh | V8.5</div>", unsafe_allow_html=True)
