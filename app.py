import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pytz
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

# =====================================================================
# 1. CONFIGURATION & CONSTANTS
# =====================================================================
AIRPORT_ICAO       = "YBBN"
TIMEZONE           = pytz.timezone("Australia/Brisbane")
LOOKBACK_HOURS     = 4
LOOKAHEAD_HOURS    = 8
RECENT_LANDED_MAX  = 60
PAX_TO_STORE_MINS  = 25
GAP_MIN_MINUTES    = 20
GAP_DISPLAY_MIN    = 5
IMAGE_WORKERS      = 15

DOMESTIC_TERMINALS    = {'D', 'DOM', 'D-ANC', 'GAT'}
SMALL_AIRCRAFT_FILTER = {'BEECH', 'FAIRCHILD', 'CESSNA', 'PIPER', 'PILATUS', 'KING AIR', 'METROLINER', 'SAAB'}
CITY_MAP = {
    "Lapu-Lapu City": "Cebu", "Denpasar-Bali Island": "Bali",
    "Ho Chi Minh City": "Saigon", "Yaren District": "Nauru",
    "Guangzhou Baiyun": "Guangzhou"
}

# --- UI Theme Dictionary ---
# Centralized styling makes it easy to change colors later without digging into logic.
THEMES = {
    "CANCELED":    {"bg": "#0F172A", "border": "#475569", "text": "#94A3B8", "opacity": "0.5", "filter": "grayscale(100%)"},
    "CANCELED_NEW":{"bg": "#1E293B", "border": "#EF4444", "text": "#F87171", "opacity": "0.5", "filter": "grayscale(100%)"},
    "AT_STORE":    {"bg": "#0F172A", "border": "#F59E0B", "text": "#FBBF24", "opacity": "1.0", "filter": "none"},
    "LANDED_FRESH":{"bg": "#0F172A", "border": "#059669", "text": "#34D399", "opacity": "0.75","filter": "grayscale(40%)"},
    "LANDED_OLD":  {"bg": "#0F172A", "border": "#475569", "text": "#94A3B8", "opacity": "0.4", "filter": "grayscale(80%)"},
    "NO_UPDATE":   {"bg": "#0F172A", "border": "#F59E0B", "text": "#FBBF24", "opacity": "1.0", "filter": "none"},
    "SEVERE_DELAY":{"bg": "#1E293B", "border": "#7F1D1D", "text": "#FCA5A5", "opacity": "1.0", "filter": "none"},
    "INCOMING_HOT":{"bg": "#1E293B", "border": "#EF4444", "text": "#F87171", "opacity": "1.0", "filter": "none"},
    "INCOMING":    {"bg": "#1E293B", "border": "#3B82F6", "text": "#60A5FA", "opacity": "1.0", "filter": "none"}
}

# =====================================================================
# 2. DATA MODELS (The Object-Oriented Approach)
# =====================================================================
class Flight:
    """Encapsulates all logic and data for a single flight."""
    def __init__(self, raw_data: dict, now_tz: datetime):
        self.raw = raw_data
        self.now = now_tz
        
        # Basic Extraction
        self.number = self.raw.get("number", "N/A")
        self.status_raw = self.raw.get("status", "").lower()
        
        dep = self.raw.get("departure") or self.raw.get("movement") or {}
        arr = self.raw.get("arrival") or self.raw.get("movement") or {}
        ac  = self.raw.get("aircraft") or {}
        
        self.origin_iata = str(dep.get("iata", "")).upper()
        raw_city = dep.get("municipalityName") or dep.get("name") or "Unknown"
        self.origin_city = CITY_MAP.get(raw_city, raw_city)
        self.country_code = str(dep.get("countryCode", "")).lower()
        
        self.gate = arr.get("gate", "TBA")
        self.terminal = str(arr.get("terminal", "")).strip().upper()
        
        self.ac_model = ac.get("model", "")
        self.ac_reg = ac.get("reg", "")
        self.ac_text = f"{self.ac_model} ({self.ac_reg})" if self.ac_model and self.ac_reg else self.ac_model or self.ac_reg

        # Time Calculation
        self.s_dt, self.best_dt, self.time_type = self._parse_times(arr, dep)
        self.mins_to_arrival = int((self.best_dt - self.now).total_seconds() / 60)
        self.delay_hours = (self.best_dt - self.s_dt).total_seconds() / 3600

        # State Flags
        self.is_canceled = self.status_raw in ("canceled", "cancelled")
        self.is_landed = self._determine_if_landed()
        
        # Display & UI properties
        self.logo_url = self._generate_logo_url()
        self.photo_url = "NOT_FOUND" # Populated later via threading
        self.ui_state = self._determine_ui_state()

    def is_valid_international(self) -> bool:
        """The strict gatekeeper logic."""
        if self.origin_iata == "NLK": return True
        if self.terminal in DOMESTIC_TERMINALS: return False
        if self.country_code == "au": return False
        if any(k in self.ac_model.upper() for k in SMALL_AIRCRAFT_FILTER): return False
        return True

    def format_time(self, total_minutes: int) -> str:
        h, m = divmod(abs(total_minutes), 60)
        return f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"

    # --- Internal Logic Methods ---
    def _parse_times(self, arr_node: dict, dep_node: dict) -> Tuple[datetime, datetime, str]:
        s_dt_raw = arr_node.get("scheduledTime", {}).get("local")
        s_dt = pd.to_datetime(s_dt_raw).replace(tzinfo=None) if s_dt_raw else datetime.min
        s_dt = TIMEZONE.localize(s_dt) if s_dt != datetime.min else self.now
        
        best_dt, t_type = s_dt, "scheduled"
        for key, label in (("actualTime", "actual"), ("revisedTime", "revised")):
            raw = arr_node.get(key, {}).get("local")
            if raw:
                dt = TIMEZONE.localize(pd.to_datetime(raw).replace(tzinfo=None))
                best_dt, t_type = dt, label
                break

        # Fake Estimate Defense
        is_airborne = self.status_raw in ["en route", "enroute", "departed", "approaching", "active", "airborne"] or dep_node.get("actualTime")
        if t_type == "revised" and abs((best_dt - s_dt).total_seconds()) < 60 and not is_airborne:
            t_type = "scheduled"

        return s_dt, best_dt, t_type

    def _determine_if_landed(self) -> bool:
        if self.is_canceled: return False
        if self.status_raw in ("landed", "arrived"): return True
        if self.mins_to_arrival <= 0 and self.time_type in ("actual", "revised"): return True
        return False

    def _determine_ui_state(self) -> dict:
        """Maps business logic directly to a UI theme."""
        if self.is_canceled:
            is_old_cancel = (self.now - self.s_dt).total_seconds() / 60 > 15
            theme = THEMES["CANCELED"] if is_old_cancel else THEMES["CANCELED_NEW"]
            return {"theme": theme, "text": "CANCELED"}
        
        if self.is_landed:
            mins_ago = abs(self.mins_to_arrival)
            time_to_store = PAX_TO_STORE_MINS - mins_ago
            
            if time_to_store > 0:
                return {"theme": THEMES["AT_STORE"], "text": f"🏃 At Store ~{time_to_store}m"}
            elif mins_ago <= RECENT_LANDED_MAX:
                return {"theme": THEMES["LANDED_FRESH"], "text": f"Landed {self.format_time(mins_ago)} ago"}
            else:
                return {"theme": THEMES["LANDED_OLD"], "text": f"Landed {self.format_time(mins_ago)} ago"}

        # Not Landed, Not Cancelled
        if self.time_type == "scheduled" and self.mins_to_arrival <= 0:
            return {"theme": THEMES["NO_UPDATE"], "text": "NO UPDATE"}
        if self.delay_hours >= 12:
            return {"theme": THEMES["SEVERE_DELAY"], "text": "SEVERE DELAY"}
        if self.mins_to_arrival < 25:
            return {"theme": THEMES["INCOMING_HOT"], "text": f"In {self.format_time(self.mins_to_arrival)}"}
        
        return {"theme": THEMES["INCOMING"], "text": f"In {self.format_time(self.mins_to_arrival)}"}

    def _generate_logo_url(self) -> str:
        prefix = "".join(c for c in self.number if c.isalpha())[:2].upper()
        return f"https://pics.avs.io/200/200/{prefix}.png" if len(prefix) == 2 else ""

# =====================================================================
# 3. SERVICE LAYER (API & Caching)
# =====================================================================
@st.cache_data(ttl=API_DATA_TTL_SEC, show_spinner=False)
def fetch_flight_data(anchor: str, from_time: str, to_time: str) -> List[dict]:
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{AIRPORT_ICAO}/{from_time}/{to_time}"
    headers = {"X-RapidAPI-Key": st.secrets["X_RAPIDAPI_KEY"], "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"}
    try:
        r = requests.get(url, headers=headers, params={"direction": "Arrival", "withCancelled": "true", "withCodeshared": "false"}, timeout=10)
        r.raise_for_status()
        st.session_state.api_last_hit = datetime.now(TIMEZONE)
        return r.json().get("arrivals", [])
    except Exception as e:
        st.session_state.api_error = str(e)
        return []

@st.cache_data(show_spinner=False)
def fetch_aircraft_photo(reg: str) -> str:
    if not reg: return "NOT_FOUND"
    try:
        r = requests.get(f"https://api.planespotters.net/pub/photos/reg/{reg}", headers={"User-Agent": "BNE-Board-App/2.0"}, timeout=3.0)
        if r.status_code == 200 and r.json().get("photos"):
            return r.json()["photos"][0]["thumbnail_large"]["src"]
    except: pass
    return "NOT_FOUND"

def get_anchor_time(now: datetime) -> str:
    """Generates a stable cache key string based on TTL intervals."""
    epoch = datetime(2000, 1, 1, tzinfo=TIMEZONE)
    delta_seconds = int((now - epoch).total_seconds())
    bucket = (delta_seconds // API_DATA_TTL_SEC) * API_DATA_TTL_SEC
    return (epoch + timedelta(seconds=bucket)).strftime("%Y-%m-%dT%H:%M")

# =====================================================================
# 4. PRESENTATION LAYER (UI Rendering)
# =====================================================================
def render_flight_card(f: Flight, index: int):
    """Generates the HTML for a single flight."""
    mid = f"z_{index}"
    t = f.ui_state["theme"]
    tag = "Act" if (f.is_landed or f.time_type == "actual") else ("Est" if f.time_type == "revised" else "Sch")
    time_color = "#7DD3FC" if tag == "Act" else ("#E2E8F0" if tag == "Est" else "#94A3B8")
    
    # Text displays
    check_board_html = ' <span style="color:#FBBF24; font-size:0.75em; font-weight:700;">⚠️ Check Board</span>' if (tag == "Sch" and not f.is_canceled) else ""
    if tag == "Sch" and not f.is_canceled:
        time_display = f'<span class="mono" style="color:#94A3B8;">Sch {f.s_dt.strftime("%H:%M")}</span>{check_board_html}'
    else:
        time_display = f'<span class="mono" style="color:#94A3B8;">Sch {f.s_dt.strftime("%H:%M")}</span> • <span class="mono" style="color:{time_color}; font-weight:700;">{tag} {f.best_dt.strftime("%H:%M")}</span>'

    # Image processing
    has_photo = f.photo_url != "NOT_FOUND"
    img_html = (
        f'<div class="flip-container" style="filter:{t["filter"]};">'
        f'<label for="{mid}" style="cursor:pointer; display:block; width:100%; height:100%;">'
        f'<img src="{f.logo_url}" class="flip-img logo-layer" style="border-color:{t["border"]};"/>'
        f'<img src="{f.photo_url}" class="flip-img photo-layer" style="border-color:{t["border"]};"/>'
        f'</label></div>'
    ) if has_photo else (
        f'<div class="flip-container" style="filter:{t["filter"]};">'
        f'<img src="{f.logo_url}" class="flip-img" style="border-color:{t["border"]}; background:#FFF; padding:4px; object-fit:contain; border-radius:8px;"/>'
        f'</div>'
    )
    zoom_src = f.photo_url if has_photo else f.logo_url

    st.markdown(f"""
    <div class="flight-card" style="border-left-color:{t['border']}; background-color:{t['bg']}; opacity:{t['opacity']};">
        {img_html}
        <div class="info-col">
            <div style="font-size:1.1em; font-weight:700;">{f.number}<span style="font-size:0.7em; color:#94A3B8; margin-left:8px;">{f.origin_city}</span></div>
            <div style="font-size:0.7em; color:#CBD5E1; margin: 1px 0;">{f.ac_text[:25]}</div>
            <div style="font-size:0.8em; color:#94A3B8;">{time_display}</div>
        </div>
        <div class="status-col">
            <div style="font-size:0.6em; color:#94A3B8; font-weight:700; letter-spacing:1px;">GATE</div>
            <div class="mono" style="font-size:1.85em; font-weight:700; line-height:1;">{f.gate}</div>
            <div style="font-size:0.85em; font-weight:700; color:{t['text']}; margin-top:2px;">{f.ui_state['text']}</div>
        </div>
    </div>
    <input type="checkbox" id="{mid}" class="img-zoom-chk" style="display:none;">
    <div class="img-zoom-modal">
        <label for="{mid}" class="img-zoom-close-bg"></label>
        <label for="{mid}" class="close-btn">&times;</label>
        <img src="{zoom_src}"/>
    </div>
    """, unsafe_allow_html=True)

# =====================================================================
# 5. MAIN EXECUTION (The Orchestrator)
# =====================================================================
def main():
    st.set_page_config(page_title="BNE Pro Arrivals", page_icon="✈️", layout="centered")
    
    # Apply CSS
    with open("style.css", "w") as f: pass # Placeholder if external CSS is used. Inline below:
    st.markdown(f"""
    <meta http-equiv="refresh" content="{UI_REFRESH_SEC}">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=JetBrains+Mono:wght@600&display=swap');
        #MainMenu, header {{visibility: hidden;}}
        .block-container {{padding-top: 1rem; font-family: 'Inter', sans-serif; max-width: 700px;}}
        .mono {{ font-family: 'JetBrains Mono', monospace; letter-spacing: -0.5px; }}
        .flip-container {{ position: relative; width: 55px; height: 55px; margin-right: 12px; flex-shrink: 0; }}
        .flip-img {{ position: absolute; top: 0; left: 0; width: 55px; height: 55px; border-radius: 8px; border: 2.5px solid #475569; transition: opacity 1s ease-in-out; box-sizing: border-box; }}
        @keyframes logoFade {{ 0%, 45% {{ opacity: 1; }} 55%, 100% {{ opacity: 0; }} }}
        @keyframes photoFade {{ 0%, 45% {{ opacity: 0; }} 55%, 95% {{ opacity: 1; }} 100% {{ opacity: 0; }} }}
        .logo-layer {{ animation: logoFade 10s infinite; background: #FFFFFF; padding: 4px; object-fit: contain !important; border-radius: 8px; z-index: 2; }}
        .photo-layer {{ animation: photoFade 10s infinite; object-fit: cover !important; z-index: 1; }}
        .flight-card {{ border-radius: 10px; padding: 10px 14px; margin-bottom: 6px; display: flex; align-items: center; color: white; box-shadow: 0 4px 10px rgba(0,0,0,0.2); border-left: 5px solid transparent; transition: opacity 0.3s ease; }}
        .info-col {{ flex-grow: 1; min-width: 0; }}
        .status-col {{ text-align: right; min-width: 120px; display: flex; flex-direction: column; justify-content: center; }}
        .gap-bar {{ background-color: #0F172A; border: 1px dashed #475569; border-left: 5px solid transparent; border-radius: 8px; padding: 8px 14px; margin: 4px 0 10px 0; text-align: center; color: #94A3B8; font-weight: 600; font-size: 0.85em; box-sizing: border-box; }}
        .gap-active {{ background-color: #064E3B; border-color: #10B981; border-left-color: #10B981; color: #A7F3D0; }}
        .img-zoom-modal {{ display: none; position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; background: rgba(15,23,42,0.92); z-index: 10000; align-items: center; justify-content: center; backdrop-filter: blur(10px); }}
        .img-zoom-chk:checked + .img-zoom-modal {{ display: flex !important; }}
        .img-zoom-modal img {{ max-width: 90%; max-height: 80%; border-radius: 12px; border: 2px solid #475569; object-fit: contain; z-index: 10001; }}
        .img-zoom-close-bg {{ position: absolute; top: 0; left: 0; width: 100%; height: 100%; cursor: pointer; z-index: 10000; }}
        .close-btn {{ position: absolute; top: 20px; right: 30px; color: white; font-size: 3.5em; font-weight: bold; cursor: pointer; z-index: 10002; line-height: 1; }}
    </style>
    """, unsafe_allow_html=True)

    # State & Header
    if "api_last_hit" not in st.session_state: st.session_state.api_last_hit = None
    if "api_error" not in st.session_state: st.session_state.api_error = None
    now = datetime.now(TIMEZONE)

    c1, c2 = st.columns([2, 1])
    with c1: st.subheader("✈️ Arrivals")
    with c2:
        st.markdown(f'<div style="font-size:0.8em;color:#94A3B8;text-align:right;margin-top:5px;">🕒 Live: {now.strftime("%H:%M:%S")}</div>', unsafe_allow_html=True)
        api_t = st.session_state.get("api_last_hit")
        st.markdown(f'<div style="font-size:0.7em;color:#64748B;text-align:right;">API: {api_t.strftime("%H:%M") if api_t else "--:--"}</div>', unsafe_allow_html=True)

    # Fetch Data
    anchor = get_anchor_time(now)
    from_str = (now - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M")
    to_str = (now + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M")
    
    raw_data = fetch_flight_data(anchor, from_str, to_str)
    
    if st.session_state.api_error:
        st.error(f"⚠️ API Error — {st.session_state.api_error}")
        st.session_state.api_error = None

    if not raw_data:
        st.info("⏳ Synchronizing radar... data will appear on next refresh.")
        st.stop()

    # Parse, Filter & Deduplicate
    flight_dict = {}
    for raw_f in raw_data:
        f = Flight(raw_f, now)
        if f.is_valid_international() and f.number not in flight_dict:
            flight_dict[f.number] = f
    
    flights = list(flight_dict.values())

    # Concurrent Photo Fetching
    unique_regs = {f.ac_reg for f in flights if f.ac_reg}
    with ThreadPoolExecutor(max_workers=IMAGE_WORKERS) as executor:
        photo_results = list(executor.map(fetch_aircraft_photo, unique_regs))
    photo_map = dict(zip(unique_regs, photo_results))
    
    for f in flights:
        f.photo_url = photo_map.get(f.ac_reg, "NOT_FOUND")

    # Generate Gaps
    active_flights = sorted([f for f in flights if not f.is_landed and not f.is_canceled], key=lambda x: x.best_dt)
    gaps = []
    if active_flights:
        windows = [(now, active_flights[0].best_dt)] + [(active_flights[i].best_dt, active_flights[i+1].best_dt) for i in range(len(active_flights)-1)]
        for t1, t2 in windows:
            if t2 <= now: continue
            g_mins = int((t2 - max(t1, now)).total_seconds() / 60)
            if (t2 - t1).total_seconds() / 60 >= GAP_MIN_MINUTES and g_mins >= GAP_DISPLAY_MIN:
                is_active = t1 <= now
                cls, lbl = ("gap-bar gap-active", "🟢 ACTIVE") if is_active else ("gap-bar", "🔄")
                html = f'<div class="{cls}">{lbl} {format_hm(g_mins)} GAP <span style="opacity:0.6; font-weight:400; margin-left:8px;">({max(t1, now).strftime("%H:%M")}–{t2.strftime("%H:%M")})</span></div>'
                gaps.append({"is_gap": True, "dt": t1, "html": html})

    # Master Sort
    display_items = [{"is_gap": False, "obj": f} for f in flights] + gaps
    
    def sort_key(item):
        if item["is_gap"]: return (1, item["dt"].timestamp())
        f = item["obj"]
        if f.is_canceled: return (2, f.s_dt.timestamp())
        if f.is_landed and abs(f.mins_to_arrival) <= RECENT_LANDED_MAX: return (0, -f.best_dt.timestamp())
        if f.is_landed: return (2, -f.best_dt.timestamp())
        return (1, f.best_dt.timestamp())

    display_items.sort(key=sort_key)

    # Render Screen
    for i, item in enumerate(display_items):
        if item["is_gap"]:
            st.markdown(item["html"], unsafe_allow_html=True)
        else:
            f = item["obj"]
            if not f.is_canceled: render_flight_card(f, i)

    # Render Cancelled Section
    cancelled_flights = [item["obj"] for item in display_items if not item["is_gap"] and item["obj"].is_canceled]
    if cancelled_flights:
        st.markdown("<hr style='margin:15px 0 8px 0; opacity:0.2;'><div style='color:#F87171; font-size:0.85em; font-weight:700; margin-bottom:5px;'>❌ Canceled</div>", unsafe_allow_html=True)
        for i, f in enumerate(cancelled_flights):
            render_flight_card(f, i + 999) # Offset index for unique modal IDs

    st.markdown("<div style='text-align:center; color:#475569; font-size:0.65em; margin-top:20px;'>Dev: Phillip Yeh | V10.0 (Clean Architecture)</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()
