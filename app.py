import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, time
import pytz

# ==========================================
# BNE Flight Board - V4.0 "Aero-Glass" Edition
# ==========================================

st.set_page_config(page_title="BNE Flight Board", page_icon="✈️", layout="centered")

# 注入全新 Glassmorphism CSS
st.markdown("""
<style>
    /* 全域背景微調 (如果在暗色模式下效果更好) */
    .stApp {
        background-color: #0B1120;
    }

    /* 客製化按鈕 - 科技感霓虹邊框 */
    .stButton > button {
        background: rgba(30, 41, 59, 0.5) !important;
        color: #E2E8F0 !important;
        border: 1px solid rgba(147, 197, 253, 0.3) !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        letter-spacing: 0.05em !important;
        padding: 0.5rem 1rem !important;
        transition: all 0.3s ease !important;
        backdrop-filter: blur(10px);
    }
    .stButton > button:hover {
        background: rgba(59, 130, 246, 0.2) !important;
        border: 1px solid rgba(147, 197, 253, 0.8) !important;
        box-shadow: 0 0 15px rgba(59, 130, 246, 0.3) !important;
    }

    /* Aero-Glass 航班卡片基礎設定 */
    .glass-card {
        display: flex;
        justify-content: space-between;
        align-items: center;
        background: rgba(30, 41, 59, 0.6);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 12px;
        padding: 14px 18px;
        margin-bottom: 14px;
        color: #F8FAFC;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .glass-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.3);
    }

    /* 側邊狀態光條 (Status Edge) */
    .edge-normal { border-left: 5px solid #475569; }
    .edge-soon { border-left: 5px solid #F59E0B; }
    .edge-urgent { border-left: 5px solid #EF4444; }
    .edge-landed-new { border-left: 5px solid #10B981; }
    .edge-landed-old { 
        border-left: 5px solid #334155; 
        opacity: 0.5; 
        background: rgba(15, 23, 42, 0.4);
    }
    
    @keyframes edge-pulse {
        0% { border-left: 5px solid rgba(139, 92, 246, 0.4); box-shadow: -5px 0 15px rgba(139, 92, 246, 0.1); }
        50% { border-left: 5px solid rgba(167, 139, 250, 1); box-shadow: -8px 0 20px rgba(167, 139, 250, 0.5); }
        100% { border-left: 5px solid rgba(139, 92, 246, 0.4); box-shadow: -5px 0 15px rgba(139, 92, 246, 0.1); }
    }
    .edge-purple { animation: edge-pulse 2s infinite; }

    /* 文字排版系統 */
    .flight-id { font-size: 1.3em; font-weight: 700; letter-spacing: 0.02em; color: #FFFFFF; }
    .flight-route { font-size: 1em; font-weight: 500; color: #94A3B8; margin-left: 8px; }
    .flight-meta { font-size: 0.85em; color: #64748B; margin-top: 4px; }
    .time-main { font-size: 1.6em; font-weight: 800; color: #F8FAFC; margin-top: 2px; }
    .time-sub { font-size: 0.8em; font-weight: 600; color: #94A3B8; text-transform: uppercase; letter-spacing: 0.05em; }
    
    .gate-box {
        text-align: center;
        background: rgba(15, 23, 42, 0.5);
        padding: 8px 14px;
        border-radius: 8px;
        border: 1px solid rgba(255, 255, 255, 0.03);
    }
    .gate-num { font-size: 2.2em; font-weight: 800; color: #FFFFFF; line-height: 1; }
    .gate-label { font-size: 0.7em; color: #64748B; text-transform: uppercase; font-weight: 700; margin-bottom: 4px; letter-spacing: 0.1em; }

    /* 警告標籤 */
    .alert-tag {
        background: rgba(239, 68, 68, 0.2);
        color: #FCA5A5;
        font-size: 0.75em;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
        margin-bottom: 6px;
        display: inline-block;
        border: 1px solid rgba(239, 68, 68, 0.3);
    }
    .prep-tag {
        background: rgba(139, 92, 246, 0.2);
        color: #C4B5FD;
        border: 1px solid rgba(139, 92, 246, 0.3);
    }

    /* 極簡空檔分隔線 (Minimal Gap Divider) */
    .gap-divider {
        display: flex;
        align-items: center;
        text-align: center;
        margin: 15px 0 20px 0;
        color: #10B981;
        font-weight: 700;
        font-size: 0.95em;
        letter-spacing: 0.05em;
    }
    .gap-divider::before, .gap-divider::after {
        content: '';
        flex: 1;
        border-bottom: 1px dashed rgba(16, 185, 129, 0.3);
    }
    .gap-divider:not(:empty)::before { margin-right: .5em; }
    .gap-divider:not(:empty)::after { margin-left: .5em; }
    
    .gap-future { color: #64748B; }
    .gap-future::before, .gap-future::after { border-bottom: 1px dashed rgba(100, 116, 139, 0.3); }

    .update-time { font-size: 0.85em; color: #64748B; text-align: center; margin-bottom: 8px; font-weight: 500; }
</style>
""", unsafe_allow_html=True)

if 'last_update_time' not in st.session_state:
    st.session_state.last_update_time = None

@st.cache_data(ttl=86400, show_spinner=False)
def fetch_aircraft_image(reg):
    if not reg: return ""
    try:
        url = f"https://api.planespotters.net/pub/photos/reg/{reg}"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            if data.get("photos") and len(data["photos"]) > 0:
                return data["photos"][0]["thumbnail_large"]["src"]
    except: pass
    return ""

@st.cache_data(ttl=60) 
def fetch_flight_data(from_time, to_time):
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/YBBN/{from_time}/{to_time}"
    querystring = {"direction": "Arrival", "withCancelled": "false", "withCodeshared": "false"}
    headers = {
        "X-RapidAPI-Key": st.secrets["X_RAPIDAPI_KEY"],
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }
    try:
        response = requests.get(url, headers=headers, params=querystring, timeout=10)
        response.raise_for_status()
        st.session_state.last_update_time = datetime.now(pytz.timezone('Australia/Brisbane'))
        return response.json().get('arrivals', [])
    except Exception as e:
        st.error(f"API Request Failed: {e}")
        return []

aest = pytz.timezone('Australia/Brisbane')
now_aest = datetime.now(aest)

from_time = (now_aest - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")
to_time = (now_aest + timedelta(hours=11)).strftime("%Y-%m-%dT%H:%M")

col1, col2 = st.columns([2, 1])
with col1:
    st.title("✈️ Arrivals Board")
with col2:
    update_display = st.session_state.last_update_time.strftime('%H:%M') if st.session_state.last_update_time else "Just Now"
    st.markdown(f'<div class="update-time">🕒 Last Updated: {update_display}</div>', unsafe_allow_html=True)
    if st.button("🔄 Refresh", use_container_width=True):
        fetch_flight_data.clear()
        st.rerun()

flights = fetch_flight_data(from_time, to_time)
if not flights:
    st.info("No flight data available in the current window.")
    st.stop()

processed_flights = []

def format_hm(total_minutes):
    h = total_minutes // 60
    m = total_minutes % 60
    return f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"

for f in flights:
    flight_num = f.get('number', 'N/A')
    
    dep = f.get('departure') or {}
    movement = f.get('movement') or {}
    airport_info = dep.get('airport') or movement.get('airport') or {}
    
    city = airport_info.get('municipalityName') or airport_info.get('name') or airport_info.get('iata') or airport_info.get('icao') or "Unknown"
    country_code = str(airport_info.get('countryCode', '')).strip().lower()
    
    aircraft_node = f.get('aircraft') or {}
    aircraft_model = aircraft_node.get('model', '')
    aircraft_reg = aircraft_node.get('reg', '')
    
    image_url = fetch_aircraft_image(aircraft_reg)
    ac_text = f"{aircraft_model} ({aircraft_reg})" if aircraft_model and aircraft_reg else aircraft_model or aircraft_reg
    
    # 圖片改為 80x80 方形，更契合新版版型
    if image_url:
        image_html = f'<div style="width: 80px; height: 80px; border-radius: 8px; overflow: hidden; flex-shrink: 0;"><img src="{image_url}" style="width: 100%; height: 100%; object-fit: cover;" /></div>'
    else:
        image_html = '<div style="width: 80px; height: 80px; border-radius: 8px; background: rgba(255,255,255,0.05); display: flex; align-items: center; justify-content: center; flex-shrink: 0;"><span style="font-size: 2em; opacity:0.3;">✈️</span></div>'

    time_candidates = []
    scheduled_time_raw = None
    for node_name in ['arrival', 'movement', 'departure']:
        node = f.get(node_name, {})
        if not isinstance(node, dict): continue
        if not scheduled_time_raw:
            scheduled_time_raw = node.get('scheduledTime', {}).get('local') or node.get('scheduledTimeLocal')
        for t_key in ['actualTime', 'revisedTime', 'scheduledTime']:
            t_obj = node.get(t_key)
            if isinstance(t_obj, dict) and t_obj.get('local'): time_candidates.append(t_obj.get('local'))
        for t_key in ['actualTimeLocal', 'estimatedTimeLocal', 'scheduledTimeLocal']:
            t_val = node.get(t_key)
            if t_val: time_candidates.append(t_val)
                
    if not scheduled_time_raw and f.get('scheduledTimeLocal'):
        scheduled_time_raw = f.get('scheduledTimeLocal')
    for t_key in ['actualTimeLocal', 'estimatedTimeLocal', 'scheduledTimeLocal']:
        t_val = f.get(t_key)
        if t_val: time_candidates.append(t_val)
    
    valid_times = [t for t in time_candidates if isinstance(t, str) and len(t) > 5]
    if not valid_times: continue
    best_time_str = valid_times[0]
    
    try:
        dt = pd.to_datetime(best_time_str).to_pydatetime()
        if dt.tzinfo is None: dt = aest.localize(dt)
        else: dt = dt.astimezone(aest)
    except: continue

    sch_display = ""
    if scheduled_time_raw:
        try:
            s_dt = pd.to_datetime(scheduled_time_raw).to_pydatetime()
            if s_dt.tzinfo is None: s_dt = aest.localize(s_dt)
            else: s_dt = s_dt.astimezone(aest)
            sch_display = f'<span style="opacity: 0.5; margin-left: 8px; font-weight: normal; font-size: 0.85em;">Sch {s_dt.strftime("%H:%M")}</span>'
        except: pass

    arr_node = f.get('arrival') or f.get('movement') or {}
    gate = arr_node.get('gate', 'TBA')
    terminal = str(arr_node.get('terminal', '')).strip().upper()
    if terminal == 'D' or terminal == 'DOM' or country_code == 'au': continue
        
    status = f.get('status', '').lower()
    time_diff_minutes = int((dt - now_aest).total_seconds() / 60)
    is_landed = status in ['landed', 'arrived'] or time_diff_minutes <= 0

    if scheduled_time_raw:
        try:
            s_dt_check = pd.to_datetime(scheduled_time_raw).to_pydatetime()
            if s_dt_check.tzinfo is None: s_dt_check = aest.localize(s_dt_check)
            else: s_dt_check = s_dt_check.astimezone(aest)
            diff_hours = (dt - s_dt_check).total_seconds() / 3600
            if diff_hours < -2 or diff_hours > 12: continue
            if not is_landed and (now_aest - s_dt_check).total_seconds() > 8 * 3600: continue  
        except: pass
    
    edge_class = "edge-normal"
    tags_html = ""
    landed_mins = 0
    time_label = ""
    time_value = ""
    
    is_early_prep = (dt.hour == 2 and dt.minute >= 30) or (dt.hour == 3) or (dt.hour == 4 and dt.minute <= 10)
    if is_early_prep: tags_html += '<span class="alert-tag prep-tag">⏰ EARLY PREP</span> '

    if is_landed:
        landed_mins = max(0, -time_diff_minutes)
        if landed_mins <= 60: edge_class = "edge-landed-new"
        else: edge_class = "edge-landed-old"
        time_label = "LANDED"
        time_value = f"{format_hm(landed_mins)} ago"
    else:
        minutes_left = max(0, time_diff_minutes)
        if is_early_prep: edge_class = "edge-purple"
        elif minutes_left < 25: 
            edge_class = "edge-urgent"
            tags_html += '<span class="alert-tag">🔥 IMMINENT</span> '
        elif minutes_left <= 60: edge_class = "edge-soon"
        time_label = "ARRIVING IN"
        time_value = format_hm(minutes_left)
            
    processed_flights.append({
        'is_gap': False,
        'num': flight_num,
        'origin': city,
        'sch_display': sch_display,
        'image_html': image_html,
        'ac_text': ac_text,
        'gate': gate,
        'time_label': time_label,
        'time_value': time_value,
        'actual_time': dt.strftime('%H:%M'),
        'is_landed': is_landed,
        'landed_mins': landed_mins,
        'edge': edge_class,
        'tags': tags_html,
        'dt': dt
    })

# === 離櫃空檔偵測系統 ===
future_flights = [pf for pf in processed_flights if not pf['is_landed']]
future_flights.sort(key=lambda x: x['dt'])

gaps = []
if future_flights:
    landed_flights = [pf for pf in processed_flights if pf['is_landed']]
    last_landed_time = max([pf['dt'] for pf in landed_flights]) if landed_flights else now_aest
    start_time = max(last_landed_time, now_aest)
    first_future = future_flights[0]['dt']
    
    if (first_future - start_time).total_seconds() / 60 >= 20: gaps.append((start_time, first_future))
    for i in range(len(future_flights) - 1):
        t1 = future_flights[i]['dt']
        t2 = future_flights[i+1]['dt']
        if (t2 - t1).total_seconds() / 60 >= 20: gaps.append((t1, t2))

for t_start, t_end in gaps:
    if t_end <= now_aest: continue 
    display_start = max(t_start, now_aest)
    gap_mins = int((t_end - display_start).total_seconds() / 60)
    if gap_mins < 5: continue 
    
    is_active = t_start <= now_aest
    gap_display = format_hm(gap_mins)
    css_ext = "" if is_active else "gap-future"
    icon = "🟢" if is_active else "⏱️"
    
    # 全新的極簡分隔線設計
    gap_html = f'<div class="gap-divider {css_ext}"> {icon} {gap_display} OFF-FLOOR WINDOW ({display_start.strftime("%H:%M")} - {t_end.strftime("%H:%M")}) </div>'
    
    processed_flights.append({
        'is_gap': True,
        'html': gap_html,
        'time_key': t_start.timestamp() + 1 
    })

# === 排序與渲染 ===
def custom_sort(pf):
    if pf['is_gap']: return (1, pf['time_key'])
    if pf['is_landed']:
        return (0, -pf['dt'].timestamp()) if pf['landed_mins'] <= 60 else (2, -pf['dt'].timestamp())
    return (1, pf['dt'].timestamp())

processed_flights.sort(key=custom_sort)

for pf in processed_flights:
    if pf['is_gap']:
        st.markdown(pf['html'], unsafe_allow_html=True)
        continue
        
    # 全新的 HTML 結構：Aero-Glass Layout
    card_html = f"""<div class="glass-card {pf['edge']}">
<div style="display: flex; gap: 16px; align-items: center; flex: 1;">
{pf['image_html']}
<div style="display: flex; flex-direction: column;">
<div style="margin-bottom: 2px;">{pf['tags']}</div>
<div><span class="flight-id">{pf['num']}</span><span class="flight-route">{pf['origin']}</span></div>
<div class="flight-meta">✈️ {pf['ac_text']} {pf['sch_display']}</div>
</div>
</div>
<div style="display: flex; gap: 24px; align-items: center;">
<div style="text-align: right;">
<div class="time-sub">{pf['time_label']}</div>
<div class="time-main">{pf['time_value']}</div>
<div style="font-size: 0.85em; color: #94A3B8; margin-top: 2px;">Act {pf['actual_time']}</div>
</div>
<div class="gate-box">
<div class="gate-label">Gate</div>
<div class="gate-num">{pf['gate']}</div>
</div>
</div>
</div>"""

    st.markdown(card_html, unsafe_allow_html=True)
