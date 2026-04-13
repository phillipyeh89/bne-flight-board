import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, time
import pytz

# 設定頁面與手機直式螢幕最佳化
st.set_page_config(page_title="BNE Flight Board", page_icon="✈️", layout="centered")

# 注入 V5.2 旗艦醒目版 CSS (新增 Act Time 高亮設計)
st.markdown("""
<style>
    /* 全域背景微調 */
    .stApp { background-color: #0F172A; }

    /* 客製化按鈕 */
    .stButton > button {
        background-color: #1E293B !important;
        color: #F8FAFC !important;
        border: 1px solid #334155 !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        padding: 0.5rem 1rem !important;
        transition: all 0.2s !important;
    }
    .stButton > button:hover {
        border-color: #64748B !important;
        background-color: #334155 !important;
    }

    /* 統一暗色卡片設計 */
    .uniform-card {
        background-color: #1E293B;
        border: 1px solid #334155;
        border-radius: 16px;
        padding: 18px 20px;
        margin-bottom: 16px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
        font-family: 'Inter', -apple-system, sans-serif;
        color: #F8FAFC;
        transition: transform 0.2s;
    }
    .uniform-card:hover {
        transform: translateY(-2px);
        border-color: #475569;
    }

    /* 文字排版系統 */
    .flight-route { font-size: 1.5em; font-weight: 800; letter-spacing: -0.02em; color: #FFFFFF; }
    .flight-num { font-size: 1.1em; font-weight: 600; color: #94A3B8; margin-top: 2px; }
    .flight-meta { font-size: 0.85em; font-weight: 500; color: #64748B; margin-top: 8px; display: flex; align-items: center; gap: 8px; }
    
    /* 實際時間 (Act Time) 專屬高亮設計 */
    .act-time {
        color: #7DD3FC; /* 明亮天空藍 */
        background: rgba(14, 165, 233, 0.15);
        border: 1px solid rgba(14, 165, 233, 0.4);
        padding: 2px 8px;
        border-radius: 6px;
        font-weight: 700;
        letter-spacing: 0.02em;
        box-shadow: 0 0 8px rgba(14, 165, 233, 0.1);
    }

    .gate-text { font-size: 3em; font-weight: 800; line-height: 1; color: #FFFFFF; text-align: right; letter-spacing: -0.03em; margin-top: 4px; }
    .gate-label { font-size: 0.75em; font-weight: 700; color: #64748B; text-transform: uppercase; letter-spacing: 0.1em; text-align: right; }

    /* 膠囊狀態標籤 (高反差醒目版) */
    .badge {
        padding: 6px 14px;
        border-radius: 99px;
        font-size: 0.9em;
        font-weight: 800;
        display: inline-flex;
        align-items: center;
        gap: 6px;
        white-space: nowrap;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .badge-landed { background: #10B981; color: #FFFFFF; box-shadow: 0 0 10px rgba(16, 185, 129, 0.4); }
    .badge-archived { background: #334155; color: #E2E8F0; border: 1px solid #475569; }
    .badge-soon { background: #F59E0B; color: #FFFFFF; box-shadow: 0 0 10px rgba(245, 158, 11, 0.4); }
    .badge-urgent { background: #EF4444; color: #FFFFFF; box-shadow: 0 0 10px rgba(239, 68, 68, 0.6); animation: pulse-red 2s infinite; }
    .badge-purple { background: #8B5CF6; color: #FFFFFF; box-shadow: 0 0 10px rgba(139, 92, 246, 0.4); }
    .badge-normal { background: #0EA5E9; color: #FFFFFF; box-shadow: 0 0 10px rgba(14, 165, 233, 0.3); }

    /* 緊急航班紅色呼吸燈 */
    @keyframes pulse-red {
        0% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.6); }
        70% { box-shadow: 0 0 0 12px rgba(239, 68, 68, 0); }
        100% { box-shadow: 0 0 0 0 rgba(239, 68, 68, 0); }
    }

    /* 離櫃空檔橫幅 */
    .gap-banner {
        background: #0F172A;
        border-left: 4px solid #10B981;
        border-radius: 8px;
        padding: 14px 20px;
        margin: 8px 0 24px 0;
        display: flex;
        justify-content: space-between;
        align-items: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.2);
    }
    .gap-banner.future {
        border-left: 4px solid #475569;
        background: rgba(30, 41, 59, 0.5);
    }
    .gap-title { font-size: 1.05em; font-weight: 700; color: #A7F3D0; }
    .gap-banner.future .gap-title { color: #94A3B8; }
    .gap-time { font-size: 0.9em; font-weight: 600; color: #64748B; }

    .update-time { font-size: 0.85em; color: #64748B; text-align: center; margin-bottom: 8px; font-weight: 500; }
    
    /* 照片容器 */
    .photo-container {
        width: 100px; 
        height: 100px; 
        border-radius: 12px; 
        overflow: hidden; 
        flex-shrink: 0; 
        box-shadow: 0 4px 8px rgba(0,0,0,0.4); 
        background: #0F172A;
    }
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
    st.title("✈️ Arrivals")
with col2:
    update_display = st.session_state.last_update_time.strftime('%H:%M') if st.session_state.last_update_time else "Just Now"
    st.markdown(f'<div class="update-time">🕒 Updated: {update_display}</div>', unsafe_allow_html=True)
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
    ac_display_parts = []
    if aircraft_model: ac_display_parts.append(aircraft_model)
    if aircraft_reg: ac_display_parts.append(f"({aircraft_reg})")
    ac_text = " ".join(ac_display_parts)
    
    if image_url:
        image_html = f'<div class="photo-container"><img src="{image_url}" style="width: 100%; height: 100%; object-fit: cover;" /></div>'
    else:
        image_html = '<div class="photo-container" style="display: flex; align-items: center; justify-content: center;"><span style="font-size: 2.5em; opacity: 0.3;">✈️</span></div>'

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
            sch_display = s_dt.strftime("%H:%M")
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
    
    badge_class = ""
    badge_text = ""
    landed_mins = 0
    
    is_early_prep = (dt.hour == 2 and dt.minute >= 30) or (dt.hour == 3) or (dt.hour == 4 and dt.minute <= 10)

    if is_landed:
        landed_mins = max(0, -time_diff_minutes)
        badge_text = f"Landed {format_hm(landed_mins)} ago"
        badge_class = "badge-landed" if landed_mins <= 60 else "badge-archived"
    else:
        minutes_left = max(0, time_diff_minutes)
        time_str = f"In {format_hm(minutes_left)}"
        
        if is_early_prep:
            badge_class = "badge-purple"
            badge_text = f"⏰ {time_str} (Prep)"
        elif minutes_left < 25:
            badge_class = "badge-urgent"
            badge_text = f"🔥 {time_str}"
        elif minutes_left <= 60:
            badge_class = "badge-soon"
            badge_text = time_str
        else:
            badge_class = "badge-normal"
            badge_text = time_str
            
    processed_flights.append({
        'is_gap': False,
        'num': flight_num,
        'origin': city,
        'sch_display': sch_display,
        'image_html': image_html,
        'ac_text': ac_text,
        'gate': gate,
        'actual_time': dt.strftime('%H:%M'),
        'is_landed': is_landed,
        'landed_mins': landed_mins,
        'badge_class': badge_class,
        'badge_text': badge_text,
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
    
    title_text = f"🟢 ACTIVE OFF-FLOOR ({gap_display} left)" if is_active else f"🔄 {gap_display} OFF-FLOOR WINDOW"
    css_ext = "" if is_active else "future"
    time_text = f"{display_start.strftime('%H:%M')} - {t_end.strftime('%H:%M')}"
    
    gap_html = f'<div class="gap-banner {css_ext}"><div class="gap-title">{title_text}</div><div class="gap-time">{time_text}</div></div>'
    
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
        
    # 動態組合表定時間，防呆處理
    sch_part = f"Sch {pf['sch_display']}" if pf['sch_display'] else ""
        
    card_html = f"""<div class="uniform-card">
<div style="display: flex; gap: 18px; align-items: center; width: 100%;">
{pf['image_html']}
<div style="flex-grow: 1; display: flex; flex-direction: column; justify-content: center;">
<div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 4px;">
<div class="flight-route">{pf['origin']}</div>
<div class="badge {pf['badge_class']}">{pf['badge_text']}</div>
</div>
<div class="flight-num">{pf['num']} <span style="opacity:0.5; font-weight:400; margin: 0 4px;">|</span> {pf['ac_text']}</div>
<div class="flight-meta">
    {sch_part} <span class="act-time">Act {pf['actual_time']}</span>
</div>
</div>
<div style="text-align: right; min-width: 80px; padding-left: 10px; border-left: 1px solid rgba(255,255,255,0.05); margin-left: 10px;">
<div class="gate-label">Gate</div>
<div class="gate-text">{pf['gate']}</div>
</div>
</div>
</div>"""

    st.markdown(card_html, unsafe_allow_html=True)
