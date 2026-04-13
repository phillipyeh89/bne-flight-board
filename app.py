import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time

# 1. 頁面基礎設定
st.set_page_config(page_title="BNE Flight Board PRO", page_icon="✈️", layout="centered")

# 自動重整機制 (20分鐘)
st.markdown('<meta http-equiv="refresh" content="1200">', unsafe_allow_html=True)

# 2. 注入強化版 CSS (包含 Heartbeat 動畫)
st.markdown("""
<style>
    .flight-card {
        padding: 16px 20px;
        margin-bottom: 15px;
        border-radius: 12px;
        color: #F8FAFC;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        font-family: 'Inter', sans-serif;
        border: 1px solid rgba(255, 255, 255, 0.05);
    }
    .gate-text { font-size: 2.5em; font-weight: 800; line-height: 1; }
    .time-text { font-size: 1.6em; font-weight: 700; margin-top: 6px; }
    .label-tag {
        background-color: rgba(255, 255, 255, 0.2);
        padding: 4px 10px; border-radius: 6px;
        font-size: 0.85em; font-weight: 600; margin-bottom: 12px;
        display: inline-block; text-transform: uppercase;
    }
    
    /* 狀態顏色 */
    .status-normal { background: linear-gradient(135deg, #334155, #1E293B); }
    .status-soon { background: linear-gradient(135deg, #D97706, #B45309); }
    .status-urgent { background: linear-gradient(135deg, #E11D48, #BE123C); }
    .status-landed-new { background: linear-gradient(135deg, #059669, #047857); }
    .status-landed-old { background: #0F172A; color: #64748B; border: 1px dashed #334155; }
    
    /* 提早開店警報 (紫色呼吸燈) */
    @keyframes pulse-purple {
        0% { box-shadow: 0 0 0 0 rgba(139, 92, 246, 0.5); }
        70% { box-shadow: 0 0 0 10px rgba(139, 92, 246, 0); }
        100% { box-shadow: 0 0 0 0 rgba(139, 92, 246, 0); }
    }
    .status-purple { background: linear-gradient(135deg, #8B5CF6, #6D28D9); animation: pulse-purple 2s infinite; border: 1px solid #A78BFA; }

    /* Heartbeat Indicator */
    .heartbeat { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 5px; }
    .hb-live { background-color: #22C55E; box-shadow: 0 0 8px #22C55E; }
    .hb-offline { background-color: #EF4444; box-shadow: 0 0 8px #EF4444; }
</style>
""", unsafe_allow_html=True)

# 3. 初始化 Session State (用於斷網恢復機制)
if 'last_good_data' not in st.session_state:
    st.session_state.last_good_data = []
if 'last_success_time' not in st.session_state:
    st.session_state.last_success_time = None

# 4. 強化版 API 抓取函式 (帶有錯誤處理與快取)
@st.cache_data(ttl=1200) # 嚴格 20 分鐘 TTL
def fetch_flight_data(from_time, to_time):
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/YBBN/{from_time}/{to_time}"
    headers = {
        "X-RapidAPI-Key": st.secrets["X_RAPIDAPI_KEY"],
        "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
    }
    params = {"direction": "Arrival", "withCancelled": "false", "withCodeshared": "false"}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=12)
        response.raise_for_status()
        data = response.json().get('arrivals', [])
        # 更新備份資料
        st.session_state.last_good_data = data
        st.session_state.last_success_time = datetime.now(pytz.timezone('Australia/Brisbane'))
        return data, True
    except Exception:
        # 抓取失敗時，回傳備份資料與錯誤標記
        return st.session_state.last_good_data, False

# 5. 時間處理
aest = pytz.timezone('Australia/Brisbane')
now_aest = datetime.now(aest)
from_t = (now_aest - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")
to_t = (now_aest + timedelta(hours=10)).strftime("%Y-%m-%dT%H:%M")

def format_hm(total_minutes):
    h, m = total_minutes // 60, total_minutes % 60
    return f"{m:02d}m" if h == 0 else f"{h:02d}h{m:02d}m"

# 6. 介面標題區
col1, col2 = st.columns([2, 1])
with col1:
    st.title("✈️ Arrivals Board")
    # Heartbeat UI (3)
    if st.session_state.last_success_time:
        diff = (now_aest - st.session_state.last_success_time).total_seconds() / 60
        is_fresh = diff < 25
        hb_class = "hb-live" if is_fresh else "hb-offline"
        status_text = "Live" if is_fresh else "Offline Mode (Delayed)"
        st.markdown(f'<div><span class="heartbeat {hb_class}"></span><span style="font-size:0.85em; opacity:0.7;">System Status: {status_text} (Last: {st.session_state.last_success_time.strftime("%H:%M")})</span></div>', unsafe_allow_html=True)
    else:
        st.caption("Initializing system...")

with col2:
    if st.button("🔄 Refresh", use_container_width=True):
        fetch_flight_data.clear()
        st.rerun()
    st.markdown('<div style="font-size:0.7em; opacity:0.5; text-align:center;">Daily API quota protected (20m lock)</div>', unsafe_allow_html=True)

# 7. 獲取資料
flights_raw, is_success = fetch_flight_data(from_t, to_t)

if not is_success and not flights_raw:
    st.error("Connection Error: Unable to fetch data and no backup available.")
    st.stop()

processed_flights = []

for f in flights_raw:
    flight_num = f.get('number', 'N/A')
    
    # 地名處理
    dep = f.get('departure') or {}
    mv = f.get('movement') or {}
    api = dep.get('airport') or mv.get('airport') or {}
    origin = api.get('municipalityName') or api.get('shortName') or api.get('iata') or "Unknown"
    country = str(api.get('countryCode', '')).strip().lower()
    
    # 機型處理
    ac_model = (f.get('aircraft') or {}).get('model', '')
    ac_html = f'<div style="font-size: 0.85em; opacity: 0.6; margin-top: 4px;">✈️ {ac_model}</div>' if ac_model else ''
    
    # 時間解析
    time_candidates = []
    sch_raw = None
    
    for node_name in ['arrival', 'movement']:
        node = f.get(node_name, {})
        if not isinstance(node, dict): continue
        if not sch_raw:
            sch_raw = node.get('scheduledTime', {}).get('local') or node.get('scheduledTimeLocal')
        
        # 優先順序: Actual > Revised > Scheduled
        for k in ['actualTime', 'revisedTime', 'scheduledTime']:
            t_obj = node.get(k)
            if isinstance(t_obj, dict) and t_obj.get('local'): time_candidates.append(t_obj.get('local'))
        for k in ['actualTimeLocal', 'estimatedTimeLocal', 'scheduledTimeLocal']:
            if node.get(k): time_candidates.append(node.get(k))

    valid_t = [t for t in time_candidates if isinstance(t, str) and len(t) > 5]
    if not valid_t: continue
    
    try:
        dt = pd.to_datetime(valid_t[0]).to_pydatetime().replace(tzinfo=None)
        dt = aest.localize(dt)
        
        # 表定時間處理
        sch_str = ""
        if sch_raw:
            s_dt = pd.to_datetime(sch_raw).to_pydatetime().replace(tzinfo=None)
            s_dt = aest.localize(s_dt)
            sch_str = f'<span style="font-size: 0.75em; opacity: 0.7; margin-left: 8px;">(Sch {s_dt.strftime("%H:%M")})</span>'
            
            # 幽靈航班與國內線過濾 (Terminal + Country + Reality Check)
            terminal = str((f.get('arrival') or {}).get('terminal', '')).upper()
            if terminal in ['D', 'DOM'] or country == 'au': continue
            
            diff_h = (dt - s_dt).total_seconds() / 3600
            if diff_h < -2 or diff_h > 12: continue
    except: continue

    # 狀態計算
    diff_min = int((dt - now_aest).total_seconds() / 60)
    is_landed = (f.get('status', '').lower() in ['landed', 'arrived']) or (diff_min <= 0)
    
    # 早開店提醒 (02:30 - 04:10)
    is_early = (dt.hour == 2 and dt.minute >= 30) or (dt.hour == 3) or (dt.hour == 4 and dt.minute <= 10)
    
    css, tag_html, landed_m = "status-normal", "", 0
    if is_early: tag_html = '<div class="label-tag">⏰ Early Prep Required</div>'

    if is_landed:
        landed_m = max(0, -diff_min)
        css = "status-landed-new" if landed_m <= 40 else "status-landed-old"
        display = f"Landed {format_hm(landed_m)} ago ({dt.strftime('%H:%M')})"
    else:
        m_left = max(0, diff_min)
        if is_early: css = "status-purple"
        elif m_left < 25: css = "status-urgent"
        elif m_left <= 60: css = "status-soon"
        display = f"In {format_hm(m_left)} ({dt.strftime('%H:%M')})"

    processed_flights.append({
        'html': f"""
<div class
