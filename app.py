import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

# 設定頁面與手機直式螢幕最佳化
st.set_page_config(page_title="BNE 航班看板", page_icon="✈️", layout="centered")

# 自動更新機制：每 20 分鐘 (1200秒) 自動重整
st.markdown('<meta http-equiv="refresh" content="1200">', unsafe_allow_html=True)

# 注入自訂 CSS
st.markdown("""
<style>
    .flight-card {
        padding: 18px;
        margin-bottom: 15px;
        border-radius: 12px;
        color: white;
        box-shadow: 0 4px 8px rgba(0,0,0,0.3);
        font-family: 'sans-serif';
    }
    .gate-text {
        font-size: 2.3em;
        font-weight: 900;
        line-height: 1.1;
    }
    .time-text {
        font-size: 1.8em;
        font-weight: bold;
        margin-top: 5px;
    }
    .label-tag {
        background-color: rgba(255, 255, 255, 0.25);
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 0.9em;
        font-weight: bold;
        margin-bottom: 8px;
        display: inline-block;
    }
    .status-normal { background-color: #2E2E2E; }
    .status-landed { background-color: #616161; color: #E0E0E0; opacity: 0.8; }
    .status-orange { background-color: #F57C00; }
    .status-red { background-color: #D32F2F; }
    
    @keyframes flashAlert {
        0% { background-color: #8E24AA; box-shadow: 0 0 10px #8E24AA; }
        50% { background-color: #D500F9; box-shadow: 0 0 25px #D500F9; }
        100% { background-color: #8E24AA; box-shadow: 0 0 10px #8E24AA; }
    }
    .status-purple {
        animation: flashAlert 1.5s infinite;
        border: 2px solid #EA80FC;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=1200)
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
        return response.json().get('arrivals', [])
    except Exception as e:
        st.error(f"API 請求失敗：{e}")
        return []

aest = pytz.timezone('Australia/Brisbane')
now_aest = datetime.now(aest)

from_time = (now_aest - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")
to_time = (now_aest + timedelta(hours=10)).strftime("%Y-%m-%dT%H:%M")

col1, col2 = st.columns([2, 1])
with col1:
    st.title("✈️ BNE 航班看板")
with col2:
    if st.button("🔄 手動更新", use_container_width=True):
        fetch_flight_data.clear()
        st.rerun()

flights = fetch_flight_data(from_time, to_time)

if not flights:
    st.info("目前視窗內無航班資料。")
    st.stop()

processed_flights = []

for f in flights:
    flight_num = f.get('number', 'N/A')
    
    # 完美修復版：同時搜尋 departure 與 movement 節點
    dep = f.get('departure') or {}
    movement = f.get('movement') or {}
    airport_info = dep.get('airport') or movement.get('airport') or {}
    
    city = airport_info.get('municipalityName')
    name = airport_info.get('name')
    iata = airport_info.get('iata')
    icao = airport_info.get('icao')
    
    if city: origin = city
    elif name: origin = name
    elif iata: origin = iata
    elif icao: origin = icao
    else: origin = "Unknown"
    
    time_candidates = []
    for node_name in ['arrival', 'movement', 'departure']:
        node = f.get(node_name, {})
        if not isinstance(node, dict): continue
        time_candidates.extend([node.get('actualTimeLocal'), node.get('scheduledTimeLocal')])
        for t_key in ['actualTime', 'scheduledTime', 'revisedTime']:
            t_obj = node.get(t_key)
            if isinstance(t_obj, dict):
                time_candidates.append(t_obj.get('local'))
                
    time_candidates.extend([f.get('actualTimeLocal'), f.get('scheduledTimeLocal')])
    
    valid_times = [t for t in time_candidates if isinstance(t, str) and len(t) > 5]
    if not valid_times:
        continue
        
    best_time_str = valid_times[0]
    
    try:
        dt = pd.to_datetime(best_time_str).to_pydatetime()
        if dt.tzinfo is None: dt = aest.localize(dt)
        else: dt = dt.astimezone(aest)
    except:
        continue

    arr_node = f.get('arrival') or f.get('movement') or {}
    gate = arr_node.get('gate', 'TBA')
    terminal = str(arr_node.get('terminal', '')).strip().upper()
    
    if terminal == 'D' or terminal == 'DOM':
        continue
        
    status = f.get('status', '').lower()
    time_diff_minutes = int((dt - now_aest).total_seconds() / 60)
    
    is_landed = status in ['landed', 'arrived'] or time_diff_minutes <= 0
    
    css_class = "status-normal"
    tags = []
    
    if dt.hour < 4 or (dt.hour == 4 and dt.minute <= 10):
        tags.append("🚨 早班高消費客群預警")

    if is_landed:
        css_class = "status-landed"
        landed_mins = max(0, -time_diff_minutes)
        time_display = f"Landed {landed_mins} 分鐘 ago ({dt.strftime('%H:%M')})"
    else:
        minutes_left = max(0, time_diff_minutes)
        if dt.hour < 4 or (dt.hour == 4 and dt.minute <= 10):
            css_class = "status-purple"
        elif minutes_left < 25:
            css_class = "status-red"
        elif minutes_left <= 60:
            css_class = "status-orange"
        time_display = f"倒數 {minutes_left} 分鐘 ({dt.strftime('%H:%M')})"
            
    processed_flights.append({
        'num': flight_num,
        'origin': origin,
        'gate': gate,
        'display': time_display,
        'is_landed': is_landed,
        'css': css_class,
        'tags': tags,
        'dt': dt
    })

processed_flights.sort(key=lambda x: (1 if x['is_landed'] else 0, -x['dt'].timestamp() if x['is_landed'] else x['dt'].timestamp()))

for pf in processed_flights:
    tag_html = "".join([f'<div class="label-tag">{tag}</div>' for tag in pf['tags']])
    
    card_html = f"""
<div class="flight-card {pf['css']}">
{tag_html}
<div style="display: flex; justify-content: space-between; align-items: flex-end;">
<div>
<div style="font-size: 1.3em; opacity: 0.95;">{pf['num']} • {pf['origin']}</div>
<div class="time-text">{pf['display']}</div>
</div>
<div style="text-align: right;">
<div style="font-size: 1em; opacity: 0.8; margin-bottom: -5px;">Gate</div>
<div class="gate-text">{pf['gate']}</div>
</div>
</div>
</div>
"""
    st.markdown(card_html, unsafe_allow_html=True)
