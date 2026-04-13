import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, time
import pytz

# 設定頁面與手機直式螢幕最佳化
st.set_page_config(page_title="BNE Flight Board", page_icon="✈️", layout="centered")

# 自動更新機制：每 20 分鐘 (1200秒) 自動重整
st.markdown('<meta http-equiv="refresh" content="1200">', unsafe_allow_html=True)

# 注入高質感漸層 CSS (包含飛機照片與離櫃空檔橫幅設計)
st.markdown("""
<style>
    .flight-card {
        padding: 16px 20px;
        margin-bottom: 15px;
        border-radius: 12px;
        color: #F8FAFC;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        border: 1px solid rgba(255, 255, 255, 0.05);
    }
    .gate-text {
        font-size: 2.5em;
        font-weight: 800;
        line-height: 1;
        letter-spacing: -0.02em;
    }
    .time-text {
        font-size: 1.5em;
        font-weight: 700;
        margin-top: 4px;
        letter-spacing: -0.01em;
    }
    .label-tag {
        background-color: rgba(255, 255, 255, 0.2);
        padding: 4px 10px;
        border-radius: 6px;
        font-size: 0.85em;
        font-weight: 600;
        margin-bottom: 12px;
        display: inline-block;
        letter-spacing: 0.02em;
        text-transform: uppercase;
    }
    
    .status-normal { background: linear-gradient(135deg, #334155, #1E293B); } 
    .status-soon { background: linear-gradient(135deg, #D97706, #B45309); } 
    .status-urgent { background: linear-gradient(135deg, #E11D48, #BE123C); } 
    .status-landed-new { background: linear-gradient(135deg, #059669, #047857); } 
    
    .status-landed-old { 
        background: #0F172A; 
        color: #64748B; 
        border: 1px dashed #334155; 
        box-shadow: none; 
    }
    .status-landed-old .time-text, .status-landed-old .gate-text { opacity: 0.6; }

    @keyframes pulse-purple {
        0% { box-shadow: 0 0 0 0 rgba(139, 92, 246, 0.5); }
        70% { box-shadow: 0 0 0 10px rgba(139, 92, 246, 0); }
        100% { box-shadow: 0 0 0 0 rgba(139, 92, 246, 0); }
    }
    .status-purple {
        background: linear-gradient(135deg, #8B5CF6, #6D28D9);
        animation: pulse-purple 2s infinite;
        border: 1px solid #A78BFA;
    }

    /* 空檔橫幅專屬設計 */
    .gap-card {
        text-align: center;
        padding: 12px;
        margin: 5px 0 15px 0;
        border-radius: 8px;
        background: linear-gradient(90deg, rgba(15, 23, 42, 0) 0%, rgba(34, 197, 94, 0.15) 50%, rgba(15, 23, 42, 0) 100%);
        border-top: 1px dashed rgba(34, 197, 94, 0.5);
        border-bottom: 1px dashed rgba(34, 197, 94, 0.5);
        color: #86EFAC;
        font-size: 1.05em;
        font-weight: 700;
        letter-spacing: 0.03em;
    }
    .gap-card.future {
        background: linear-gradient(90deg, rgba(15, 23, 42, 0) 0%, rgba(51, 65, 85, 0.6) 50%, rgba(15, 23, 42, 0) 100%);
        border-top: 1px dashed rgba(148, 163, 184, 0.4);
        border-bottom: 1px dashed rgba(148, 163, 184, 0.4);
        color: #94A3B8;
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
    if h == 0:
        return f"{m:02d}m"
    return f"{h:02d}h{m:02d}m"

for f in flights:
    flight_num = f.get('number', 'N/A')
    
    dep = f.get('departure') or {}
    movement = f.get('movement') or {}
    airport_info = dep.get('airport') or movement.get('airport') or {}
    
    city = airport_info.get('municipalityName')
    name = airport_info.get('name')
    iata = airport_info.get('iata')
    icao = airport_info.get('icao')
    country_code = str(airport_info.get('countryCode', '')).strip().lower()
    
    if city: origin = city
    elif name: origin = name
    elif iata: origin = iata
    elif icao: origin = icao
    else: origin = "Unknown"
    
    # === 抓取機型、註冊編號與照片 (Aircraft Image) ===
    aircraft_node = f.get('aircraft') or {}
    aircraft_model = aircraft_node.get('model', '')
    aircraft_reg = aircraft_node.get('reg', '')
    image_node = aircraft_node.get('image') or {}
    image_url = image_node.get('url', '')
    
    ac_display_parts = []
    if aircraft_model: ac_display_parts.append(aircraft_model)
    if aircraft_reg: ac_display_parts.append(f"({aircraft_reg})")
    ac_text = " ".join(ac_display_parts)
    
    # 生成照片的 HTML (70x70 圓角縮圖)
    if image_url:
        image_html = f'''
        <div style="width: 70px; height: 70px; border-radius: 8px; overflow: hidden; flex-shrink: 0; box-shadow: 0 2px 4px rgba(0,0,0,0.2);">
            <img src="{image_url}" style="width: 100%; height: 100%; object-fit: cover;" />
        </div>
        '''
    else:
        image_html = f'''
        <div style="width: 70px; height: 70px; border-radius: 8px; background: rgba(255,255,255,0.08); display: flex; align-items: center; justify-content: center; flex-shrink: 0; opacity: 0.5;">
            <span style="font-size: 2em;">✈️</span>
        </div>
        '''

    aircraft_display = f'<div style="font-size: 0.85em; opacity: 0.7; font-weight: 500;">{ac_text}</div>' if ac_text else ''
    
    time_candidates = []
    scheduled_time_raw = None
    
    for node_name in ['arrival', 'movement', 'departure']:
        node = f.get(node_name, {})
        if not isinstance(node, dict): continue
        
        if not scheduled_time_raw:
            s_obj = node.get('scheduledTime')
            if isinstance(s_obj, dict) and s_obj.get('local'):
                scheduled_time_raw = s_obj.get('local')
            elif node.get('scheduledTimeLocal'):
                scheduled_time_raw = node.get('scheduledTimeLocal')
        
        for t_key in ['actualTime', 'revisedTime', 'scheduledTime']:
            t_obj = node.get(t_key)
            if isinstance(t_obj, dict) and t_obj.get('local'):
                time_candidates.append(t_obj.get('local'))
                
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
    except:
        continue

    sch_display = ""
    if scheduled_time_raw:
        try:
            s_dt = pd.to_datetime(scheduled_time_raw).to_pydatetime()
            if s_dt.tzinfo is None: s_dt = aest.localize(s_dt)
            else: s_dt = s_dt.astimezone(aest)
            sch_display = f'<span style="font-size: 0.75em; opacity: 0.7; margin-left: 8px;">(Sch {s_dt.strftime("%H:%M")})</span>'
        except:
            pass

    arr_node = f.get('arrival') or f.get('movement') or {}
    gate = arr_node.get('gate', 'TBA')
    terminal = str(arr_node.get('terminal', '')).strip().upper()
    
    if terminal == 'D' or terminal == 'DOM' or country_code == 'au':
        continue
        
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
        except:
            pass
    
    css_class = "status-normal"
    tags = []
    landed_mins = 0
    
    is_early_prep = (dt.hour == 2 and dt.minute >= 30) or (dt.hour == 3) or (dt.hour == 4 and dt.minute <= 10)
    if is_early_prep: tags.append("⏰ Early Prep Required")

    if is_landed:
        landed_mins = max(0, -time_diff_minutes)
        if landed_mins <= 60: css_class = "status-landed-new"
        else: css_class = "status-landed-old"
        time_display = f"Landed {format_hm(landed_mins)} ago ({dt.strftime('%H:%M')})"
    else:
        minutes_left = max(0, time_diff_minutes)
        if is_early_prep: css_class = "status-purple"
        elif minutes_left < 25: css_class = "status-urgent"
        elif minutes_left <= 60: css_class = "status-soon"
        time_display = f"In {format_hm(minutes_left)} ({dt.strftime('%H:%M')})"
            
    processed_flights.append({
        'is_gap': False,
        'num': flight_num,
        'origin': origin,
        'sch_display': sch_display,
        'image_html': image_html,
        'aircraft_display': aircraft_display,
        'gate': gate,
        'display': time_display,
        'is_landed': is_landed,
        'landed_mins': landed_mins,
        'css': css_class,
        'tags': tags,
        'dt': dt
    })

# === 離櫃空檔偵測系統 (真實相鄰航班計算) ===
future_flights = [pf for pf in processed_flights if not pf['is_landed']]
future_flights.sort(key=lambda x: x['dt'])

gaps = []

if future_flights:
    landed_flights = [pf for pf in processed_flights if pf['is_landed']]
    last_landed_time = max([pf['dt'] for pf in landed_flights]) if landed_flights else now_aest
    
    start_time = max(last_landed_time, now_aest)
    first_future = future_flights[0]['dt']
    
    if (first_future - start_time).total_seconds() / 60 >= 20:
        gaps.append((start_time, first_future))
        
    for i in range(len(future_flights) - 1):
        t1 = future_flights[i]['dt']
        t2 = future_flights[i+1]['dt']
        if (t2 - t1).total_seconds() / 60 >= 20:
            gaps.append((t1, t2))

for t_start, t_end in gaps:
    if t_end <= now_aest: continue 
    
    display_start = max(t_start, now_aest)
    gap_mins = int((t_end - display_start).total_seconds() / 60)
    
    if gap_mins < 5: continue 
    
    is_active = t_start <= now_aest
    gap_display = format_hm(gap_mins)
    
    status_text = f"🟢 ACTIVE OFF-FLOOR TIME ({gap_display} left)" if is_active else f"🔄 {gap_display} OFF-FLOOR WINDOW (Break / Duties)"
    css_ext = "" if is_active else "future"
    
    gap_html = f'''
    <div class="gap-card {css_ext}">
        {status_text} <span style="opacity:0.6; margin-left:8px;">({display_start.strftime('%H:%M')} - {t_end.strftime('%H:%M')})</span>
    </div>
    '''
    
    processed_flights.append({
        'is_gap': True,
        'html': gap_html,
        'time_key': t_start.timestamp() + 1 
    })

# === 排序與渲染 ===
def custom_sort(pf):
    if pf['is_gap']:
        return (1, pf['time_key'])
    if pf['is_landed']:
        if pf['landed_mins'] <= 60:
            return (0, -pf['dt'].timestamp())
        else:
            return (2, -pf['dt'].timestamp())
    else:
        return (1, pf['dt'].timestamp())

processed_flights.sort(key=custom_sort)

for pf in processed_flights:
    if pf['is_gap']:
        st.markdown(pf['html'], unsafe_allow_html=True)
        continue
        
    tag_html = "".join([f'<div class="label-tag">{tag}</div>' for tag in pf['tags']])
    
    # 全新的圖文並排 HTML 佈局
    card_html = f"""
<div class="flight-card {pf['css']}">
    {tag_html}
    <div style="display: flex; justify-content: space-between; align-items: center;">
        <div style="display: flex; gap: 15px; align-items: center;">
            {pf['image_html']}
            <div style="display: flex; flex-direction: column; justify-content: center;">
                <div style="font-size: 1.2em; opacity: 0.95; font-weight: 600;">{pf['num']} • {pf['origin']} {pf['sch_display']}</div>
                {pf['aircraft_display']}
                <div class="time-text">{pf['display']}</div>
            </div>
        </div>
        <div style="text-align: right; padding-left: 10px;">
            <div style="font-size: 0.8em; opacity: 0.7; margin-bottom: -2px;">Gate</div>
            <div class="gate-text">{pf['gate']}</div>
        </div>
    </div>
</div>
"""
    st.markdown(card_html, unsafe_allow_html=True)
