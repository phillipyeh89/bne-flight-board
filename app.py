import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

# 設定頁面與手機直式螢幕最佳化
st.set_page_config(page_title="BNE Flight Board", page_icon="✈️", layout="centered")

# 隱藏 Streamlit 預設的頂部空白與選單，加入圖片放大的 CSS
st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    .block-container {padding-top: 2rem;}
    
    /* 點擊放大飛機照片的特效按鈕 */
    .avatar-btn {
        cursor: pointer; 
        margin-right: 18px; 
        flex-shrink: 0; 
        display: block; 
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        border-radius: 35px;
    }
    .avatar-btn:hover {
        transform: scale(1.08); 
        box-shadow: 0 0 15px rgba(255,255,255,0.3);
    }

    /* 純 CSS 圖片放大燈箱 (Lightbox) */
    .img-zoom-chk:checked + .img-zoom-modal { display: flex; }
    .img-zoom-modal {
        display: none; 
        position: fixed; 
        top: 0; left: 0; right: 0; bottom: 0;
        background: rgba(15, 23, 42, 0.92); 
        z-index: 999999;
        align-items: center; 
        justify-content: center; 
        backdrop-filter: blur(5px);
    }
    .img-zoom-modal img {
        max-width: 90vw; 
        max-height: 80vh; 
        border-radius: 12px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.6); 
        border: 2px solid #475569;
        object-fit: contain;
    }
    .img-zoom-close { 
        position: absolute; 
        top: 0; left: 0; right: 0; bottom: 0; 
        cursor: pointer; 
    }
    .close-btn-text {
        position: absolute; 
        top: 20px; right: 30px; 
        color: #F8FAFC;
        font-size: 3em; 
        font-weight: bold;
        cursor: pointer; 
        z-index: 1000000; 
        line-height: 1;
        text-shadow: 0 2px 4px rgba(0,0,0,0.5);
    }
    .close-btn-text:hover { color: #EF4444; }
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
        r = requests.get(url, headers=headers, timeout=3)
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
    update_display = st.session_state.last_update_time.strftime('%H:%M:%S') if st.session_state.last_update_time else "Just Now"
    st.markdown(f'<div style="font-size: 0.85em; color: #94A3B8; text-align: center; margin-bottom: 8px;">🕒 Last Updated: {update_display}</div>', unsafe_allow_html=True)
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
    mv = f.get('movement') or {}
    airport_info = dep.get('airport') or mv.get('airport') or {}
    
    city = airport_info.get('municipalityName') or airport_info.get('name') or airport_info.get('iata') or "Unknown"
    country_code = str(airport_info.get('countryCode', '')).strip().lower()
    
    aircraft_node = f.get('aircraft') or {}
    aircraft_model = aircraft_node.get('model', '')
    aircraft_reg = aircraft_node.get('reg', '')
    
    image_url = fetch_aircraft_image(aircraft_reg)
    ac_text = f"{aircraft_model} ({aircraft_reg})" if aircraft_model and aircraft_reg else aircraft_model or aircraft_reg

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
    
    landed_mins = max(0, -time_diff_minutes) if is_landed else 0
    minutes_left = max(0, time_diff_minutes) if not is_landed else 0
    is_early_prep = not is_landed and ((dt.hour == 2 and dt.minute >= 30) or (dt.hour == 3) or (dt.hour == 4 and dt.minute <= 10))

    if is_landed:
        if landed_mins <= 60:
            border_color = "#10B981" 
            status_color = "#34D399"
            bg_color = "#1E293B"
        else:
            border_color = "#475569" 
            status_color = "#94A3B8"
            bg_color = "#0F172A"
        status_text = f"Landed {format_hm(landed_mins)} ago"
    else:
        bg_color = "#1E293B"
        if is_early_prep:
            border_color = "#8B5CF6" 
            status_color = "#A78BFA"
            status_text = f"⏰ In {format_hm(minutes_left)} (Prep)"
        elif minutes_left < 25:
            border_color = "#EF4444" 
            status_color = "#F87171"
            status_text = f"🔥 In {format_hm(minutes_left)}"
        elif minutes_left <= 60:
            border_color = "#F59E0B" 
            status_color = "#FBBF24"
            status_text = f"In {format_hm(minutes_left)}"
        else:
            border_color = "#3B82F6" 
            status_color = "#60A5FA"
            status_text = f"In {format_hm(minutes_left)}"
            
    processed_flights.append({
        'is_gap': False,
        'num': flight_num,
        'origin': city,
        'sch_display': sch_display,
        'ac_text': ac_text,
        'gate': gate,
        'actual_time': dt.strftime('%H:%M'),
        'is_landed': is_landed,
        'landed_mins': landed_mins,
        'dt': dt,
        'image_url': image_url,
        'border_color': border_color,
        'status_color': status_color,
        'status_text': status_text,
        'bg_color': bg_color
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
    time_text = f"{display_start.strftime('%H:%M')} - {t_end.strftime('%H:%M')}"
    
    gap_bg = "#064E3B" if is_active else "#0F172A"
    gap_border = "#10B981" if is_active else "#475569"
    gap_color = "#A7F3D0" if is_active else "#94A3B8"
    
    # 無縮排的 HTML
    gap_html = f"""<div style="background-color: {gap_bg}; border: 1px dashed {gap_border}; border-radius: 8px; padding: 12px; margin-bottom: 12px; text-align: center; color: {gap_color}; font-family: sans-serif; font-weight: bold; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
{title_text} <span style="opacity: 0.7; font-weight: normal; margin-left: 8px;">({time_text})</span>
</div>"""
    
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

for i, pf in enumerate(processed_flights):
    if pf['is_gap']:
        st.markdown(pf['html'], unsafe_allow_html=True)
        continue
        
    # 動態產生無縮排的燈箱 HTML
    if pf['image_url']:
        modal_id = f"modal_{i}"
        image_element = f"""<label for="{modal_id}" class="avatar-btn">
<img src="{pf['image_url']}" style="width: 70px; height: 70px; border-radius: 35px; object-fit: cover; border: 2px solid {pf['border_color']}; display: block;" />
</label>
<input type="checkbox" id="{modal_id}" class="img-zoom-chk" style="display:none;">
<div class="img-zoom-modal">
<label for="{modal_id}" class="img-zoom-close"></label>
<label for="{modal_id}" class="close-btn-text">&times;</label>
<img src="{pf['image_url']}" />
</div>"""
    else:
        image_element = f'<div style="width: 70px; height: 70px; border-radius: 35px; background: #334155; display: flex; align-items: center; justify-content: center; margin-right: 18px; font-size: 1.6em; border: 2px solid {pf["border_color"]}; flex-shrink: 0;">✈️</div>'
        
    sch_str = f"Sch {pf['sch_display']} • " if pf['sch_display'] else ""
    
    # 最堅固的 HTML 排版 (絕對無縮排)
    card_html = f"""<div style="background-color: {pf['bg_color']}; border-left: 6px solid {pf['border_color']}; border-radius: 8px; padding: 16px 20px; margin-bottom: 12px; display: flex; align-items: center; color: white; font-family: sans-serif; box-shadow: 0 4px 6px rgba(0,0,0,0.15);">
{image_element}
<div style="flex-grow: 1;">
<div style="font-size: 1.4em; font-weight: bold; margin-bottom: 4px;">{pf['num']} <span style="font-size: 0.75em; color: #94A3B8; font-weight: normal; margin-left: 6px;">{pf['origin']}</span></div>
<div style="font-size: 0.85em; color: #CBD5E1; margin-bottom: 6px;">{pf['ac_text']}</div>
<div style="font-size: 0.85em; color: #CBD5E1;">{sch_str}<span style="color: #7DD3FC; font-weight: bold; background: rgba(14,165,233,0.15); padding: 2px 6px; border-radius: 4px; border: 1px solid rgba(14,165,233,0.3);">Act {pf['actual_time']}</span></div>
</div>
<div style="text-align: right; min-width: 110px;">
<div style="font-size: 0.8em; color: #94A3B8; text-transform: uppercase; font-weight: bold; letter-spacing: 0.05em;">Gate</div>
<div style="font-size: 2.6em; font-weight: bold; line-height: 1; margin-top: 4px;">{pf['gate']}</div>
<div style="font-size: 1.05em; font-weight: bold; color: {pf['status_color']}; margin-top: 6px;">{pf['status_text']}</div>
</div>
</div>"""

    st.markdown(card_html, unsafe_allow_html=True)
