import streamlit as st
import requests
import pandas as pd
from datetime import datetime
import pytz

# 設定頁面與手機直式螢幕最佳化
st.set_page_config(page_title="BNE 免稅店航班看板", page_icon="✈️", layout="centered")

# 自動更新機制：利用 HTML meta 標籤每 10 分鐘 (600秒) 自動重整頁面，節省 API 額度
st.markdown('<meta http-equiv="refresh" content="600">', unsafe_allow_html=True)

# 注入自訂 CSS 以最佳化行動裝置閱讀體驗
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
        font-size: 2.8em;
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
    
    /* 紫色警報動畫 */
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

# 抓取航班資訊，快取設定為 600 秒 (10分鐘)
@st.cache_data(ttl=600)
def fetch_flight_data(date_str):
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/YBBN/{date_str}T02:30/{date_str}T12:00"
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
        st.error(f"API 請求失敗，請確認連線或 API Key 額度：{e}")
        return []

# 建立布里斯本時區
aest = pytz.timezone('Australia/Brisbane')
now_aest = datetime.now(aest)

# 設定監控時段 (02:30 AM - 12:00 PM)
start_time = now_aest.replace(hour=2, minute=30, second=0, microsecond=0)
end_time = now_aest.replace(hour=12, minute=0, second=0, microsecond=0)

if not (start_time <= now_aest <= end_time):
    st.title("✈️ BNE 國際線免稅看板")
    st.success("非監控時段，請休息充電 🔋")
    st.stop()

# 標題與手動更新按鈕
col1, col2 = st.columns([2, 1])
with col1:
    st.title("✈️ 國際入境看板")
with col2:
    if st.button("🔄 手動更新", use_container_width=True):
        fetch_flight_data.clear()
        st.rerun()

date_str = now_aest.strftime("%Y-%m-%d")
flights = fetch_flight_data(date_str)

if not flights:
    st.info("目前無航班資料，或 API 尚未回傳結果。")
    st.stop()

processed_flights = []
alert_threshold_time = now_aest.replace(hour=4, minute=10, second=0, microsecond=0)

# 處理並計算航班屬性
for f in flights:
    flight_num = f.get('number', 'N/A')
    origin = f.get('departure', {}).get('airport', {}).get('name', 'Unknown')
    arr = f.get('arrival', {})
    
    # 優先使用實際時間，若無則使用預定時間
    best_time_str = arr.get('actualTimeLocal') or arr.get('scheduledTimeLocal')
    
    if not best_time_str:
        continue
        
    try:
        # 解析帶有時區的時間字串
        dt = pd.to_datetime(best_time_str).to_pydatetime()
        if dt.tzinfo is None:
            dt = aest.localize(dt)
        else:
            dt = dt.astimezone(aest)
    except:
        continue

    gate = arr.get('gate', 'TBA')
    status = f.get('status', '').lower()
    
    is_landed = status in ['landed', 'arrived']
    time_diff_minutes = int((dt - now_aest).total_seconds() / 60)
    
    css_class = "status-normal"
    tags = []
    landed_mins = 0
    minutes_left = 0
    
    # 無論是否已經降落，只要排定在 04:10 前，就掛上早班標籤
    if dt < alert_threshold_time:
        tags.append("🚨 早班高消費客群預警")

    if is_landed:
        css_class = "status-landed"
        # 計算已經降落多久 (取絕對值避免 API 延遲導致的負數異常)
        landed_mins = max(0, -time_diff_minutes)
    else:
        minutes_left = max(0, time_diff_minutes)
        # 特殊警報優先級最高
        if dt < alert_threshold_time:
            css_class = "status-purple"
        elif minutes_left < 25:
            css_class = "status-red"
        elif minutes_left <= 60:
            css_class = "status-orange"
            
    processed_flights.append({
        'num': flight_num,
        'origin': origin,
        'gate': gate,
        'time': dt.strftime('%H:%M'),
        'minutes_left': minutes_left,
        'landed_mins': landed_mins,
        'is_landed': is_landed,
        'css': css_class,
        'tags': tags,
        'dt': dt
    })

# 排序：未降落的排前面（依照時間早到晚），已降落的自動移動到最下方 (依照降落時間新到舊)
processed_flights.sort(key=lambda x: (1 if x['is_landed'] else 0, -x['dt'].timestamp() if x['is_landed'] else x['dt'].timestamp()))

# 渲染卡片
for pf in processed_flights:
    tag_html = "".join([f'<div class="label-tag">{tag}</div>' for tag in pf['tags']])
    
    if pf['is_landed']:
        # 新增已降落的分鐘數顯示
        time_display = f"已降落 {pf['landed_mins']} 分鐘 ({pf['time']})"
    else:
        time_display = f"倒數 {pf['minutes_left']} 分鐘 ({pf['time']})"
        
    card_html = f"""
    <div class="flight-card {pf['css']}">
        {tag_html}
        <div style="display: flex; justify-content: space-between; align-items: flex-end;">
            <div>
                <div style="font-size: 1.3em; opacity: 0.95;">{pf['num']} • {pf['origin']}</div>
                <div class="time-text">{time_display}</div>
            </div>
            <div style="text-align: right; padding-left: 10px;">
                <div style="font-size: 1em; opacity: 0.8; margin-bottom: -5px;">Gate</div>
                <div class="gate-text">{pf['gate']}</div>
            </div>
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)
