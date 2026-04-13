import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pytz

# ─────────────────────────────────────────────
# Constants (BNE Operations - 12H Pro Stable)
# ─────────────────────────────────────────────
AIRPORT_ICAO       = "YBBN"
TIMEZONE           = "Australia/Brisbane"
LOOKBACK_HOURS     = 1
LOOKAHEAD_HOURS    = 11 
RECENT_LANDED_MAX  = 60
GAP_MIN_MINUTES    = 20
GAP_DISPLAY_MIN    = 5
IMAGE_WORKERS      = 10
DOMESTIC_TERMINALS = ('D', 'DOM', 'D-ANC', 'GAT', 'TBA')
SMALL_AIRCRAFT_FILTER = ('BEECH', 'FAIRCHILD', 'CESSNA', 'PIPER', 'PILATUS', 'KING AIR', 'METROLINER')

CITY_MAP = {
    "Lapu-Lapu City": "Cebu",
    "Denpasar-Bali Island": "Bali",
    "Ho Chi Minh City": "Saigon",
    "Yaren District": "Nauru",
    "Guangzhou Baiyun": "Guangzhou"
}

UI_REFRESH_SEC     = 60   
API_DATA_TTL_SEC   = 600  
STALE_DATA_THRESHOLD_MIN = 30 

# ─────────────────────────────────────────────
# Page Config & Typography
# ─────────────────────────────────────────────
st.set_page_config(page_title="BNE Pro Arrivals", page_icon="✈️", layout="centered")
st.markdown(f"""
<meta http-equiv="refresh" content="{UI_REFRESH_SEC}">
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&family=JetBrains+Mono:wght@600&display=swap');
    #MainMenu {{visibility: hidden;}}
    header {{visibility: hidden;}}
    .block-container {{padding-top: 1.5rem; font-family: 'Inter', sans-serif;}}
    div, span, label, p, li {{ font-family: 'Inter', sans-serif; }}
    .mono {{ font-family: 'JetBrains Mono', monospace; letter-spacing: -0.5px; }}
    @keyframes blink {{ 50% {{ opacity: 0; }} }}
    .stale-warning {{ color: #EF4444 !important; font-weight: 700 !important; animation: blink 1.2s linear infinite; }}
    
    .avatar-btn {{
        cursor: pointer; margin-right: 18px; flex-shrink: 0;
        display: block; transition: transform 0.2s ease, box-shadow 0.2s ease;
        border-radius: 35px; box-sizing: border-box;
    }}
    .avatar-btn:hover {{ transform: scale(1.08); box-shadow: 0 0 15px rgba(255,255,255,0.3); }}
    
    .img-zoom-chk:checked + .img-zoom-modal {{ display: flex; }}
    .img-zoom-modal {{
        display: none; position: fixed; top:0; left:0; right:0; bottom:0;
        background: rgba(15,23,42,0.92); z-index: 999999;
        align-items: center; justify-content: center; backdrop-filter: blur(5px);
    }}
    .img-zoom-modal img {{
        max-width: 90vw; max-height: 80vh; border-radius: 12px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.6); border: 2px solid #475569;
        object-fit: contain;
    }}
    .img-zoom-close {{ position: absolute; top:0; left:0; right:0; bottom:0; cursor: pointer; }}
    .close-btn-text {{
        position: absolute; top: 20px; right: 30px;
        color: #F8FAFC; font-size: 3em; font-weight: bold;
        cursor: pointer; z-index: 1000000; line-height: 1;
        text-shadow: 0 2px 4px rgba(0,0,0,0.5);
    }}
    .close-btn-text:hover {{ color: #EF4444; }}
    
    .streamlit-expanderHeader {{ font-size: 0.9em !important; color: #94A3B8 !important; }}
    .streamlit-expanderContent {{ font-size: 0.85em; color: #CBD5E1; }}
</style>
""", unsafe_allow_html=True)

if "api_last_hit" not in st.session_state:
    st.session_state.api_last_hit = None

# ─────────────────────────────────────────────
# Data Fetchers
# ─────────────────────────────────────────────
@st.cache_data(ttl=86400, show_spinner=False)
def fetch_aircraft_image(reg: str) -> str:
    if not reg: return ""
    try:
        url = f"https://api.planespotters.net/pub/photos/reg/{reg}"
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=3)
        photos = r.json().get("photos", [])
        if photos: return photos[0]["thumbnail_large"]["src"]
    except Exception: pass
    return ""

def prefetch_images(flights: list):
    regs = [f.get("aircraft", {}).get("reg", "") for f in flights]
    with ThreadPoolExecutor(max_workers=IMAGE_WORKERS) as ex:
        list(ex.map(fetch_aircraft_image, regs))

@st.cache_data(ttl=API_DATA_TTL_SEC)
def fetch_flight_data(from_time: str, to_time: str) -> list:
    url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{AIRPORT_ICAO}/{from_time}/{to_time}"
    params = {"direction": "Arrival", "withCancelled": "true", "withCodeshared": "false"}
    headers = {"X-RapidAPI-Key": st.secrets["X_RAPIDAPI_KEY"], "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        st.session_state.api_last_hit = datetime.now(pytz.timezone(TIMEZONE))
        return r.json().get("arrivals", [])
    except Exception as e:
        st.error(f"API Request Failed: {e}"); return []

# ─────────────────────────────────────────────
# Logic Helpers
# ─────────────────────────────────────────────
def format_hm(total_minutes: int) -> str:
    h, m = divmod(total_minutes, 60)
    return f"{m:02d}m" if h == 0 else f"{h:02d}h {m:02d}m"

def _parse_local_dt(raw: str | None, tz) -> datetime | None:
    if not raw: return None
    try:
        dt = pd.to_datetime(raw).to_pydatetime()
        return tz.localize(dt) if dt.tzinfo is None else dt.astimezone(tz)
    except Exception: return None

def extract_best_time(node: dict, tz) -> tuple:
    for key, label in (("actualTime", "actual"), ("revisedTime", "revised"), ("scheduledTime", "scheduled")):
        raw = node.get(key).get("local") if isinstance(node.get(key), dict) else node.get(key + "Local")
        if raw:
            dt = _parse_local_dt(raw, tz)
            if dt: return dt, label
    return None, ""

def is_strictly_international(terminal: str, country_code: str, aircraft_model: str) -> bool:
    t, ac = terminal.strip().upper(), aircraft_model.upper()
    if t in DOMESTIC_TERMINALS: return False
    if country_code == "au": return False
    if any(k in ac for k in SMALL_AIRCRAFT_FILTER): return False
    return True

def get_card_style(is_canceled, is_archived, is_landed, landed_mins, delay_hours, mins_left):
    if is_canceled:
        if is_archived: return "#475569", "#94A3B8", "#0F172A", "❌ CANCELED"
        return "#EF4444", "#F87171", "#1E293B", "❌ CANCELED"
    if is_landed:
        if landed_mins <= RECENT_LANDED_MAX: return "#10B981", "#34D399", "#1E293B", f"Landed {format_hm(landed_mins)} ago"
        return "#475569", "#94A3B8", "#0F172A", f"Landed {format_hm(landed_mins)} ago"

    bg = "#1E293B"
    if mins_left < 25:
        delay_suffix = f" (+{int(delay_hours)}h late)" if delay_hours >= 1 else ""
        return "#EF4444", "#F87171", bg, f"🔥 In {format_hm(mins_left)}{delay_suffix}"
    if delay_hours >= 3:
        return "#EF4444", "#F87171", bg, f"⚠️ HEAVY DELAY  In {format_hm(mins_left)}"
    if mins_left <= 60:
        return "#F59E0B", "#FBBF24", bg, f"In {format_hm(mins_left)}"
    return "#3B82F6", "#60A5FA", bg, f"In {format_hm(mins_left)}"

# ─────────────────────────────────────────────
# Renderers
# ─────────────────────────────────────────────
def render_flight_card(pf: dict, index: int):
    img_url, border_col = pf["image_url"], pf["border_color"]
    if img_url:
        mid = f"modal_{index}"
        image_element = f"""<label for="{mid}" class="avatar-btn">
<img src="{img_url}" style="width:70px;height:70px;border-radius:35px;object-fit:cover;border:2px solid {border_col};display:block;" />
</label>
<input type="checkbox" id="{mid}" class="img-zoom-chk" style="display:none;">
<div class="img-zoom-modal">
<label for="{mid}" class="img-zoom-close"></label>
<label for="{mid}" class="close-btn-text">&times;</label>
<img src="{pf['image_url']}" />
</div>"""
    else:
        image_element = f'<div style="width:70px;height:70px;border-radius:35px;background:#334155;display:flex;align-items:center;justify-content:center;margin-right:18px;font-size:1.6em;border:2px solid {border_col};flex-shrink:0;box-sizing:border-box;">✈️</div>'

    sch_str = f'<span class="mono">Sch {pf["sch_display"]}</span> • ' if pf["sch_display"] else ""
    next_day_tag = ' <small style="opacity:0.6;">(Next Day)</small>' if pf["is_next_day"] else ''

    # 💡 V7.12 修正：從檢查數字改為檢查「時間來源標籤」
    # 如果 time_type 只能拿到 scheduled，代表雷達沒有更新預估時間
    check_board_tag = ""
    if not pf["is_landed"] and not pf["is_canceled"] and pf["time_type"] == "scheduled":
        check_board_tag = ' <span style="color:#FBBF24; font-size:0.85em; margin-left:6px; font-weight:700;">⚠️ Check Board</span>'

    if pf["is_landed"] or pf["time_type"] == "actual":
        act_html = f'<span class="mono" style="color:#7DD3FC;font-weight:bold;background:rgba(14,165,233,0.15);padding:2px 6px;border-radius:4px;border:1px solid rgba(14,165,233,0.3);">Act {pf["actual_time"]}</span>{next_day_tag}'
    elif pf["time_type"] == "revised":
        act_html = f'<span class="mono" style="color:#E2E8F0;font-weight:bold;background:rgba(226,232,240,0.15);padding:2px 6px;border-radius:4px;border:1px solid rgba(226,232,240,0.3);">Est {pf["actual_time"]}</span>{next_day_tag}{check_board_tag}'
    else: # pf["time_type"] == "scheduled"
        act_html = f'<span class="mono" style="color:#94A3B8;font-weight:bold;background:rgba(148,163,184,0.15);padding:2px 6px;border-radius:4px;border:1px solid rgba(148,163,184,0.3);">Sch {pf["actual_time"]}</span>{next_day_tag}{check_board_tag}'
        # 若只有 scheduled，連前面原本顯示的 "Sch xx:xx • " 都不需要重複顯示，讓版面更乾淨
        sch_str = "" 

    origin_display = f"{pf['origin']} <span class='mono' style='font-size:0.85em; opacity:0.8;'>({pf['iata']})</span>" if pf['iata'] else pf['origin']

    card_html = f"""<div style="background-color:{pf['bg_color']};border-left:6px solid {border_col};border-radius:8px;padding:16px 20px;margin-bottom:12px;display:flex;align-items:center;color:white;box-shadow:0 4px 6px rgba(0,0,0,0.15);">
{image_element}
<div style="flex-grow:1;">
<div style="font-size:1.4em;font-weight:700;margin-bottom:4px;">{pf['num']}<span style="font-size:0.75em;color:#94A3B8;font-weight:400;margin-left:8px;">{origin_display}</span></div>
<div style="font-size:0.85em;color:#CBD5E1;margin-bottom:6px;">{pf['ac_text']}</div>
<div style="font-size:0.85em;color:#CBD5E1;">{sch_str}{act_html}</div>
</div>
<div style="text-align:right;min-width:110px;">
<div style="font-size:0.8em;color:#94A3B8;text-transform:uppercase;font-weight:700;letter-spacing:0.05em;">Gate</div>
<div class="mono" style="font-size:2.6em;font-weight:700;line-height:1;margin-top:4px;">{pf['gate']}</div>
<div style="font-size:1.05em;font-weight:700;color:{pf['status_color']};margin-top:6px;">{pf['status_text']}</div>
</div>
</div>"""
    st.markdown(card_html, unsafe_allow_html=True)

# ─────────────────────────────────────────────
# Main Process
# ─────────────────────────────────────────────
aest = pytz.timezone(TIMEZONE); now_aest = datetime.now(aest)
from_t = (now_aest - timedelta(hours=LOOKBACK_HOURS)).strftime("%Y-%m-%dT%H:%M")
to_t = (now_aest + timedelta(hours=LOOKAHEAD_HOURS)).strftime("%Y-%m-%dT%H:%M")

col1, col2 = st.columns([2, 1])
with col1: st.title("✈️ Arrivals")
with col2:
    st.markdown(f'<div style="font-size:0.85em;color:#94A3B8;text-align:center;margin-top:10px;">🕒 Live: {now_aest.strftime("%H:%M:%S")}</div>', unsafe_allow_html=True)
    api_t = st.session_state.get('api_last_hit')
    if api_t and (now_aest - api_t).total_seconds() / 60 > STALE_DATA_THRESHOLD_MIN:
        api_html = f'<span class="stale-warning">API: {api_t.strftime("%H:%M")} (STALE)</span>'
    else:
        api_html = f'API: {api_t.strftime("%H:%M") if api_t else "--:--"}'
    st.markdown(f'<div style="font-size:0.75em;color:#64748B;text-align:center;">{api_html}</div>', unsafe_allow_html=True)

# ── 系統說明區塊 (Expander) ──
with st.expander("ℹ️ 系統運作說明與常見問題 (System Info)"):
    st.markdown(f"""
    **1. 數據更新機制**
    * 畫面每 **60 秒**自動跳動倒數，而背後的即時 API 航班資料每 **10 分鐘**同步一次。
    * 當前顯示範圍：過去 **{LOOKBACK_HOURS}** 小時至未來 **{LOOKAHEAD_HOURS}** 小時。

    **2. 為什麼有些飛機沒有顯示照片？**
    * 系統只會透過「實體機身編號 (Registration Number)」來抓取精確的飛機照片。
    * 若飛機仍在遙遠航程中，航空公司有時尚未指派或回傳確切的機身編號，系統為了防呆會顯示預設 ✈️ 圖示。通常在降落前 1-2 小時會自動補齊。

    **3. 時間標籤狀態說明**
    * <span class="mono" style="color:#7DD3FC;font-weight:bold;background:rgba(14,165,233,0.15);padding:2px 4px;border-radius:4px;">Act</span> **(天藍色)**：飛機已實際降落。
    * <span class="mono" style="color:#E2E8F0;font-weight:bold;background:rgba(226,232,240,0.15);padding:2px 4px;border-radius:4px;">Est</span> **(冷灰色)**：雷達已掌握即時動態，並計算出精準預估時間。就算航班表現完美、沒有延遲，只要有雷達追蹤就會顯示此標籤。
    * <span class="mono" style="color:#94A3B8;font-weight:bold;background:rgba(148,163,184,0.15);padding:2px 4px;border-radius:4px;">Sch</span> **(暗灰色)** + **⚠️ Check Board**：雷達尚未收到該航班動態，僅能提供原本的表定時間。此時請務必抬頭核對機場實體螢幕，以免錯估客流時間。
    
    **4. 隱藏航班與過濾器**
    * 系統已啟動「免稅店專屬國際線過濾器」，自動排除了國內航廈 (Domestic)、小型私人飛機與非載客航班。
    """, unsafe_allow_html=True)
st.write("") 

flights = fetch_flight_data(from_t, to_t)
if not flights:
    st.info("No data available. Re-checking..."); st.stop()

unique_flights = {f.get("number"): f for f in flights}.values()
prefetch_images(list(unique_flights))
processed_flights = []

for f in unique_flights:
    flight_num, status = f.get("number", "N/A"), f.get("status", "").lower()
    dep, mv = f.get("departure", {}), f.get("movement", {})
    ai = dep.get("airport") or mv.get("airport") or {}
    raw_city = ai.get("municipalityName") or ai.get("name") or ai.get("iata") or "Unknown"
    city = CITY_MAP.get(raw_city, raw_city)
    iata = ai.get("iata", "")
    country = str(ai.get("countryCode", "")).strip().lower()
    arr_n = f.get("arrival") or f.get("movement") or {}
    term, gate = str(arr_n.get("terminal", "")).strip().upper(), arr_n.get("gate", "TBA")
    ac = f.get("aircraft", {}); ac_m, ac_r = ac.get("model", ""), ac.get("reg", "")

    if not is_strictly_international(term, country, ac_m): continue

    best_dt, t_type = extract_best_time(arr_n, aest)
    if best_dt is None: continue

    s_dt = _parse_local_dt((arr_n.get("scheduledTime", {}) or {}).get("local"), aest) or best_dt
    sch_disp = s_dt.strftime("%H:%M") if (arr_n.get("scheduledTime", {}) or {}).get("local") else ""

    delay_hours = (best_dt - s_dt).total_seconds() / 3600 if s_dt else 0
    if delay_hours < -2 or delay_hours > 12: continue

    t_diff = int((best_dt - now_aest).total_seconds() / 60)
    is_can = status in ("canceled", "cancelled")
    is_lan = (status in ("landed", "arrived") or t_diff <= 0) and not is_can
    l_min, m_left = max(0, -t_diff) if is_lan else 0, max(0, t_diff) if not is_lan else 0

    is_arch_can = is_can and bool(s_dt) and (now_aest - s_dt).total_seconds() / 60 > 15
    is_next_day = best_dt.date() > now_aest.date()

    bc, sc, bg, st_txt = get_card_style(is_can, is_arch_can, is_lan, l_min, delay_hours, m_left)

    processed_flights.append({
        "num": flight_num, "origin": city, "iata": iata, "sch_display": sch_disp,
        "ac_text": f"{ac_m} ({ac_r})" if ac_m and ac_r else ac_m or ac_r,
        "gate": gate, "actual_time": best_dt.strftime("%H:%M"), "is_landed": is_lan,
        "is_canceled": is_can, "is_archived_canceled": is_arch_can, "landed_mins": l_min,
        "dt": best_dt, "s_dt_val": s_dt, "time_type": t_type, "image_url": fetch_aircraft_image(ac_r),
        "border_color": bc, "status_color": sc, "status_text": st_txt, "bg_color": bg, "is_next_day": is_next_day
    })

# ── Gap Detection ──
future_f = sorted([p for p in processed_flights if not p["is_landed"] and not p["is_canceled"]], key=lambda x: x["dt"])
if future_f:
    windows = [(now_aest, future_f[0]["dt"])]
    for i in range(len(future_f)-1): windows.append((future_f[i]["dt"], future_f[i+1]["dt"]))
    for t1, t2 in windows:
        if t2 <= now_aest: continue
        ds = max(t1, now_aest); g_min = int((t2 - ds).total_seconds() / 60)
        if (t2 - t1).total_seconds() / 60 < GAP_MIN_MINUTES or g_min < GAP_DISPLAY_MIN: continue
        act = t1 <= now_aest
        tit = f"🟢 ACTIVE OFF-FLOOR ({format_hm(g_min)} left)" if act else f"🔄 {format_hm(g_min)} OFF-FLOOR WINDOW"
        tm = f"{ds.strftime('%H:%M')} – {t2.strftime('%H:%M')}"
        gb, gbo, gc = ("#064E3B", "#10B981", "#A7F3D0") if act else ("#0F172A", "#475569", "#94A3B8")
        gap_h = f'<div style="background-color:{gb};border:1px dashed {gbo};border-radius:8px;padding:10px;margin-bottom:12px;text-align:center;color:{gc};font-family:sans-serif;font-weight:bold;box-shadow:0 2px 4px rgba(0,0,0,0.1);font-size:0.95em;">{tit} <span style="opacity:0.7;font-weight:normal;margin-left:6px;display:inline-block;">({tm})</span></div>'
        processed_flights.append({"is_gap": True, "html": gap_h, "time_key": t1.timestamp() + 1})

# ── Sort & Render ──
def s_key(p):
    if p.get("is_gap"): return (1, p["time_key"])
    if p["is_canceled"]: return (2, -p["s_dt_val"].timestamp()) if p.get("is_archived_canceled") else (1, p["dt"].timestamp())
    if p["is_landed"]: return (0, -p["dt"].timestamp()) if p["landed_mins"] <= RECENT_LANDED_MAX else (2, -p["dt"].timestamp())
    return (1, p["dt"].timestamp())

processed_flights.sort(key=s_key)
for i, pf in enumerate(processed_flights):
    if pf.get("is_gap"): st.markdown(pf["html"], unsafe_allow_html=True)
    else: render_flight_card(pf, i)
