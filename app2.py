"""
QuantAQ Multi-Site Air Quality Dashboard  ·  v4
────────────────────────────────────────────────
Run:   streamlit run app.py
       (or double-click launch.bat on Windows)

Python 3.8+ compatible. No Mapbox token needed.
"""

import sys
import os
import re
import math
import json
import time as _time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Accidental direct-run guard ───────────────────────────────────────────────
if not os.environ.get("STREAMLIT_SCRIPT_RUN_CTX") and \
        not any("streamlit" in a.lower() for a in sys.argv):
    print("\n" + "=" * 60)
    print("  QuantAQ Dashboard — Wrong launch method!")
    print("=" * 60)
    print("  Use:   streamlit run app.py")
    print("  Or:    double-click launch.bat\n")
    sys.exit(0)

import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats
import folium
from folium.plugins import MarkerCluster, HeatMap
from streamlit_folium import st_folium

# ══════════════════════════════════════════════════════════════════════════════
#  PAGE CONFIG  — MUST be first Streamlit call
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="QuantAQ Air Quality",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
#  THEME CSS  — full dark theme with all visibility fixes
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');

/* ── Base ── */
html,body,[class*="css"]{
    font-family:'DM Sans',sans-serif!important;
    background:#0d1117!important;
    color:#e6edf3!important;
}
.stApp{background:#0d1117!important}
.main .block-container{padding-top:1rem!important;}

/* ── App header ── */
.app-header{
    background:linear-gradient(135deg,#0d1117 0%,#0a2310 50%,#0d1117 100%);
    border-bottom:1px solid #30363d;
    padding:24px 36px 20px;
    margin:-1rem -1rem 1.5rem -1rem;
    position:relative;overflow:hidden;
}
.app-header::before{
    content:\'\';position:absolute;top:-40%;right:-5%;
    width:380px;height:380px;
    background:radial-gradient(circle,rgba(0,212,100,.15) 0%,transparent 70%);
    pointer-events:none;
}
.app-header h1{
    font-family:\'Space Mono\',monospace!important;
    font-size:22px;font-weight:700;
    color:#00d464!important;letter-spacing:-.5px;margin:0 0 4px;
}
.app-header p{color:#8b949e!important;font-size:12px;margin:0;}

/* ── Sidebar ── */
section[data-testid="stSidebar"]{
    background:#161b22!important;
    border-right:1px solid #30363d!important;
}
section[data-testid="stSidebar"] *{color:#e6edf3;}
section[data-testid="stSidebar"] label{
    font-family:\'Space Mono\',monospace!important;
    font-size:10px!important;font-weight:700!important;
    letter-spacing:1.5px!important;color:#00d464!important;
    text-transform:uppercase!important;
}
section[data-testid="stSidebar"] .stCaption p,
section[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p{
    color:#8b949e!important;font-size:11px!important;
}
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] textarea{
    background:#0d1117!important;border:1px solid #30363d!important;
    color:#e6edf3!important;border-radius:6px!important;
}
section[data-testid="stSidebar"] input::placeholder,
section[data-testid="stSidebar"] textarea::placeholder{color:#4a5568!important;}
section[data-testid="stSidebar"] [data-testid="stRadio"] label,
section[data-testid="stSidebar"] [data-testid="stCheckbox"] label{
    color:#e6edf3!important;font-size:13px!important;
    letter-spacing:0!important;text-transform:none!important;
    font-family:\'DM Sans\',sans-serif!important;
}
section[data-testid="stSidebar"] [data-baseweb="select"] > div{
    background:#0d1117!important;border-color:#30363d!important;color:#e6edf3!important;
}

/* ── Main area widgets ── */
[data-baseweb="select"] > div{
    background:#161b22!important;border-color:#30363d!important;color:#e6edf3!important;
}
[data-baseweb="select"] li{background:#161b22!important;color:#e6edf3!important;}
[data-baseweb="select"] li:hover{background:#1f2937!important;}
[data-testid="stTextInput"] input,[data-testid="stTextArea"] textarea{
    background:#161b22!important;border-color:#30363d!important;color:#e6edf3!important;
}
[data-testid="stWidgetLabel"] p,
.stSelectbox label,.stTextInput label,.stDateInput label,
.stTimeInput label,.stSlider label,.stCheckbox label,.stRadio label{
    color:#e6edf3!important;
}
.stCaption p,[data-testid="stCaptionContainer"] p{color:#8b949e!important;}
.stMarkdown p,.stMarkdown li{color:#e6edf3!important;}
[data-testid="stAlert"] p,
[data-testid="stAlert"] [data-testid="stMarkdownContainer"] p{color:#e6edf3!important;}

/* ── Data editor ── */
[data-testid="stDataEditor"]{
    background:#161b22!important;border:1px solid #30363d!important;
    border-radius:8px!important;
}
[data-testid="stDataEditor"] th{
    background:#1f2937!important;color:#00d464!important;
    font-family:\'Space Mono\',monospace!important;font-size:10px!important;
    letter-spacing:1px!important;text-transform:uppercase!important;
}
[data-testid="stDataEditor"] td{background:#161b22!important;color:#e6edf3!important;font-size:12px!important;}
[data-testid="stDataEditor"] tr:hover td{background:#1f2937!important;}

/* ── DataFrames ── */
.stDataFrame{border:1px solid #30363d!important;border-radius:10px!important;}
[data-testid="stDataFrame"] th{background:#1f2937!important;color:#00d464!important;}
[data-testid="stDataFrame"] td{color:#e6edf3!important;}

/* ── Expander ── */
[data-testid="stExpander"]{
    border:1px solid #30363d!important;border-radius:8px!important;
    background:#161b22!important;
}
[data-testid="stExpander"] summary{color:#e6edf3!important;background:#161b22!important;}
[data-testid="stExpander"] summary:hover{background:#1f2937!important;}
[data-testid="stExpander"] [data-testid="stMarkdownContainer"] p{color:#e6edf3!important;}

/* ── Buttons ── */
.stButton>button{
    background:linear-gradient(135deg,#00d464,#00a84f)!important;
    color:#0d1117!important;
    font-family:\'Space Mono\',monospace!important;
    font-weight:700!important;font-size:11px!important;
    letter-spacing:.8px!important;border:none!important;
    border-radius:10px!important;padding:10px 20px!important;
    text-transform:uppercase!important;width:100%!important;
    transition:all .2s!important;
}
.stButton>button:hover{
    transform:translateY(-1px)!important;
    box-shadow:0 6px 20px rgba(0,212,100,.35)!important;
}
.stDownloadButton>button{
    background:transparent!important;color:#00d464!important;
    border:1px solid #00d464!important;font-weight:600!important;
    border-radius:10px!important;width:100%!important;
}
.stDownloadButton>button:hover{background:rgba(0,212,100,.1)!important;}
[data-testid="stForm"]{
    background:#161b22!important;border:1px solid #30363d!important;
    border-radius:8px!important;padding:12px!important;
}

/* ── Metrics ── */
[data-testid="metric-container"]{
    background:#161b22!important;border:1px solid #30363d!important;
    border-radius:10px!important;padding:14px!important;
}
[data-testid="metric-container"] label{
    font-size:10px!important;text-transform:uppercase!important;
    letter-spacing:1px!important;color:#8b949e!important;
}
[data-testid="stMetricValue"]{
    font-family:\'Space Mono\',monospace!important;
    font-size:20px!important;color:#00d464!important;
}
[data-testid="stMetricDelta"]{color:#8b949e!important;font-size:12px!important;}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"]{
    background:#161b22!important;
    border-bottom:1px solid #30363d!important;gap:0!important;
}
.stTabs [data-baseweb="tab"]{
    font-size:13px!important;font-weight:500!important;
    color:#8b949e!important;background:transparent!important;
    border:none!important;border-bottom:2px solid transparent!important;
    padding:12px 16px!important;transition:all .15s!important;
}
.stTabs [aria-selected="true"]{
    color:#e6edf3!important;border-bottom:2px solid #00d464!important;background:transparent!important;
}
.stTabs [data-baseweb="tab"]:hover{color:#e6edf3!important;}

/* ── Progress / alerts / misc ── */
.stProgress>div>div>div{background:linear-gradient(90deg,#00d464,#3b82f6)!important;}
.stProgress [data-testid="stText"]{color:#e6edf3!important;}
.stAlert{border-radius:8px!important;}
hr{border-color:#30363d!important;}

/* ── Section label ── */
.section-label{
    font-family:\'Space Mono\',monospace;font-size:10px;font-weight:700;
    letter-spacing:1.5px;color:#00d464;text-transform:uppercase;
    margin-bottom:6px;display:block;
}
.kpi-card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:16px 20px;text-align:center;}
.kpi-value{font-family:\'Space Mono\',monospace;font-size:22px;font-weight:700;color:#00d464;line-height:1;}
.kpi-label{font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:1px;margin-top:4px;}

/* ── Map / Plotly frames ── */
.stfolium-container iframe{border:1px solid #30363d!important;border-radius:10px!important;}
[data-testid="stPlotlyChart"]{
    background:#161b22!important;border-radius:10px!important;
    border:1px solid #30363d!important;padding:4px!important;
}

/* ── Scrollbars ── */
::-webkit-scrollbar{width:6px;height:6px;}
::-webkit-scrollbar-track{background:#0d1117;}
::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px;}
::-webkit-scrollbar-thumb:hover{background:#8b949e;}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
DATA_DIR = Path("./Delco_data")
DATA_DIR.mkdir(exist_ok=True)

SITE_COLORS = [
    "#00d464","#3b82f6","#f59e0b","#ef4444","#8b5cf6",
    "#06b6d4","#f97316","#10b981","#e879f9","#facc15",
    "#64748b","#ec4899",
]

# Plotly color sequences matching SITE_COLORS (rgba friendly)
SITE_COLORS_PLOTLY = SITE_COLORS

POLLUTANT_ALIASES = {
    "pm25": ["pm25","pm2_5","pm2.5","pm25_env"],
    "pm10": ["pm10","pm1_0","pm10_env"],
    "temp": ["temp","temperature","temp_manifold"],
    "rh":   ["rh","humidity","relative_humidity"],
    "co":   ["co","co_we","co_aux"],
    "no2":  ["no2","no2_we","no2_aux"],
    "o3":   ["o3","ox_we"],
    "no":   ["no","no_we"],
    "co2":  ["co2"],
}

POLLUTANT_UNITS = {
    "pm25":"µg/m³","pm10":"µg/m³","temp":"°C","rh":"%",
    "co":"ppb","no2":"ppb","o3":"ppb","no":"ppb","co2":"ppm",
}

# WHO / EPA guideline thresholds (24-h avg PM2.5)
PM25_THRESHOLDS = [
    (0,   12,  "Good",           "#00d464"),
    (12,  35,  "Moderate",       "#facc15"),
    (35,  55,  "Unhealthy (S.G.)","#f97316"),
    (55,  150, "Unhealthy",      "#ef4444"),
    (150, 250, "Very Unhealthy", "#8b5cf6"),
    (250, 9999,"Hazardous",      "#7f1d1d"),
]

DIR_ANGLES = {
    "N":0,"NNE":22.5,"NE":45,"ENE":67.5,
    "E":90,"ESE":112.5,"SE":135,"SSE":157.5,
    "S":180,"SSW":202.5,"SW":225,"WSW":247.5,
    "W":270,"WNW":292.5,"NW":315,"NNW":337.5,
}

DARK = dict(
    paper_bgcolor="#161b22",
    plot_bgcolor="#0d1117",
    font=dict(family="DM Sans,sans-serif", color="#8b949e"),
    xaxis=dict(gridcolor="#30363d", zerolinecolor="#30363d"),
    yaxis=dict(gridcolor="#30363d", zerolinecolor="#30363d"),
    legend=dict(orientation="h", y=-0.22,
                bgcolor="rgba(0,0,0,0)", font=dict(color="#8b949e")),
    margin=dict(l=55, r=20, t=50, b=90),
    hovermode="x unified",
)


def dk(**extra):
    """
    Return DARK kwargs safely merged with extra keyword arguments.

    Plotly's update_layout() raises 'multiple values for keyword argument'
    when a key appears both in **DARK and as an explicit kwarg (e.g. xaxis=).
    This helper removes any key from DARK that is also in extra, then
    deep-merges the two dicts so axis gridcolors are never lost.

    Usage:  fig.update_layout(**dk(xaxis=dict(title="Hour", dtick=3)))
    """
    base = {}
    for k, v in DARK.items():
        if k in extra and isinstance(v, dict) and isinstance(extra[k], dict):
            # Deep-merge: DARK defaults first, caller overrides second
            merged = dict(v)
            merged.update(extra[k])
            base[k] = merged
        elif k not in extra:
            base[k] = v
    # Add everything from extra (already deep-merged above where needed)
    for k, v in extra.items():
        if k not in base:
            base[k] = v
    return base

# ══════════════════════════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def hex_rgba(hex_color, alpha=0.25):
    """'#RRGGBB' → 'rgba(r,g,b,a)' — correct integer conversion."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return "rgba({},{},{},{})".format(r,g,b,alpha)


def normalize_columns(df):
    df.columns = (df.columns.str.strip().str.lower()
                  .str.replace(" ","_",regex=False)
                  .str.replace("-","_",regex=False))
    return df


def resolve_col(df, aliases):
    lc = {c.lower().strip(): c for c in df.columns}
    for a in aliases:
        if a.lower() in lc:
            return lc[a.lower()]
    return None


def normalize_pollutants(df):
    for std, aliases in POLLUTANT_ALIASES.items():
        if std not in df.columns:
            src = resolve_col(df, aliases)
            if src:
                df[std] = pd.to_numeric(df[src], errors="coerce")
    return df


def extract_lat_lon(geo):
    if isinstance(geo, dict):
        if "lat" in geo and "lon" in geo:
            return float(geo["lat"]), float(geo["lon"])
        if "coordinates" in geo and len(geo["coordinates"]) >= 2:
            return float(geo["coordinates"][1]), float(geo["coordinates"][0])
    if isinstance(geo, (list,tuple)) and len(geo) >= 2:
        return float(geo[1]), float(geo[0])
    return np.nan, np.nan


def pm25_category(val):
    if pd.isna(val):
        return ("N/A", "#8b949e")
    for lo, hi, label, color in PM25_THRESHOLDS:
        if lo <= val < hi:
            return (label, color)
    return ("Hazardous", "#7f1d1d")


def auto_start_from_csvs():
    pattern = re.compile(r"to_(\d{4}-\d{2}-\d{2})[_ ](\d{2}-\d{2}-\d{2})")
    times = []
    for f in DATA_DIR.glob("*.csv"):
        m = pattern.search(f.name)
        if m:
            try:
                dt_str = "{} {}".format(m.group(1), m.group(2).replace("-",":"))
                times.append(datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S"))
            except ValueError:
                pass
    return (max(times) - timedelta(hours=24)) if times else None

# ══════════════════════════════════════════════════════════════════════════════
#  AQI HELPERS
# ══════════════════════════════════════════════════════════════════════════════

# EPA breakpoints: (C_lo, C_hi, I_lo, I_hi)
_PM25_BP = [
    (0.0,   12.0,   0,   50),
    (12.1,  35.4,  51,  100),
    (35.5,  55.4, 101,  150),
    (55.5, 150.4, 151,  200),
    (150.5, 250.4, 201, 300),
    (250.5, 350.4, 301, 400),
    (350.5, 500.4, 401, 500),
]

AQI_CATEGORY_COLORS = {
    "Good":                            "#00d464",
    "Moderate":                        "#facc15",
    "Unhealthy for Sensitive Groups":  "#f97316",
    "Unhealthy":                       "#ef4444",
    "Very Unhealthy":                  "#8b5cf6",
    "Hazardous":                       "#7f1d1d",
    "Unknown":                         "#8b949e",
}


def pm25_to_aqi(pm25_val):
    """Convert PM2.5 concentration (µg/m³) to EPA AQI integer."""
    if pd.isna(pm25_val) or pm25_val < 0:
        return float("nan")
    pm = float(pm25_val)
    for c_lo, c_hi, i_lo, i_hi in _PM25_BP:
        if c_lo <= pm <= c_hi:
            return round((i_hi - i_lo) / (c_hi - c_lo) * (pm - c_lo) + i_lo)
    return 500 if pm > 500 else float("nan")


def aqi_category(aqi_val):
    """Return EPA AQI category string from numeric AQI."""
    if pd.isna(aqi_val):
        return "Unknown"
    a = float(aqi_val)
    if a <= 50:   return "Good"
    if a <= 100:  return "Moderate"
    if a <= 150:  return "Unhealthy for Sensitive Groups"
    if a <= 200:  return "Unhealthy"
    if a <= 300:  return "Very Unhealthy"
    return "Hazardous"

# ══════════════════════════════════════════════════════════════════════════════
#  API  — per-day endpoint + parallel ThreadPoolExecutor
# ══════════════════════════════════════════════════════════════════════════════

def download_one_day(api_key, sn, date_str):
    """
    Download a single calendar day using /data-by-date/{YYYY-MM-DD}/.
    Returns a cleaned DataFrame or None on any failure.
    No Streamlit calls inside — safe to use in threads.
    """
    url  = "https://api.quant-aq.com/device-api/v1/devices/{}/data-by-date/{}/".format(
        sn, date_str)
    auth = (api_key, "")
    try:
        r = requests.get(url, auth=auth, timeout=30)
        r.raise_for_status()
        payload = r.json()
        rows = payload.get("data", [])
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df = normalize_columns(df)
        # parse timestamp
        for ts_col in ["timestamp", "timestamp_local", "datetime"]:
            if ts_col in df.columns:
                df["datetime"] = pd.to_datetime(
                    df[ts_col], utc=True, errors="coerce")
                break
        if "datetime" not in df.columns:
            return None
        df = df.dropna(subset=["datetime"]).reset_index(drop=True)
        df = normalize_pollutants(df)
        return df
    except Exception:
        return None


def download_device(api_key, sn, start, end,
                    progress_cb=None, max_workers=6):
    """
    Download every calendar day in [start, end] in parallel using
    ThreadPoolExecutor (threads, not processes — works on all web hosts).

    progress_cb(completed, total, label) is called from the MAIN thread
    after each day finishes, so Streamlit progress updates are safe.
    """
    dates     = pd.date_range(start.date(), end.date(), freq="D")
    date_strs = [d.strftime("%Y-%m-%d") for d in dates]
    total     = len(date_strs)
    results   = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all days at once
        future_to_date = {
            executor.submit(download_one_day, api_key, sn, ds): ds
            for ds in date_strs
        }
        completed = 0
        for future in as_completed(future_to_date):
            ds = future_to_date[future]
            completed += 1
            try:
                df = future.result()
            except Exception:
                df = None
            if df is not None and not df.empty:
                results.append(df)
            # progress_cb is called in the main thread (as_completed yields here)
            if progress_cb:
                progress_cb(
                    completed, total,
                    "{} · day {}/{} ({})".format(sn, completed, total, ds)
                )

    if not results:
        return None

    df = pd.concat(results, ignore_index=True)
    df = df.sort_values("datetime").drop_duplicates(
        subset=["datetime"]).reset_index(drop=True)
    return df

# ══════════════════════════════════════════════════════════════════════════════
#  POST-DOWNLOAD PREPARATION  — hourly agg + rolling + AQI
# ══════════════════════════════════════════════════════════════════════════════

def prepare_merged_for_analysis(merged):
    """
    Given a raw merged DataFrame (all sites):
      • Sort by datetime
      • Extract lat/lon from geo column if present
      • Compute hourly averages per site
      • Add 1-hr and 24-hr rolling PM2.5 averages
      • Add AQI and AQI category columns
    Returns (merged_raw, hourly_df).
    """
    merged = merged.copy().sort_values("datetime").reset_index(drop=True)

    # Extract geo if present
    if "geo" in merged.columns:
        latlon = merged["geo"].apply(extract_lat_lon)
        merged["lat"] = latlon.apply(lambda x: x[0])
        merged["lon"] = latlon.apply(lambda x: x[1])

    # Hourly aggregation per site
    merged["hour"] = merged["datetime"].dt.floor("h")
    numeric_cols = merged.select_dtypes(include=[np.number]).columns.tolist()
    # Exclude helper columns from aggregation
    agg_cols = [c for c in numeric_cols if c not in ("lat","lon")]

    if not agg_cols:
        return merged, pd.DataFrame()

    group_cols = ["site_name","hour"] if "site_name" in merged.columns else ["hour"]
    hourly = (merged.groupby(group_cols, as_index=False)[agg_cols]
                    .mean()
                    .reset_index(drop=True))

    # Rolling averages + AQI on hourly PM2.5
    if "pm25" in hourly.columns:
        hourly = hourly.sort_values(group_cols)
        grp_col = "site_name" if "site_name" in hourly.columns else None

        def rolling_by_site(col, window):
            if grp_col:
                return hourly.groupby(grp_col)[col].transform(
                    lambda x: x.rolling(window, min_periods=1).mean())
            return hourly[col].rolling(window, min_periods=1).mean()

        hourly["pm25_1hr"]  = rolling_by_site("pm25", 1)
        hourly["pm25_24hr"] = rolling_by_site("pm25", 24)
        hourly["aqi"]       = hourly["pm25"].apply(pm25_to_aqi)
        hourly["aqi_category"] = hourly["aqi"].apply(aqi_category)

    return merged, hourly

# ══════════════════════════════════════════════════════════════════════════════
#  GOOGLE MAPS HTML GENERATOR
# ══════════════════════════════════════════════════════════════════════════════

def generate_google_map_html(map_df, google_api_key, map_height=560):
    """
    Embed Google Maps JS API with coloured circle markers per site.
    map_df must have: site_name, lat, lon, aqi (numeric), aqi_category (str).
    Returns an HTML string rendered via st.components.v1.html().
    Falls back gracefully if no Google API key is provided.
    """
    center_lat = float(map_df["lat"].mean())
    center_lon = float(map_df["lon"].mean())

    # Build marker list for JS
    markers = []
    for _, row in map_df.iterrows():
        aqi_val = row.get("aqi", np.nan)
        pm25_val = row.get("pm25", np.nan)
        pm10_val = row.get("pm10", np.nan)
        markers.append({
            "site_name":    str(row["site_name"]),
            "lat":          float(row["lat"]),
            "lon":          float(row["lon"]),
            "aqi":          None if pd.isna(aqi_val) else float(aqi_val),
            "aqi_category": str(row.get("aqi_category","Unknown")),
            "pm25":         None if pd.isna(pm25_val) else round(float(pm25_val),2),
            "pm10":         None if pd.isna(pm10_val)  else round(float(pm10_val),2),
        })
    markers_json = json.dumps(markers)

    # Colour map from AQI_CATEGORY_COLORS
    color_map_js = json.dumps(AQI_CATEGORY_COLORS)

    html = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="initial-scale=1.0"/>
  <style>
    body {{ margin:0; padding:0; background:#0d1117; }}
    #map {{ height:{height}px; width:100%; }}
    .gm-iw {{ font-family:'DM Sans',Arial,sans-serif; font-size:13px; }}
    .iw-title {{ font-weight:700; font-size:14px; margin-bottom:6px; }}
    .iw-row {{ display:flex; justify-content:space-between;
               gap:12px; margin-bottom:2px; font-size:12px; }}
    .iw-label {{ color:#666; }}
    .aqi-dot {{ display:inline-block; width:11px; height:11px;
                border-radius:50%; margin-right:5px; vertical-align:middle; }}
    .legend {{ position:fixed; bottom:28px; left:28px; z-index:9999;
               background:rgba(13,17,23,0.93);
               border:1px solid #30363d; border-radius:8px;
               padding:12px 16px; font-family:monospace;
               font-size:11px; color:#e6edf3; min-width:200px; }}
    .legend-title {{ color:#00d464; font-weight:700;
                     letter-spacing:1px; margin-bottom:8px; }}
    .legend-row {{ display:flex; align-items:center; margin-bottom:4px; gap:8px; }}
  </style>
  <script src="https://maps.googleapis.com/maps/api/js?key={api_key}"></script>
</head>
<body>
  <div id="map"></div>
  <div class="legend">
    <div class="legend-title">PM2.5 AQI</div>
    <div class="legend-row"><span class="aqi-dot" style="background:#00d464"></span>Good (0–50)</div>
    <div class="legend-row"><span class="aqi-dot" style="background:#facc15"></span>Moderate (51–100)</div>
    <div class="legend-row"><span class="aqi-dot" style="background:#f97316"></span>USG (101–150)</div>
    <div class="legend-row"><span class="aqi-dot" style="background:#ef4444"></span>Unhealthy (151–200)</div>
    <div class="legend-row"><span class="aqi-dot" style="background:#8b5cf6"></span>Very Unhealthy (201–300)</div>
    <div class="legend-row"><span class="aqi-dot" style="background:#7f1d1d"></span>Hazardous (300+)</div>
  </div>
  <script>
    const MARKERS   = {markers_json};
    const COLOR_MAP = {color_map_js};

    function colorForCat(cat) {{
      return COLOR_MAP[cat] || COLOR_MAP["Unknown"];
    }}

    const map = new google.maps.Map(document.getElementById("map"), {{
      zoom: 10,
      center: {{lat: {center_lat}, lng: {center_lon}}},
      mapTypeId: "roadmap",
      styles: [
        {{elementType:"geometry",stylers:[{{color:"#212121"}}]}},
        {{elementType:"labels.icon",stylers:[{{visibility:"off"}}]}},
        {{elementType:"labels.text.fill",stylers:[{{color:"#757575"}}]}},
        {{elementType:"labels.text.stroke",stylers:[{{color:"#212121"}}]}},
        {{featureType:"road",elementType:"geometry",stylers:[{{color:"#2c2c2c"}}]}},
        {{featureType:"road.highway",elementType:"geometry",stylers:[{{color:"#3c3c3c"}}]}},
        {{featureType:"water",elementType:"geometry",stylers:[{{color:"#000000"}}]}},
        {{featureType:"poi",elementType:"labels",stylers:[{{visibility:"off"}}]}}
      ]
    }});

    let openWindow = null;

    MARKERS.forEach(function(m) {{
      if (m.lat == null || m.lon == null) return;
      const color = colorForCat(m.aqi_category);
      const marker = new google.maps.Marker({{
        position: {{lat: m.lat, lng: m.lon}},
        map: map,
        title: m.site_name,
        icon: {{
          path: google.maps.SymbolPath.CIRCLE,
          scale: 13,
          fillColor: color,
          fillOpacity: 0.92,
          strokeWeight: 1.5,
          strokeColor: "#ffffff"
        }}
      }});

      const aqiText  = (m.aqi   == null) ? "N/A" : m.aqi.toFixed(0);
      const pm25Text = (m.pm25  == null) ? "N/A" : m.pm25.toFixed(2) + " µg/m³";
      const pm10Text = (m.pm10  == null) ? "N/A" : m.pm10.toFixed(2) + " µg/m³";

      const content = `
        <div class="gm-iw" style="min-width:200px;padding:4px 2px">
          <div class="iw-title">${{m.site_name}}</div>
          <div class="iw-row">
            <span class="iw-label">AQI</span>
            <span><span class="aqi-dot" style="background:${{color}}"></span>
              <strong>${{aqiText}}</strong> — ${{m.aqi_category}}</span>
          </div>
          <div class="iw-row">
            <span class="iw-label">PM2.5</span><span>${{pm25Text}}</span>
          </div>
          <div class="iw-row">
            <span class="iw-label">PM10</span><span>${{pm10Text}}</span>
          </div>
          <div class="iw-row">
            <span class="iw-label">Lat / Lon</span>
            <span>${{m.lat.toFixed(4)}}, ${{m.lon.toFixed(4)}}</span>
          </div>
        </div>`;

      const infowindow = new google.maps.InfoWindow({{content}});
      marker.addListener("click", function() {{
        if (openWindow) openWindow.close();
        infowindow.open(map, marker);
        openWindow = infowindow;
      }});
    }});
  </script>
</body>
</html>""".format(
        height=map_height,
        api_key=google_api_key,
        markers_json=markers_json,
        color_map_js=color_map_js,
        center_lat=center_lat,
        center_lon=center_lon,
    )
    return html

# ══════════════════════════════════════════════════════════════════════════════
#  STATISTICS
# ══════════════════════════════════════════════════════════════════════════════

def describe_site(df):
    cols = [c for c in POLLUTANT_ALIASES if c in df.columns]
    rows = []
    for col in cols:
        s = df[col].dropna()
        if len(s) < 2:
            continue
        mv = float(s.mean())
        rows.append({
            "Pollutant": col.upper(),
            "Unit":      POLLUTANT_UNITS.get(col,""),
            "N":         int(len(s)),
            "Mean":      round(mv, 3),
            "Median":    round(float(s.median()), 3),
            "Std Dev":   round(float(s.std()), 3),
            "Min":       round(float(s.min()), 3),
            "5th %":     round(float(np.percentile(s,5)), 3),
            "25th %":    round(float(s.quantile(0.25)), 3),
            "75th %":    round(float(s.quantile(0.75)), 3),
            "95th %":    round(float(np.percentile(s,95)), 3),
            "Max":       round(float(s.max()), 3),
            "Skewness":  round(float(stats.skew(s)), 3),
            "Kurtosis":  round(float(stats.kurtosis(s)), 3),
            "CV%":       round(s.std()/mv*100,2) if mv != 0 else float("nan"),
        })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).set_index("Pollutant")

# ══════════════════════════════════════════════════════════════════════════════
#  PLOTLY CHARTS
# ══════════════════════════════════════════════════════════════════════════════

def chart_timeseries_site(df, site, color):
    avail = [c for c in list(POLLUTANT_ALIASES.keys())
             if c in df.columns and df[c].notna().any()]
    if not avail:
        return go.Figure()
    n = len(avail)
    titles = ["{} ({})".format(c.upper(), POLLUTANT_UNITS.get(c,"")) for c in avail]
    fig = make_subplots(rows=n, cols=1, shared_xaxes=True,
                        subplot_titles=titles, vertical_spacing=0.04)
    for i, col in enumerate(avail, 1):
        s = df.dropna(subset=[col])
        fig.add_trace(go.Scatter(
            x=s["datetime"], y=s[col], name=col.upper(),
            mode="lines", line=dict(color=color, width=1.3),
            fill="tozeroy", fillcolor=hex_rgba(color,0.07),
            showlegend=(i==1),
            hovertemplate="%{{y:.2f}} {}<extra>{}</extra>".format(
                POLLUTANT_UNITS.get(col,""), col.upper()),
        ), row=i, col=1)
    lkw = {k:v for k,v in DARK.items() if k not in ("xaxis","yaxis")}
    fig.update_layout(
        title=dict(text="Time Series — {}".format(site),
                   font=dict(color="#e6edf3",size=14)),
        height=210*n, **lkw)
    for i in range(1, n+1):
        fig.update_xaxes(gridcolor="#30363d", row=i, col=1)
        fig.update_yaxes(gridcolor="#30363d", row=i, col=1)
    return fig


def chart_timeseries_compare(data, pollutant, colors):
    fig = go.Figure()
    for site, df in data.items():
        if df is None or pollutant not in df.columns:
            continue
        s = df.dropna(subset=[pollutant])
        fig.add_trace(go.Scatter(
            x=s["datetime"], y=s[pollutant], name=site,
            mode="lines", line=dict(color=colors[site], width=1.6),
            hovertemplate="%{{y:.2f}} {}<extra>{}</extra>".format(
                POLLUTANT_UNITS.get(pollutant,""), site),
        ))
    fig.update_layout(
        title=dict(text="{} — All Sites".format(pollutant.upper()),
                   font=dict(color="#e6edf3",size=14)),
        yaxis_title=POLLUTANT_UNITS.get(pollutant,""), **DARK)
    return fig


def chart_histogram(df, col, site, color):
    s = df[col].dropna()
    if len(s) < 5:
        return go.Figure()
    counts, edges = np.histogram(s, bins=50)
    mids = (edges[:-1]+edges[1:])/2
    kde  = stats.gaussian_kde(s)
    xk   = np.linspace(float(s.min()), float(s.max()), 300)
    yk   = kde(xk)*len(s)*(edges[1]-edges[0])
    fig  = go.Figure()
    fig.add_trace(go.Bar(x=mids, y=counts, name="Count",
                         marker_color=hex_rgba(color,0.6),
                         marker_line_color=color, marker_line_width=0.5))
    fig.add_trace(go.Scatter(x=xk, y=yk, name="KDE",
                             mode="lines", line=dict(color=color,width=2.5)))
    fig.add_vline(x=float(s.mean()), line_color="#fbbf24", line_dash="dash",
                  annotation_text="Mean {:.2f}".format(float(s.mean())),
                  annotation_font_color="#fbbf24")
    fig.add_vline(x=float(s.median()), line_color="#e879f9", line_dash="dot",
                  annotation_text="Median {:.2f}".format(float(s.median())),
                  annotation_font_color="#e879f9")

    # Add AQI thresholds for PM2.5
    if col == "pm25":
        for _, hi, label, tc in PM25_THRESHOLDS[:4]:
            if hi < float(s.max()):
                fig.add_vline(x=hi, line_color=tc, line_dash="longdash",
                              line_width=1, opacity=0.5,
                              annotation_text=label,
                              annotation_font_color=tc)

    fig.update_layout(
        title=dict(text="{} Distribution — {}".format(col.upper(),site),
                   font=dict(color="#e6edf3",size=13)),
        xaxis_title="{} ({})".format(col.upper(),POLLUTANT_UNITS.get(col,"")),
        yaxis_title="Count", bargap=0.04, **DARK)
    return fig


def chart_diurnal(df, col, site, color):
    s = df.dropna(subset=[col]).copy()
    s["hour"] = s["datetime"].dt.hour
    grp = s.groupby("hour")[col].agg(["mean","std","count"]).reset_index()
    grp["se"] = grp["std"] / np.sqrt(grp["count"])
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=pd.concat([grp["hour"], grp["hour"][::-1]]),
        y=pd.concat([grp["mean"]+grp["se"], (grp["mean"]-grp["se"])[::-1]]),
        fill="toself", fillcolor=hex_rgba(color,0.15),
        line=dict(color="rgba(0,0,0,0)"), showlegend=False))
    fig.add_trace(go.Scatter(
        x=grp["hour"], y=grp["mean"], name="Hourly mean",
        mode="lines+markers",
        line=dict(color=color,width=2.2), marker=dict(size=7),
        hovertemplate="Hour %{x}:00 → %{y:.2f}<extra></extra>"))
    fig.update_layout(
        title=dict(text="{} Diurnal Pattern — {}".format(col.upper(),site),
                   font=dict(color="#e6edf3",size=13)),
        yaxis_title="{} ({})".format(col.upper(),POLLUTANT_UNITS.get(col,"")),
        **dk(xaxis=dict(title="Hour of Day", tickmode="linear",
                        dtick=3, gridcolor="#30363d")))
    return fig


def chart_weekly(df, col, site, color):
    """Box-plot per day of week."""
    s = df.dropna(subset=[col]).copy()
    s["dow"] = s["datetime"].dt.day_name()
    order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    fig = go.Figure()
    for day in order:
        vals = s[s["dow"]==day][col]
        if vals.empty:
            continue
        fig.add_trace(go.Box(
            y=vals, name=day[:3],
            marker_color=color, line_color=color,
            fillcolor=hex_rgba(color,0.2), boxmean="sd",
            showlegend=False))
    fig.update_layout(
        title=dict(text="{} by Day of Week — {}".format(col.upper(),site),
                   font=dict(color="#e6edf3",size=13)),
        yaxis_title="{} ({})".format(col.upper(),POLLUTANT_UNITS.get(col,"")),
        **DARK)
    return fig


def chart_monthly(df, col, site, color):
    """Monthly mean ± std bar chart."""
    s = df.dropna(subset=[col]).copy()
    s["month"] = s["datetime"].dt.to_period("M")
    grp = s.groupby("month")[col].agg(["mean","std"]).reset_index()
    grp["month_str"] = grp["month"].astype(str)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=grp["month_str"], y=grp["mean"],
        error_y=dict(type="data", array=grp["std"].fillna(0).tolist(), visible=True,
                     color="#8b949e"),
        marker_color=hex_rgba(color,0.7),
        marker_line_color=color, marker_line_width=1,
        name="Monthly Mean"))
    fig.update_layout(
        title=dict(text="{} Monthly Average — {}".format(col.upper(),site),
                   font=dict(color="#e6edf3",size=13)),
        xaxis_title="Month",
        yaxis_title="{} ({})".format(col.upper(),POLLUTANT_UNITS.get(col,"")),
        **DARK)
    return fig


def chart_rolling(df, col, site, color):
    tmp = df.set_index("datetime")[col].dropna()
    if len(tmp) < 5:
        return go.Figure()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=tmp.index, y=tmp.values, name="Raw",
                             mode="lines",
                             line=dict(color=hex_rgba(color,0.3),width=0.6)))
    for label, w, wc, ww in [("1-h",1,"#fbbf24",1.4),
                               ("3-h",3,"#f97316",1.8),
                               ("24-h",24,"#ef4444",2.2)]:
        rolled = tmp.rolling(window=w, min_periods=max(1,w//2)).mean()
        fig.add_trace(go.Scatter(x=rolled.index, y=rolled.values,
                                  name=label, mode="lines",
                                  line=dict(color=wc,width=ww)))
    fig.update_layout(
        title=dict(text="{} Rolling Avg — {}".format(col.upper(),site),
                   font=dict(color="#e6edf3",size=13)),
        yaxis_title=POLLUTANT_UNITS.get(col,""), **DARK)
    return fig


def chart_boxplot_compare(data, col, colors):
    fig = go.Figure()
    for site, df in data.items():
        if df is None or col not in df.columns:
            continue
        s = df[col].dropna()
        if s.empty:
            continue
        c = colors[site]
        fig.add_trace(go.Box(
            y=s, name=site, marker_color=c, line_color=c,
            fillcolor=hex_rgba(c,0.2), boxmean="sd"))
    fig.update_layout(
        title=dict(text="{} — Site Comparison".format(col.upper()),
                   font=dict(color="#e6edf3",size=13)),
        yaxis_title=POLLUTANT_UNITS.get(col,""), **DARK)
    return fig


def chart_correlation(df, site):
    cols = [c for c in POLLUTANT_ALIASES
            if c in df.columns and df[c].notna().sum() > 10]
    if len(cols) < 2:
        return go.Figure()
    corr = df[cols].corr().round(2)
    labels = [c.upper() for c in corr.columns]
    lkw = {k:v for k,v in DARK.items() if k not in ("xaxis","yaxis","hovermode")}
    fig = go.Figure(go.Heatmap(
        z=corr.values, x=labels, y=labels,
        colorscale=[[0,"#3b82f6"],[0.5,"#0d1117"],[1,"#ef4444"]],
        zmin=-1, zmax=1,
        text=corr.values.round(2), texttemplate="%{text}",
        textfont=dict(size=11),
        hovertemplate="%{y} vs %{x}: %{z:.2f}<extra></extra>"))
    fig.update_layout(
        title=dict(text="Correlation Matrix — {}".format(site),
                   font=dict(color="#e6edf3",size=13)),
        xaxis=dict(gridcolor="#30363d"),
        yaxis=dict(gridcolor="#30363d"),
        height=460, **lkw)
    return fig


def chart_scatter_pair2(df, col_x, col_y, site, color):
    """Scatter + linear regression for any two pollutants."""
    clean = df[[col_x,col_y]].dropna()
    if len(clean) < 5:
        return go.Figure()
    x = clean[col_x].values
    y = clean[col_y].values
    coeffs = np.polyfit(x, y, 1)
    xl = np.linspace(x.min(), x.max(), 200)
    yl = np.polyval(coeffs, xl)
    y_hat = np.polyval(coeffs, x)
    ss_res = np.sum((y-y_hat)**2)
    ss_tot = np.sum((y-y.mean())**2)
    r2 = 1 - ss_res/ss_tot if ss_tot > 0 else float("nan")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x, y=y, mode="markers", name="Obs",
        marker=dict(color=color, opacity=0.5, size=5),
        hovertemplate="{}: %{{x:.2f}}<br>{}: %{{y:.2f}}<extra></extra>".format(
            col_x.upper(), col_y.upper())))
    fig.add_trace(go.Scatter(
        x=xl, y=yl, mode="lines", name="Linear fit",
        line=dict(color="#fbbf24", width=2, dash="dash")))
    fig.update_layout(
        title=dict(text="{} vs {}  |  R²={:.3f}  slope={:.3f} — {}".format(
            col_x.upper(), col_y.upper(), r2, coeffs[0], site),
                   font=dict(color="#e6edf3",size=13)),
        xaxis_title="{} ({})".format(col_x.upper(),POLLUTANT_UNITS.get(col_x,"")),
        yaxis_title="{} ({})".format(col_y.upper(),POLLUTANT_UNITS.get(col_y,"")),
        **DARK)
    return fig

def chart_scatter_pair(df, col_x, col_y, site, color, **kwargs):
    """Scatter + linear regression for any two pollutants.

    Accepts extra kwargs and ignores them so it is safe to call from
    Streamlit callbacks or widgets that may pass a 'key' or other kwargs.
    """
    # Validate columns exist
    if col_x not in df.columns or col_y not in df.columns:
        return go.Figure()

    clean = df[[col_x, col_y]].dropna()
    if len(clean) < 5:
        return go.Figure()

    x = clean[col_x].values
    y = clean[col_y].values

    # Fit linear model y = m*x + b
    coeffs = np.polyfit(x, y, 1)
    slope, intercept = coeffs[0], coeffs[1]

    xl = np.linspace(x.min(), x.max(), 200)
    yl = np.polyval(coeffs, xl)
    y_hat = np.polyval(coeffs, x)

    ss_res = np.sum((y - y_hat) ** 2)
    ss_tot = np.sum((y - y.mean()) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=x, y=y, mode="markers", name="Observations",
        marker=dict(color=color, opacity=0.6, size=6),
        hovertemplate=f"{col_x.upper()}: %{{x:.2f}}<br>{col_y.upper()}: %{{y:.2f}}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        x=xl, y=yl, mode="lines", name="Linear fit",
        line=dict(color="#fbbf24", width=2, dash="dash")
    ))

    fig.update_layout(
        title=dict(
            text=f"{col_x.upper()} vs {col_y.upper()}  |  R²={r2:.3f}  slope={slope:.3f} — {site}",
            font=dict(color="#e6edf3", size=13)
        ),
        xaxis_title=f"{col_x.upper()} ({POLLUTANT_UNITS.get(col_x,'')})",
        yaxis_title=f"{col_y.upper()} ({POLLUTANT_UNITS.get(col_y,'')})",
        **DARK
    )

    return fig


def chart_wind_rose(df, site):
    wd_col = resolve_col(df, ["wd","wind_dir","wind_direction"])
    ws_col = resolve_col(df, ["ws","wind_speed","wind_spd"])
    if not wd_col or not ws_col:
        return None
    valid = df[[wd_col,ws_col]].dropna().copy()
    if len(valid) < 10:
        return None
    if valid[wd_col].dtype == object:
        valid["_angle"] = valid[wd_col].str.upper().str.strip().map(DIR_ANGLES)
    else:
        valid["_angle"] = pd.to_numeric(valid[wd_col], errors="coerce")
    valid = valid.dropna(subset=["_angle"])
    if valid.empty:
        return None
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSW","SW","WSW","W","WNW","NW","NNW"]
    speed_bins  = [0,2,5,10,15,9999]
    speed_names = ["0-2","2-5","5-10","10-15","15+"]
    rose_colors = ["#00d464","#3b82f6","#8b5cf6","#f59e0b","#ef4444"]
    total = len(valid)
    valid["_sector"] = pd.cut(valid["_angle"]%360, bins=np.linspace(0,360,17),
                               labels=dirs, right=False)
    valid[ws_col] = pd.to_numeric(valid[ws_col], errors="coerce")
    valid["_sbin"] = pd.cut(valid[ws_col], bins=speed_bins,
                             labels=speed_names, right=False)
    fig = go.Figure()
    for sbin, rc in zip(speed_names, rose_colors):
        sub  = valid[valid["_sbin"]==sbin]
        cnts = sub["_sector"].value_counts()
        fig.add_trace(go.Barpolar(
            r=[cnts.get(d,0)/total*100 for d in dirs],
            theta=dirs, name="{} m/s".format(sbin),
            marker_color=rc, opacity=0.85))
    fig.update_layout(
        title=dict(text="Wind Rose — {}".format(site),
                   font=dict(color="#e6edf3",size=13)),
        polar=dict(
            bgcolor="#0d1117",
            radialaxis=dict(visible=True, gridcolor="#30363d",
                            tickfont=dict(color="#8b949e",size=9)),
            angularaxis=dict(gridcolor="#30363d",
                             tickfont=dict(color="#8b949e",size=10))),
        paper_bgcolor="#161b22",
        font=dict(color="#8b949e"),
        legend=dict(orientation="h",y=-0.15,bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=30,r=30,t=50,b=60))
    return fig

# ══════════════════════════════════════════════════════════════════════════════
#  FOLIUM MAP  (like R leaflet — no token, multiple tile layers)
# ══════════════════════════════════════════════════════════════════════════════

def build_folium_map(geo_df, map_style="Dark", show_heatmap=False):
    """
    Interactive Folium map — OpenStreetMap / CartoDB tile layers.
    Markers coloured by PM2.5 AQI category.
    Popup shows site stats. Optional HeatMap layer.
    """
    valid = geo_df.dropna(subset=["lat","lon"])
    if valid.empty:
        return None

    center_lat = float(valid["lat"].mean())
    center_lon = float(valid["lon"].mean())

    # Choose tile layer
    tiles_map = {
        "Dark":    "CartoDB dark_matter",
        "Light":   "CartoDB positron",
        "Street":  "OpenStreetMap",
        "Satellite": "https://server.arcgisonline.com/ArcGIS/rest/services/"
                     "World_Imagery/MapServer/tile/{z}/{y}/{x}",
    }
    tile = tiles_map.get(map_style, "CartoDB dark_matter")

    if map_style == "Satellite":
        m = folium.Map(location=[center_lat, center_lon], zoom_start=10,
                       tiles=tile,
                       attr="Esri World Imagery")
    else:
        m = folium.Map(location=[center_lat, center_lon], zoom_start=10,
                       tiles=tile)

    # Layer control
    folium.TileLayer("CartoDB positron",  name="Light").add_to(m)
    folium.TileLayer("CartoDB dark_matter", name="Dark").add_to(m)
    folium.TileLayer("OpenStreetMap",     name="Street").add_to(m)

    marker_cluster = MarkerCluster(name="Sites").add_to(m)

    for _, row in valid.iterrows():
        pm25_val = row.get("pm25", np.nan)
        pm10_val = row.get("pm10", np.nan)
        label, cat_color = pm25_category(pm25_val)

        # Custom circle marker coloured by AQI
        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=14,
            color="white",
            weight=1.5,
            fill=True,
            fill_color=cat_color,
            fill_opacity=0.85,
            tooltip="{} | PM2.5: {} µg/m³".format(
                row["site_name"],
                "{:.1f}".format(pm25_val) if not pd.isna(pm25_val) else "N/A"),
            popup=folium.Popup("""
                <div style="font-family:monospace;min-width:200px;">
                <b style="font-size:14px">{site}</b><br>
                <hr style="margin:4px 0">
                <table style="width:100%;font-size:12px">
                  <tr><td>PM2.5</td><td><b>{pm25}</b> µg/m³</td></tr>
                  <tr><td>PM10</td><td><b>{pm10}</b> µg/m³</td></tr>
                  <tr><td>AQI Cat.</td>
                      <td><b style="color:{col}">{label}</b></td></tr>
                  <tr><td>Lat</td><td>{lat:.4f}</td></tr>
                  <tr><td>Lon</td><td>{lon:.4f}</td></tr>
                </table>
                </div>
            """.format(
                site=row["site_name"],
                pm25="{:.2f}".format(pm25_val) if not pd.isna(pm25_val) else "N/A",
                pm10="{:.2f}".format(pm10_val) if not pd.isna(pm10_val) else "N/A",
                col=cat_color, label=label,
                lat=row["lat"], lon=row["lon"],
            ), max_width=280),
        ).add_to(marker_cluster)

        # Label
        folium.Marker(
            location=[row["lat"], row["lon"]],
            icon=folium.DivIcon(
                html='<div style="font-family:monospace;font-size:10px;'
                     'font-weight:700;color:white;white-space:nowrap;'
                     'text-shadow:0 0 3px #000">{}</div>'.format(row["site_name"]),
                icon_size=(120, 20),
                icon_anchor=(0, -16),
            )
        ).add_to(m)

    # Optional PM2.5 heat-map layer
    if show_heatmap:
        heat_data = [
            [row["lat"], row["lon"], float(row["pm25"])]
            for _, row in valid.iterrows()
            if not pd.isna(row.get("pm25", np.nan))
        ]
        if heat_data:
            HeatMap(heat_data, name="PM2.5 Heatmap",
                    min_opacity=0.3, radius=40, blur=30,
                    gradient={0.2:"blue",0.4:"cyan",0.6:"lime",
                               0.8:"yellow",1:"red"}).add_to(m)

    # AQI legend
    legend_html = """
    <div style="position:fixed;bottom:30px;left:30px;z-index:9999;
         background:rgba(13,17,23,0.92);border:1px solid #30363d;
         border-radius:8px;padding:12px 16px;font-family:monospace;
         font-size:11px;color:#e6edf3;min-width:160px;">
      <b style="color:#00d464;letter-spacing:1px">PM2.5 AQI</b><br><br>
      {}
    </div>""".format("".join(
        '<div style="margin-bottom:3px">'
        '<span style="display:inline-block;width:12px;height:12px;'
        'border-radius:50%;background:{};margin-right:6px;'
        'vertical-align:middle"></span>{} ({}-{})</div>'.format(
            tc, label, lo, hi if hi < 9999 else "+")
        for lo, hi, label, tc in PM25_THRESHOLDS
    ))
    m.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl().add_to(m)
    return m

# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown('<p class="section-label">🔑 API Key</p>', unsafe_allow_html=True)
    api_key = st.text_input("API Key", type="password",
                             help="QuantAQ API key — used as Basic Auth username")

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown('<p class="section-label">📡 Device Manager</p>', unsafe_allow_html=True)

    # ── Initialise device table in session state ──────────────────────────────
    if "device_df" not in st.session_state:
        st.session_state.device_df = pd.DataFrame(
            columns=["sn","site_name","lat","lon"]
        )

    # ── Three input methods as radio ──────────────────────────────────────────
    input_mode = st.radio(
        "Add devices via:",
        ["📋 Paste list", "📁 Upload Excel", "➕ Add one"],
        horizontal=True,
        label_visibility="collapsed",
    )

    # ── MODE 1: Paste list ────────────────────────────────────────────────────
    if input_mode == "📋 Paste list":
        st.caption("One device per line:  `SN, Site Name, lat, lon`  "
                   "(lat/lon optional)")
        pasted = st.text_area(
            "Paste devices",
            placeholder="SN000001, Downtown, 39.952, -75.163\nSN000002, Airport",
            height=130,
            label_visibility="collapsed",
        )
        if st.button("➕ Load pasted list", use_container_width=True):
            rows = []
            for line in pasted.strip().splitlines():
                parts = [p.strip() for p in line.split(",")]
                if not parts[0]:
                    continue
                sn   = parts[0]
                name = parts[1] if len(parts) > 1 else sn
                try:
                    lat = float(parts[2]) if len(parts) > 2 and parts[2] else np.nan
                    lon = float(parts[3]) if len(parts) > 3 and parts[3] else np.nan
                except ValueError:
                    lat, lon = np.nan, np.nan
                rows.append({"sn": sn, "site_name": name,
                              "lat": lat, "lon": lon})
            if rows:
                new_df = pd.DataFrame(rows)
                # Merge — keep existing, add new, no duplicates by sn
                combined = pd.concat(
                    [st.session_state.device_df, new_df], ignore_index=True
                ).drop_duplicates(subset=["sn"], keep="last")
                st.session_state.device_df = combined.reset_index(drop=True)
                st.success("Loaded {} device(s)".format(len(rows)))

    # ── MODE 2: Upload Excel ──────────────────────────────────────────────────
    elif input_mode == "📁 Upload Excel":
        st.caption("Columns: `sn`, `site_name`  (optional: `lat`, `lon`)")
        up_file = st.file_uploader(
            "sensor_to_site.xlsx", type=["xlsx","xls"],
            label_visibility="collapsed",
        )
        if up_file:
            try:
                xdf = pd.read_excel(up_file)
                xdf.columns = xdf.columns.str.strip().str.lower()
                if not {"sn","site_name"}.issubset(xdf.columns):
                    st.error("Excel must contain: `sn`, `site_name`")
                else:
                    if "lat" not in xdf.columns:
                        xdf["lat"] = np.nan
                    if "lon" not in xdf.columns:
                        xdf["lon"] = np.nan
                    new_df = xdf[["sn","site_name","lat","lon"]].copy()
                    new_df["sn"] = new_df["sn"].astype(str)
                    new_df["site_name"] = new_df["site_name"].astype(str)
                    combined = pd.concat(
                        [st.session_state.device_df, new_df], ignore_index=True
                    ).drop_duplicates(subset=["sn"], keep="last")
                    st.session_state.device_df = combined.reset_index(drop=True)
                    st.success("Loaded {} device(s)".format(len(new_df)))
            except Exception as exc:
                st.error("Excel error: {}".format(exc))

    # ── MODE 3: Add one device ────────────────────────────────────────────────
    elif input_mode == "➕ Add one":
        with st.form("add_device_form", clear_on_submit=True):
            fc1, fc2 = st.columns(2)
            with fc1:
                new_sn   = st.text_input("Serial Number *", placeholder="SN000001")
            with fc2:
                new_name = st.text_input("Site Name *",     placeholder="Downtown")
            fc3, fc4 = st.columns(2)
            with fc3:
                new_lat = st.text_input("Latitude",  placeholder="39.9526")
            with fc4:
                new_lon = st.text_input("Longitude", placeholder="-75.1652")
            submitted = st.form_submit_button("➕ Add Device",
                                              use_container_width=True)
            if submitted:
                if not new_sn.strip():
                    st.error("Serial Number is required.")
                else:
                    try:
                        lat_v = float(new_lat) if new_lat.strip() else np.nan
                        lon_v = float(new_lon) if new_lon.strip() else np.nan
                    except ValueError:
                        lat_v, lon_v = np.nan, np.nan
                        st.warning("Lat/Lon must be numbers — saved as blank.")
                    new_row = pd.DataFrame([{
                        "sn":        new_sn.strip(),
                        "site_name": new_name.strip() or new_sn.strip(),
                        "lat":       lat_v,
                        "lon":       lon_v,
                    }])
                    combined = pd.concat(
                        [st.session_state.device_df, new_row], ignore_index=True
                    ).drop_duplicates(subset=["sn"], keep="last")
                    st.session_state.device_df = combined.reset_index(drop=True)
                    st.success("Added: {} — {}".format(
                        new_sn.strip(), new_name.strip() or new_sn.strip()))

    # ── Live editable device table ────────────────────────────────────────────
    st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)

    if not st.session_state.device_df.empty:
        n_dev = len(st.session_state.device_df)
        st.markdown(
            '<span style="font-size:11px;color:#00d464;font-family:Space Mono,'
            'monospace;font-weight:700">{} DEVICE(S) CONFIGURED</span>'.format(n_dev),
            unsafe_allow_html=True,
        )

        # st.data_editor — inline-editable, rows can be deleted
        edited = st.data_editor(
            st.session_state.device_df,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",       # allows adding/deleting rows in UI
            column_config={
                "sn":        st.column_config.TextColumn("Serial No.",  width="medium"),
                "site_name": st.column_config.TextColumn("Site Name",   width="medium"),
                "lat":       st.column_config.NumberColumn("Lat",  format="%.4f", width="small"),
                "lon":       st.column_config.NumberColumn("Lon",  format="%.4f", width="small"),
            },
            key="device_editor",
        )
        # Sync edits back to session state
        st.session_state.device_df = edited.dropna(
            subset=["sn"]).reset_index(drop=True)

        # Action buttons row
        btn_clear, btn_export = st.columns(2)
        with btn_clear:
            if st.button("🗑 Clear All", use_container_width=True):
                st.session_state.device_df = pd.DataFrame(
                    columns=["sn","site_name","lat","lon"])
                st.rerun()
        with btn_export:
            csv_bytes = st.session_state.device_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇ Export List",
                data=csv_bytes,
                file_name="sensor_to_site_{}.csv".format(
                    datetime.now().strftime("%Y%m%d")),
                mime="text/csv",
                use_container_width=True,
            )
    else:
        st.caption("No devices yet — paste, upload, or add one above.")

    # ── Build sensor_map / device_list / lat_lon_map from device_df ──────────
    sensor_map  = {}
    device_list = []
    lat_lon_map = {}
    for _, row in st.session_state.device_df.iterrows():
        sn = str(row["sn"]).strip()
        if not sn:
            continue
        sensor_map[sn]  = str(row["site_name"]).strip() or sn
        device_list.append(sn)
        if not pd.isna(row.get("lat")) and not pd.isna(row.get("lon")):
            lat_lon_map[sn] = (float(row["lat"]), float(row["lon"]))

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown('<p class="section-label">📅 Date & Time Range</p>', unsafe_allow_html=True)
    st.caption("Any range — auto-splits into 30-day chunks per device.")

    auto_start = auto_start_from_csvs()
    default_start = auto_start if auto_start else (datetime.utcnow()-timedelta(days=90))

    c1, c2 = st.columns(2)
    with c1:
        s_date = st.date_input("Start Date", value=default_start.date())
        s_time = st.time_input("Start Time", value=default_start.time(), key="ts")
    with c2:
        e_date = st.date_input("End Date", value=datetime.utcnow().date())
        e_time = st.time_input("End Time", value=datetime.utcnow().time(), key="te")

    start_dt = datetime.combine(s_date, s_time)
    end_dt   = datetime.combine(e_date, e_time)

    n_days = max(1, (end_dt - start_dt).days)
    st.caption("Range: {} day(s) — downloaded one day at a time in parallel.".format(n_days))
    if auto_start:
        st.caption("🔄 Auto-start: {}".format(auto_start.strftime("%Y-%m-%d %H:%M")))

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown('<p class="section-label">🗺 Map Options</p>', unsafe_allow_html=True)
    google_api_key = st.text_input(
        "Google Maps API Key (optional)",
        type="password",
        help="If provided, the Map tab shows an embedded Google Maps view. "
             "Leave blank to use the free Folium/OpenStreetMap map.",
    )
    map_workers = st.slider("Parallel download threads", min_value=1,
                             max_value=20, value=6,
                             help="More threads = faster download but more API load.")

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown('<p class="section-label">💾 Save Options</p>', unsafe_allow_html=True)
    save_individual = st.checkbox("Save individual CSVs", value=False)
    save_merged     = st.checkbox("Save merged CSV",      value=True)

    st.markdown("<hr>", unsafe_allow_html=True)
    fetch_btn   = st.button("⚡ Download Data", use_container_width=True)
    dl_slot     = st.empty()
    status_slot = st.empty()
    status_slot.markdown(
        '<span style="font-size:12px;color:#8b949e;">● Awaiting request</span>',
        unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div class="app-header">
  <h1>🌿 QUANTAQ MULTI-SITE AIR QUALITY DASHBOARD</h1>
  <p>Multi-device · 30-day chunks · Folium interactive map · Full analytics suite</p>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  SESSION STATE
# ══════════════════════════════════════════════════════════════════════════════
for _k in ("site_data","geo_df","hourly_df","fetch_msg"):
    if _k not in st.session_state:
        st.session_state[_k] = None

# ══════════════════════════════════════════════════════════════════════════════
#  FETCH
# ══════════════════════════════════════════════════════════════════════════════
if fetch_btn:
    if not api_key:
        st.error("Enter your QuantAQ API key.")
    elif start_dt >= end_dt:
        st.error("Start must be before End.")
    elif not device_list:
        st.error("No devices configured.")
    else:
        status_slot.markdown(
            '<span style="font-size:12px;color:#00d464;">● Downloading…</span>',
            unsafe_allow_html=True)

        # Total days × devices for overall progress
        total_days   = max(1, n_days) * len(device_list)
        days_done    = [0]
        prog_bar     = st.progress(0, text="Initialising…")
        prog_text    = st.empty()

        # Progress callback — called in main thread by as_completed loop
        def progress_cb(completed, total, label):
            days_done[0] += 1
            pct = min(days_done[0] / total_days, 1.0)
            prog_bar.progress(pct, text=label)

        site_data = {}
        geo_rows  = []
        failed    = []

        for sn in device_list:
            site_name = sensor_map.get(sn, sn)

            # ── Per-day parallel download ─────────────────────────────────
            df = download_device(
                api_key, sn, start_dt, end_dt,
                progress_cb=progress_cb,
                max_workers=map_workers,      # from sidebar slider
            )

            if df is not None and not df.empty:
                df["device_sn"] = sn
                df["site_name"] = site_name
                site_data[site_name] = df

                # Resolve lat/lon (API geo field > Excel columns)
                lat, lon = np.nan, np.nan
                if "geo" in df.columns and df["geo"].notna().any():
                    lat, lon = extract_lat_lon(df["geo"].dropna().iloc[0])
                if pd.isna(lat) and sn in lat_lon_map:
                    lat, lon = lat_lon_map[sn]

                pm25_avg = float(df["pm25"].mean()) if "pm25" in df.columns else np.nan
                pm10_avg = float(df["pm10"].mean()) if "pm10" in df.columns else np.nan
                geo_rows.append({"site_name": site_name, "lat": lat, "lon": lon,
                                  "pm25": pm25_avg, "pm10": pm10_avg})

                if save_individual:
                    fn = "{}_{}_to_{}.csv".format(
                        sn,
                        start_dt.strftime("%Y-%m-%d_%H-%M-%S"),
                        end_dt.strftime("%Y-%m-%d_%H-%M-%S"))
                    df.to_csv(DATA_DIR / fn, index=False)
            else:
                failed.append(site_name)

        prog_bar.empty()
        prog_text.empty()

        # Build merged + hourly DataFrames
        if site_data:
            merged_raw = pd.concat(site_data.values(), ignore_index=True)

            # Attach AQI + hourly aggregates
            merged_raw, hourly_df = prepare_merged_for_analysis(merged_raw)

            # Refresh per-site data with AQI columns added
            for site_name in list(site_data.keys()):
                site_data[site_name] = merged_raw[
                    merged_raw["site_name"] == site_name
                ].reset_index(drop=True)

            # Recalculate geo_rows with AQI category for map
            for row in geo_rows:
                sn_df = site_data.get(row["site_name"])
                if sn_df is not None and "aqi" in sn_df.columns:
                    avg_aqi = sn_df["aqi"].mean()
                    row["aqi"]          = float(avg_aqi) if not pd.isna(avg_aqi) else np.nan
                    row["aqi_category"] = aqi_category(row["aqi"])
                else:
                    row["aqi"]          = np.nan
                    row["aqi_category"] = "Unknown"

            if save_merged:
                fn = "merged_{}_to_{}.csv".format(
                    start_dt.strftime("%Y-%m-%d_%H-%M-%S"),
                    end_dt.strftime("%Y-%m-%d_%H-%M-%S"))
                merged_raw.to_csv(DATA_DIR / fn, index=False)

            st.session_state.hourly_df = hourly_df
        else:
            st.session_state.hourly_df = None

        st.session_state.site_data = site_data or None
        st.session_state.geo_df    = pd.DataFrame(geo_rows) if geo_rows else None

        ok_n, fail_n = len(site_data), len(failed)
        msg = "✓ {} site(s) · {} raw rows".format(
            ok_n,
            "{:,}".format(sum(len(d) for d in site_data.values())) if site_data else 0,
        )
        if fail_n:
            msg += " · ⚠ {} failed: {}".format(fail_n, ", ".join(failed))
        st.session_state.fetch_msg = msg

        status_slot.markdown(
            '<span style="font-size:12px;color:{};">● {}</span>'.format(
                "#00d464" if ok_n else "#ef4444", msg),
            unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  RENDER DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
site_data = st.session_state.site_data or {}

if not site_data:
    st.markdown("""
    <div style="text-align:center;padding:80px 40px;background:#161b22;
      border:1px dashed #30363d;border-radius:12px;margin-top:20px">
      <div style="font-size:52px;margin-bottom:16px">🌿</div>
      <div style="font-family:'Space Mono',monospace;font-size:16px;
        color:#00d464;font-weight:700;margin-bottom:8px">NO DATA LOADED</div>
      <div style="color:#8b949e;font-size:13px;max-width:440px;margin:0 auto;">
        Upload <strong style="color:#e6edf3">sensor_to_site.xlsx</strong> ·
        enter your API key · pick a date range ·
        click <strong style="color:#e6edf3">Download Data</strong>
      </div>
    </div>""", unsafe_allow_html=True)
    st.stop()

sites  = list(site_data.keys())
colors = {s: SITE_COLORS[i % len(SITE_COLORS)] for i,s in enumerate(sites)}

if st.session_state.fetch_msg:
    st.success(st.session_state.fetch_msg)

# merged CSV download
merged_all = pd.concat(site_data.values(), ignore_index=True)
dl_slot.download_button(
    label="⬇ Download Merged CSV",
    data=merged_all.to_csv(index=False).encode("utf-8"),
    file_name="quantaq_merged_{}.csv".format(datetime.now().strftime("%Y%m%d_%H%M")),
    mime="text/csv",
    use_container_width=True,
)

# ── KPI strip ────────────────────────────────────────────────────────────────
kpi_cols = st.columns(min(len(sites)+2, 8))
kpi_cols[0].metric("Sites", len(sites))
kpi_cols[1].metric("Total Rows", "{:,}".format(len(merged_all)))
for i,(site,df) in enumerate(site_data.items()):
    if i+2 >= len(kpi_cols):
        break
    pm25 = df["pm25"].mean() if "pm25" in df.columns else float("nan")
    delta = "PM2.5 {:.1f}".format(pm25) if not math.isnan(pm25) else "—"
    kpi_cols[i+2].metric(site[:16], "{:,}".format(len(df)), delta=delta)

dt_min = merged_all["datetime"].min()
dt_max = merged_all["datetime"].max()
st.info("📅 {} → {}  |  {:,} rows · {} site(s)".format(
    dt_min.strftime("%Y-%m-%d %H:%M UTC") if not pd.isna(dt_min) else "?",
    dt_max.strftime("%Y-%m-%d %H:%M UTC") if not pd.isna(dt_max) else "?",
    len(merged_all), len(sites)))

st.markdown("<div style='margin-top:10px'></div>", unsafe_allow_html=True)

# Site selector
selected_site = st.selectbox("🔍 Site for detailed analysis:", options=sites)
sel_df    = site_data[selected_site]
sel_color = colors[selected_site]
avail_poll = [c for c in POLLUTANT_ALIASES
              if c in sel_df.columns and sel_df[c].notna().any()]

# ════════════════════════ TABS ════════════════════════════════════════════════
(tab_ts, tab_stats, tab_hist, tab_diurnal, tab_weekly,
 tab_monthly, tab_rolling, tab_scatter, tab_corr,
 tab_wind, tab_compare, tab_map, tab_raw) = st.tabs([
    "📈 Time Series",
    "📊 Statistics",
    "📉 Histograms",
    "🕐 Diurnal",
    "📆 Weekly",
    "📅 Monthly",
    "〰 Rolling Avg",
    "🔵 Scatter",
    "🔥 Correlation",
    "🧭 Wind Rose",
    "⚖ Site Compare",
    "🗺 Interactive Map",
    "📋 Raw Data",
])

# ── Time Series ───────────────────────────────────────────────────────────────
with tab_ts:
    st.markdown('<p class="section-label">Site: {}</p>'.format(selected_site),
                unsafe_allow_html=True)
    st.plotly_chart(chart_timeseries_site(sel_df, selected_site, sel_color),
                    use_container_width=True, key="pc_ts_site")
    if len(sites) > 1 and avail_poll:
        st.markdown("---")
        st.markdown('<p class="section-label">All Sites Overlay</p>',
                    unsafe_allow_html=True)
        p = st.selectbox("Pollutant", avail_poll, key="ts_p")
        st.plotly_chart(chart_timeseries_compare(site_data,p,colors),
                        use_container_width=True, key="pc_ts_compare")

# ── Statistics ────────────────────────────────────────────────────────────────
with tab_stats:
    st.markdown('<p class="section-label">Descriptive Statistics — {}</p>'.format(
        selected_site), unsafe_allow_html=True)
    stats_df = describe_site(sel_df)
    if not stats_df.empty:
        st.dataframe(stats_df, use_container_width=True, height=420)
        st.download_button("⬇ Export Stats CSV",
            data=stats_df.reset_index().to_csv(index=False).encode("utf-8"),
            file_name="stats_{}_{}.csv".format(
                selected_site, datetime.now().strftime("%Y%m%d")),
            mime="text/csv")
    else:
        st.info("No numeric pollutant data available.")

    with st.expander("📊 All Sites — PM2.5 & PM10 Quick Summary"):
        rows = []
        for site, df in site_data.items():
            for pol in ["pm25","pm10"]:
                if pol in df.columns and df[pol].notna().any():
                    s = df[pol].dropna()
                    rows.append({"Site":site,"Pollutant":pol.upper(),
                        "N":int(len(s)),"Mean":round(float(s.mean()),3),
                        "Std":round(float(s.std()),3),
                        "Min":round(float(s.min()),3),
                        "Max":round(float(s.max()),3),
                        "95th":round(float(np.percentile(s,95)),3)})
        if rows:
            st.dataframe(pd.DataFrame(rows).set_index("Site"),
                         use_container_width=True)

# ── Histograms ────────────────────────────────────────────────────────────────
with tab_hist:
    if avail_poll:
        h_p = st.selectbox("Pollutant", avail_poll, key="hist_p")
        st.plotly_chart(chart_histogram(sel_df,h_p,selected_site,sel_color),
                        use_container_width=True, key="pc_hist_single")
        if st.checkbox("Show all pollutants (grid)", value=False, key="hist_all"):
            for pol in avail_poll:
                st.plotly_chart(chart_histogram(sel_df,pol,selected_site,sel_color),
                                use_container_width=True, key="pc_hist_grid_{}".format(pol))
    else:
        st.info("No pollutant data.")

# ── Diurnal ───────────────────────────────────────────────────────────────────
with tab_diurnal:
    if avail_poll:
        d_p = st.selectbox("Pollutant", avail_poll, key="diu_p")
        st.plotly_chart(chart_diurnal(sel_df,d_p,selected_site,sel_color),
                        use_container_width=True, key="pc_diurnal_site")
        st.caption("Shaded band = ±1 SE of the hourly mean.")

        if len(sites) > 1:
            st.markdown("---")
            st.markdown('<p class="section-label">All Sites Diurnal</p>',
                        unsafe_allow_html=True)
            d_p2 = st.selectbox("Pollutant", avail_poll, key="diu_p2")
            fig_d = go.Figure()
            for site, df in site_data.items():
                if d_p2 not in df.columns:
                    continue
                s = df.dropna(subset=[d_p2]).copy()
                s["hour"] = s["datetime"].dt.hour
                grp = s.groupby("hour")[d_p2].mean().reset_index()
                fig_d.add_trace(go.Scatter(
                    x=grp["hour"], y=grp[d_p2], name=site,
                    mode="lines+markers",
                    line=dict(color=colors[site],width=2),
                    marker=dict(size=5)))
            fig_d.update_layout(
                title=dict(text="{} Diurnal — All Sites".format(d_p2.upper()),
                           font=dict(color="#e6edf3",size=13)),
                yaxis_title=POLLUTANT_UNITS.get(d_p2,""),
                **dk(xaxis=dict(title="Hour", tickmode="linear",
                                dtick=3, gridcolor="#30363d")))
            st.plotly_chart(fig_d, use_container_width=True, key="pc_diurnal_all")

# ── Weekly ───────────────────────────────────────────────────────────────────
with tab_weekly:
    if avail_poll:
        w_p = st.selectbox("Pollutant", avail_poll, key="week_p")
        st.plotly_chart(chart_weekly(sel_df,w_p,selected_site,sel_color),
                        use_container_width=True, key="pc_weekly")
        st.caption("Box-plot shows spread of all observations in that day of week.")
    else:
        st.info("No pollutant data.")

# ── Monthly ───────────────────────────────────────────────────────────────────
with tab_monthly:
    if avail_poll:
        m_p = st.selectbox("Pollutant", avail_poll, key="mon_p")
        st.plotly_chart(chart_monthly(sel_df,m_p,selected_site,sel_color),
                        use_container_width=True, key="pc_monthly")
        st.caption("Error bars = ±1 standard deviation.")
    else:
        st.info("No pollutant data.")

# ── Rolling Avg ───────────────────────────────────────────────────────────────
with tab_rolling:
    if avail_poll:
        r_p = st.selectbox("Pollutant", avail_poll, key="roll_p")
        st.plotly_chart(chart_rolling(sel_df,r_p,selected_site,sel_color),
                        use_container_width=True, key="pc_rolling")
        st.caption("Faint=raw · Yellow=1-h · Orange=3-h · Red=24-h rolling mean")
    else:
        st.info("No pollutant data.")

# ── Scatter ───────────────────────────────────────────────────────────────────
with tab_scatter:
    if len(avail_poll) >= 2:
        col_a, col_b = st.columns(2)
        with col_a:
            x_p = st.selectbox("X Axis", avail_poll, index=0, key="sc_x")
        with col_b:
            y_p = st.selectbox("Y Axis", avail_poll,
                                index=min(1,len(avail_poll)-1), key="sc_y")
        st.plotly_chart(
            chart_scatter_pair(sel_df,x_p,y_p,selected_site,sel_color, key="pc_scatter"),
            use_container_width=True)
    else:
        st.info("Need at least 2 pollutant columns for scatter plot.")

# ── Correlation ───────────────────────────────────────────────────────────────
with tab_corr:
    st.plotly_chart(chart_correlation(sel_df,selected_site),
                    use_container_width=True, key="pc_correlation")
    st.caption("Pearson r.  Blue=negative · Red=positive.")

# ── Wind Rose ─────────────────────────────────────────────────────────────────
with tab_wind:
    wr = chart_wind_rose(sel_df, selected_site)
    if wr:
        st.plotly_chart(wr, use_container_width=True, key="pc_windrose")
    else:
        st.info("No wind speed/direction columns detected in this dataset.\n\n"
                "MOD-PM units don't include anemometers — wind data requires "
                "a MOD or external met station.")

# ── Site Compare ──────────────────────────────────────────────────────────────
with tab_compare:
    if avail_poll:
        c_p = st.selectbox("Pollutant", avail_poll, key="cmp_p")
        col_bx, col_ts = st.columns([1,2])
        with col_bx:
            st.plotly_chart(chart_boxplot_compare(site_data,c_p,colors),
                            use_container_width=True, key="pc_compare_box")
        with col_ts:
            st.plotly_chart(chart_timeseries_compare(site_data,c_p,colors),
                            use_container_width=True, key="pc_compare_ts")
        with st.expander("📋 Hourly Average Comparison Table"):
            hf = []
            for site, df in site_data.items():
                if c_p not in df.columns:
                    continue
                h = (df.set_index("datetime")[[c_p]]
                       .resample("h").mean()
                       .rename(columns={c_p:site}))
                hf.append(h)
            if hf:
                st.dataframe(pd.concat(hf,axis=1).round(3),
                             use_container_width=True, height=340)

# ── Interactive Map ───────────────────────────────────────────────────────────
with tab_map:
    import streamlit.components.v1 as _components

    geo_df    = st.session_state.geo_df
    hourly_df = st.session_state.hourly_df

    if geo_df is not None and not geo_df.dropna(subset=["lat","lon"]).empty:

        # Build site summary table (with AQI columns from prepare_merged)
        show_geo = geo_df.copy()
        for col in ["pm25","pm10","aqi"]:
            if col in show_geo.columns:
                show_geo[col] = show_geo[col].round(2)
        if "aqi_category" not in show_geo.columns:
            show_geo["aqi_category"] = show_geo.get(
                "pm25", pd.Series([np.nan]*len(show_geo))
            ).apply(lambda v: pm25_category(v)[0])

        # ── MAP ENGINE SELECTOR ───────────────────────────────────────────
        use_google = bool(google_api_key and google_api_key.strip())
        engine_label = "Google Maps" if use_google else "Folium / OpenStreetMap"

        col_m1, col_m2 = st.columns([3,2])
        with col_m1:
            if use_google:
                st.markdown(
                    '<span style="font-size:12px;color:#00d464;">🗺 Using Google Maps</span>',
                    unsafe_allow_html=True)
                gmap_height = st.slider("Map height (px)", 400, 800, 560, 20)
            else:
                map_style = st.selectbox("Map Style",
                    ["Dark","Light","Street","Satellite"], index=0)
        with col_m2:
            show_heatmap = st.checkbox(
                "Show PM2.5 Heat Map", value=False,
                help="Adds a heat-map overlay (Folium only).")

        # ── GOOGLE MAPS ───────────────────────────────────────────────────
        if use_google:
            # Merge aqi into geo_df for the map
            map_input = show_geo.dropna(subset=["lat","lon"]).copy()
            if "aqi" not in map_input.columns:
                map_input["aqi"] = map_input.get(
                    "pm25", pd.Series([np.nan]*len(map_input))
                ).apply(pm25_to_aqi)
            if "aqi_category" not in map_input.columns:
                map_input["aqi_category"] = map_input["aqi"].apply(aqi_category)

            html_map = generate_google_map_html(
                map_input,
                google_api_key=google_api_key.strip(),
                map_height=gmap_height,
            )
            _components.html(html_map, height=gmap_height + 20, scrolling=False)

        # ── FOLIUM MAP ────────────────────────────────────────────────────
        else:
            fmap = build_folium_map(
                geo_df,
                map_style=map_style,
                show_heatmap=show_heatmap,
            )
            if fmap:
                st_folium(fmap, width="100%", height=560, returned_objects=[])

        # ── SITE SUMMARY TABLE ────────────────────────────────────────────
        st.markdown(
            '<p class="section-label" style="margin-top:14px">Site Summary</p>',
            unsafe_allow_html=True)
        st.dataframe(show_geo, use_container_width=True)

        # ── HOURLY AQI TABLE ──────────────────────────────────────────────
        if hourly_df is not None and not hourly_df.empty and "aqi" in hourly_df.columns:
            with st.expander("📋 Hourly AQI Table (all sites)"):
                disp_cols = [c for c in
                    ["site_name","hour","pm25","pm25_1hr","pm25_24hr","aqi","aqi_category"]
                    if c in hourly_df.columns]
                show_h = hourly_df[disp_cols].copy()
                for nc in ["pm25","pm25_1hr","pm25_24hr","aqi"]:
                    if nc in show_h.columns:
                        show_h[nc] = show_h[nc].round(2)
                st.dataframe(show_h, use_container_width=True, height=380)
                st.download_button(
                    "⬇ Export Hourly AQI CSV",
                    data=show_h.to_csv(index=False).encode("utf-8"),
                    file_name="hourly_aqi_{}.csv".format(
                        datetime.now().strftime("%Y%m%d_%H%M")),
                    mime="text/csv",
                )

    else:
        st.info(
            "No lat/lon data available for the map.\n\n"
            "**Option 1:** Add `lat` and `lon` columns to sensor_to_site.xlsx.\n\n"
            "**Option 2:** Ensure devices report `geo` in the API response."
        )

# ── Raw Data ──────────────────────────────────────────────────────────────────
with tab_raw:
    st.markdown('<p class="section-label">Raw Data — {}</p>'.format(selected_site),
                unsafe_allow_html=True)
    show_df = sel_df.copy()
    for c in show_df.select_dtypes(include="number").columns:
        show_df[c] = show_df[c].round(3)
    st.dataframe(show_df, use_container_width=True, height=480)
    c1, c2 = st.columns(2)
    with c1:
        st.download_button("⬇ This Site CSV",
            data=sel_df.to_csv(index=False).encode("utf-8"),
            file_name="{}_{}.csv".format(selected_site,
                datetime.now().strftime("%Y%m%d_%H%M")),
            mime="text/csv", use_container_width=True)
    with c2:
        st.download_button("⬇ All Sites Merged",
            data=merged_all.to_csv(index=False).encode("utf-8"),
            file_name="quantaq_all_{}.csv".format(
                datetime.now().strftime("%Y%m%d_%H%M")),
            mime="text/csv", use_container_width=True)
### vv
# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("<hr>", unsafe_allow_html=True)
st.markdown(
    "<div style='text-align:center;color:#30363d;font-family:Space Mono,"
    "monospace;font-size:11px;letter-spacing:1px'>"
    "QUANTAQ MULTI-SITE DASHBOARD · v4 · PYTHON {}.{}"
    "</div>".format(sys.version_info.major, sys.version_info.minor),
    unsafe_allow_html=True)
