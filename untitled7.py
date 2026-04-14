# -*- coding: utf-8 -*-
"""
Created on Mon Apr 13 18:03:30 2026

@author: Sinchina
"""

# app.py
import os
import json
import math
import requests
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import pydeck as pdk
import streamlit.components.v1 as components
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------
# Configuration
# -------------------------
DATA_SAVE_ROOT = "downloaded_data"  # base folder where CSVs will be saved

# -------------------------
# Helpers
# -------------------------
def normalize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    return df

def extract_lat_lon_from_geo(geo):
    if isinstance(geo, dict):
        if "lat" in geo and "lon" in geo:
            return geo["lat"], geo["lon"]
        if "coordinates" in geo:
            coords = geo["coordinates"]
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
                return lat, lon
    if isinstance(geo, (list, tuple)) and len(geo) >= 2:
        lon, lat = geo[0], geo[1]
        return lat, lon
    return np.nan, np.nan

def pm25_to_aqi(pm):
    if pd.isna(pm):
        return np.nan
    breakpoints = [
        (0.0, 12.0, 0, 50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 350.4, 301, 400),
        (350.5, 500.4, 401, 500),
    ]
    for low, high, aqi_low, aqi_high in breakpoints:
        if low <= pm <= high:
            return ((aqi_high - aqi_low) / (high - low)) * (pm - low) + aqi_low
    return np.nan

def aqi_category(aqi):
    if pd.isna(aqi):
        return "Unknown"
    aqi = float(aqi)
    if aqi <= 50:
        return "Good"
    if aqi <= 100:
        return "Moderate"
    if aqi <= 150:
        return "Unhealthy for Sensitive Groups"
    if aqi <= 200:
        return "Unhealthy"
    if aqi <= 300:
        return "Very Unhealthy"
    return "Hazardous"

AQI_CATEGORY_COLORS = {
    "Good": "#00e400",
    "Moderate": "#ffff00",
    "Unhealthy for Sensitive Groups": "#ff7e00",
    "Unhealthy": "#ff0000",
    "Very Unhealthy": "#8f3f97",
    "Hazardous": "#7e0023",
    "Unknown": "#808080",
}

# -------------------------
# Network: download one day (data-by-date)
# -------------------------
def download_one_day(api_key, sn, date_str):
    """
    Download a single day using /data-by-date/{YYYY-MM-DD}/
    Returns DataFrame or None.
    """
    url = f"https://api.quant-aq.com/device-api/v1/devices/{sn}/data-by-date/{date_str}/"
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
                df["datetime"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
                break
        df = df.dropna(subset=["datetime"])
        if "pm2_5" in df.columns and "pm25" not in df.columns:
            df.rename(columns={"pm2_5": "pm25"}, inplace=True)
        return df
    except Exception:
        return None

# -------------------------
# Download range (parallel, safe)
# -------------------------
def download_device_range(api_key, sn, start_dt, end_dt, max_workers=6, progress_callback=None):
    """
    Download daily files in parallel for a device between start_dt and end_dt (inclusive).
    progress_callback(i, total) will be called from the main thread loop to update UI.
    Returns concatenated DataFrame or None.
    """
    dates = pd.date_range(start_dt.date(), end_dt.date(), freq="D")
    date_strs = [d.strftime("%Y-%m-%d") for d in dates]
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(download_one_day, api_key, sn, ds): ds for ds in date_strs}
        total = len(futures)
        completed = 0
        for fut in as_completed(futures):
            try:
                df = fut.result()
            except Exception:
                df = None
            if df is not None and not df.empty:
                results.append(df)
            completed += 1
            if progress_callback:
                progress_callback(completed, total)
    if not results:
        return None
    df = pd.concat(results, ignore_index=True)
    df = df.sort_values("datetime").reset_index(drop=True)
    return df

# -------------------------
# Prepare merged and hourly
# -------------------------
def prepare_merged_for_analysis(merged):
    merged = merged.copy()
    merged = merged.sort_values("datetime").reset_index(drop=True)
    # extract lat/lon
    if "geo" in merged.columns:
        latlon = merged["geo"].apply(extract_lat_lon_from_geo)
        merged["lat"] = latlon.apply(lambda x: x[0])
        merged["lon"] = latlon.apply(lambda x: x[1])
    # ensure pm25 column
    if "pm25" not in merged.columns and "pm2_5" in merged.columns:
        merged = merged.rename(columns={"pm2_5": "pm25"})
    # hourly aggregation
    merged["hour"] = merged["datetime"].dt.floor("H")
    numeric_cols = merged.select_dtypes(include=[np.number]).columns.tolist()
    if len(numeric_cols) == 0:
        hourly = pd.DataFrame()
    else:
        hourly = merged.groupby(["site_name", "hour"], as_index=False)[numeric_cols].mean().reset_index(drop=True)
        # rolling averages on hourly pm25
        if "pm25" in hourly.columns:
            hourly = hourly.sort_values(["site_name", "hour"])
            hourly["pm25_1hr"] = hourly.groupby("site_name")["pm25"].transform(lambda x: x.rolling(1, min_periods=1).mean())
            hourly["pm25_24hr"] = hourly.groupby("site_name")["pm25"].transform(lambda x: x.rolling(24, min_periods=1).mean())
            hourly["aqi"] = hourly["pm25"].apply(pm25_to_aqi)
            hourly["aqi_category"] = hourly["aqi"].apply(aqi_category)
    return merged, hourly

# -------------------------
# Google Map HTML generator
# -------------------------
def generate_google_map_html(map_df, google_api_key, map_height=600):
    """
    map_df must contain columns: site_name, lat, lon, aqi (numeric), aqi_category (string)
    Returns HTML string embedding Google Maps JS with colored markers.
    """
    center_lat = float(map_df["lat"].mean())
    center_lon = float(map_df["lon"].mean())
    markers = []
    for _, row in map_df.iterrows():
        markers.append({
            "site_name": str(row["site_name"]),
            "lat": float(row["lat"]),
            "lon": float(row["lon"]),
            "aqi": None if pd.isna(row.get("aqi")) else float(row.get("aqi")),
            "aqi_category": str(row.get("aqi_category")) if row.get("aqi_category") is not None else "Unknown"
        })
    markers_json = json.dumps(markers)
    html = f"""
    <!DOCTYPE html>
    <html>
      <head>
        <meta name="viewport" content="initial-scale=1.0, user-scalable=no" />
        <style>
          #map {{
            height: {map_height}px;
            width: 100%;
          }}
          .info-window {{
            font-family: Arial, sans-serif;
            font-size: 13px;
          }}
          .aqi-bullet {{
            display:inline-block;
            width:12px;
            height:12px;
            border-radius:6px;
            margin-right:6px;
            vertical-align:middle;
          }}
        </style>
        <script src="https://maps.googleapis.com/maps/api/js?key={google_api_key}"></script>
      </head>
      <body>
        <div id="map"></div>
        <script>
          const markers = {markers_json};
          const map = new google.maps.Map(document.getElementById('map'), {{
            zoom: 10,
            center: {{lat: {center_lat}, lng: {center_lon}}},
            mapTypeId: 'roadmap'
          }});

          function colorForCategory(cat) {{
            const map = {{
              "Good": "{AQI_CATEGORY_COLORS['Good']}",
              "Moderate": "{AQI_CATEGORY_COLORS['Moderate']}",
              "Unhealthy for Sensitive Groups": "{AQI_CATEGORY_COLORS['Unhealthy for Sensitive Groups']}",
              "Unhealthy": "{AQI_CATEGORY_COLORS['Unhealthy']}",
              "Very Unhealthy": "{AQI_CATEGORY_COLORS['Very Unhealthy']}",
              "Hazardous": "{AQI_CATEGORY_COLORS['Hazardous']}",
              "Unknown": "{AQI_CATEGORY_COLORS['Unknown']}"
            }};
            return map[cat] || "{AQI_CATEGORY_COLORS['Unknown']}";
          }}

          markers.forEach(function(m) {{
            const color = colorForCategory(m.aqi_category);
            const marker = new google.maps.Marker({{
              position: {{lat: m.lat, lng: m.lon}},
              map: map,
              title: m.site_name,
              icon: {{
                path: google.maps.SymbolPath.CIRCLE,
                scale: 10,
                fillColor: color,
                fillOpacity: 0.9,
                strokeWeight: 1,
                strokeColor: '#000000'
              }}
            }});

            const aqiText = (m.aqi === null) ? "N/A" : m.aqi.toFixed(0);
            const content = `
              <div class="info-window">
                <div><strong>${{m.site_name}}</strong></div>
                <div><span class="aqi-bullet" style="background:${{color}}"></span><strong>AQI:</strong> ${{aqiText}}</div>
                <div><strong>Category:</strong> ${{m.aqi_category}}</div>
              </div>
            `;
            const infowindow = new google.maps.InfoWindow({{content}});
            marker.addListener('click', function() {{
              infowindow.open(map, marker);
            }});
          }});
        </script>
      </body>
    </html>
    """
    return html

# -------------------------
# Utility: save CSVs
# -------------------------
def save_csvs_for_device(device_sn, df, start_dt, end_dt, save_individual, save_merged):
    """
    Save per-day CSVs into folder: DATA_SAVE_ROOT/{device_sn}/{YYYY-MM-DD}.csv
    Also optionally save merged CSV for the device into DATA_SAVE_ROOT/{device_sn}/merged_{start}_{end}.csv
    """
    base = os.path.join(DATA_SAVE_ROOT, device_sn)
    os.makedirs(base, exist_ok=True)
    # save per-day files if requested
    if save_individual:
        # group by date
        df["date_only"] = df["datetime"].dt.date
        for date, g in df.groupby("date_only"):
            fname = os.path.join(base, f"{date.isoformat()}.csv")
            g.drop(columns=["date_only"], errors="ignore").to_csv(fname, index=False)
    if save_merged:
        s = start_dt.strftime("%Y%m%d")
        e = end_dt.strftime("%Y%m%d")
        fname = os.path.join(base, f"merged_{s}_{e}.csv")
        df.to_csv(fname, index=False)

# -------------------------
# Main app
# -------------------------
def main():
    st.set_page_config(layout="wide", page_title="QuantAQ Analytics (Google Map)")
    # ---------- Sidebar: updated UI block ----------
    st.sidebar.markdown('<p class="section-label">🔑 API Key</p>', unsafe_allow_html=True)
    api_key = st.sidebar.text_input("API Key", type="password",
                             help="QuantAQ API key — used as Basic Auth username",
                             key="api_key_input")

    st.sidebar.markdown("<hr>", unsafe_allow_html=True)
    st.sidebar.markdown('<p class="section-label">📂 Sensor Mapping</p>', unsafe_allow_html=True)
    st.sidebar.caption("Upload **sensor_to_site.xlsx** — columns: `sn`, `site_name`  (optional: `lat`, `lon`)")
    map_file = st.sidebar.file_uploader("sensor_to_site.xlsx", type=["xlsx","xls"],
                                 label_visibility="collapsed", key="map_file")

    sensor_map   = {}
    device_list  = []
    lat_lon_map  = {}

    if map_file:
        try:
            sensor_df = pd.read_excel(map_file)
            sensor_df.columns = sensor_df.columns.str.strip().str.lower()
            if not {"sn","site_name"}.issubset(sensor_df.columns):
                st.sidebar.error("Excel must have: `sn`, `site_name` columns")
            else:
                sensor_map  = dict(zip(sensor_df["sn"].astype(str),
                                       sensor_df["site_name"].astype(str)))
                device_list = sensor_df["sn"].astype(str).tolist()
                if {"lat","lon"}.issubset(sensor_df.columns):
                    for _, row in sensor_df.iterrows():
                        if not pd.isna(row.get("lat")) and not pd.isna(row.get("lon")):
                            lat_lon_map[str(row["sn"])] = (float(row["lat"]),
                                                            float(row["lon"]))
                st.sidebar.success(f"{len(sensor_map)} site(s) loaded")
        except Exception as exc:
            st.sidebar.error(f"Excel error: {exc}")

    # Optional device_list override
    st.sidebar.caption("Optional: **device_list.xlsx** (column `sn`) to override devices")
    dev_file = st.sidebar.file_uploader("device_list.xlsx", type=["xlsx","xls"],
                                 label_visibility="collapsed", key="dev_file")
    if dev_file:
        try:
            dev_df = pd.read_excel(dev_file)
            dev_df.columns = dev_df.columns.str.strip().str.lower()
            device_list = dev_df["sn"].astype(str).tolist()
            st.sidebar.success(f"Override: {len(device_list)} device(s)")
        except Exception as exc:
            st.sidebar.error(f"device_list error: {exc}")

    # Manual entry fallback
    if not map_file:
        st.sidebar.markdown('<p class="section-label">📡 Manual Entry</p>', unsafe_allow_html=True)
        st.sidebar.caption("Format: SN, Site Name (one per line)")
        device_text = st.sidebar.text_area("Devices",
                                    value="SN000001, Site A\nSN000002, Site B",
                                    height=100, label_visibility="collapsed", key="device_text")
        for line in device_text.strip().splitlines():
            parts = [p.strip() for p in line.split(",",1)]
            if len(parts)==2 and parts[0]:
                sensor_map[parts[0]] = parts[1]
                device_list.append(parts[0])
            elif len(parts)==1 and parts[0]:
                sensor_map[parts[0]] = parts[0]
                device_list.append(parts[0])

    st.sidebar.markdown("<hr>", unsafe_allow_html=True)
    st.sidebar.markdown('<p class="section-label">📅 Date & Time Range</p>', unsafe_allow_html=True)
    st.sidebar.caption("Any range — auto-splits into 30-day chunks per device.")

    # auto_start_from_csvs() placeholder: try to infer earliest saved file if present
    def auto_start_from_csvs():
        try:
            if not os.path.exists(DATA_SAVE_ROOT):
                return None
            dates = []
            for dev in os.listdir(DATA_SAVE_ROOT):
                devdir = os.path.join(DATA_SAVE_ROOT, dev)
                if not os.path.isdir(devdir):
                    continue
                for fname in os.listdir(devdir):
                    if fname.endswith(".csv"):
                        try:
                            # try parse YYYYMMDD in merged filename
                            if fname.startswith("merged_") and "_" in fname:
                                parts = fname.split("_")
                                if len(parts) >= 3:
                                    d = parts[1]
                                    dt = datetime.strptime(d, "%Y%m%d")
                                    dates.append(dt)
                        except Exception:
                            continue
            if dates:
                return min(dates)
            return None
        except Exception:
            return None

    auto_start = auto_start_from_csvs()
    default_start = auto_start if auto_start else (datetime.utcnow()-timedelta(days=90))

    c1, c2 = st.sidebar.columns(2)
    with c1:
        s_date = st.sidebar.date_input("Start Date", value=default_start.date(), key="s_date")
        s_time = st.sidebar.time_input("Start Time", value=default_start.time(), key="s_time")
    with c2:
        e_date = st.sidebar.date_input("End Date", value=datetime.utcnow().date(), key="e_date")
        e_time = st.sidebar.time_input("End Time", value=datetime.utcnow().time(), key="e_time")

    start_dt = datetime.combine(s_date, s_time)
    end_dt   = datetime.combine(e_date, e_time)

    n_days   = max(1,(end_dt-start_dt).days + 1)
    n_chunks = math.ceil(n_days/30)
    st.sidebar.caption(f"Range: {n_days} days → {n_chunks} chunk(s) / device")
    if auto_start:
        st.sidebar.caption(f"🔄 Auto-start: {auto_start.strftime('%Y-%m-%d %H:%M')}")

    st.sidebar.markdown("<hr>", unsafe_allow_html=True)
    st.sidebar.markdown('<p class="section-label">💾 Save Options</p>', unsafe_allow_html=True)
    save_individual = st.sidebar.checkbox("Save individual CSVs", value=False, key="save_individual")
    save_merged     = st.sidebar.checkbox("Save merged CSV",      value=True, key="save_merged")

    st.sidebar.markdown("<hr>", unsafe_allow_html=True)
    st.sidebar.markdown('<p class="section-label">🔌 Map & UI</p>', unsafe_allow_html=True)
    google_api_key = st.sidebar.text_input("Google Maps JS API Key (optional)", type="password", key="google_api_key")
    max_workers = st.sidebar.slider("Parallel workers", min_value=1, max_value=12, value=6, key="max_workers")

    st.sidebar.markdown("<hr>", unsafe_allow_html=True)
    st.sidebar.markdown('<p class="section-label">🔭 Navigation</p>', unsafe_allow_html=True)
    page = st.sidebar.radio("Navigation", ["Overview", "Hourly Averages", "AQI Charts", "Statistics", "Map", "Export", "Settings"])

    fetch_btn = st.sidebar.button("⚡ Download Data", use_container_width=True, key="fetch_btn")
    # placeholders in main area for download progress/status
    dl_slot = st.empty()
    status_slot = st.empty()
    status_slot.markdown(
        '<span style="font-size:12px;color:#8b949e;">● Awaiting request</span>',
        unsafe_allow_html=True)

    # ---------- Header ----------
    st.markdown("""
    <div class="app-header">
      <h1>🌿 QUANTAQ MULTI-SITE AIR QUALITY DASHBOARD</h1>
      <p>Multi-device · 30-day chunks · Google Map · Full analytics suite</p>
    </div>
    """, unsafe_allow_html=True)

    # ---------- Download logic ----------
    if fetch_btn:
        if not api_key:
            st.error("API key required.")
        elif not device_list:
            st.error("No devices specified.")
        elif end_dt < start_dt:
            st.error("End must be after Start.")
        else:
            total_devices = len(device_list)
            dev_completed = 0
            overall_progress = dl_slot.progress(0.0)
            status_slot.markdown(f"<b>Starting downloads for {total_devices} device(s)</b>", unsafe_allow_html=True)
            all_frames = []
            for sn in device_list:
                status_slot.markdown(f"Downloading <b>{sn}</b> ...", unsafe_allow_html=True)
                # progress callback updates the dl_slot progress for the current device
                def progress_cb(completed, total):
                    # show per-device progress as fraction of overall progress
                    frac = (dev_completed + (completed/total)) / total_devices
                    overall_progress.progress(frac)
                df = download_device_range(api_key, sn, start_dt, end_dt, max_workers=max_workers, progress_callback=progress_cb)
                dev_completed += 1
                overall_progress.progress(dev_completed / total_devices)
                if df is not None and not df.empty:
                    df["device_sn"] = sn
                    df["site_name"] = sensor_map.get(sn, sn)
                    all_frames.append(df)
                    # save per-device files if requested
                    if save_individual or save_merged:
                        save_csvs_for_device(sn, df, start_dt, end_dt, save_individual, save_merged)
                else:
                    st.warning(f"No data for {sn} in the selected range.")
            overall_progress.empty()
            if not all_frames:
                status_slot.markdown('<span style="color:#d9534f;">No data downloaded for any device.</span>', unsafe_allow_html=True)
            else:
                merged = pd.concat(all_frames, ignore_index=True)
                merged, hourly = prepare_merged_for_analysis(merged)
                st.session_state["merged"] = merged
                st.session_state["hourly"] = hourly
                status_slot.markdown('<span style="color:#28a745;">Download complete.</span>', unsafe_allow_html=True)

    # ---------- Load session data ----------
    merged = st.session_state.get("merged")
    hourly = st.session_state.get("hourly")

    # ---------- Pages ----------
    if page == "Overview":
        st.header("Overview")
        if merged is None:
            st.info("No data loaded. Use the sidebar to download data.")
        else:
            st.subheader("Downloaded Date Range")
            st.write("Start:", merged["datetime"].min())
            st.write("End:", merged["datetime"].max())
            st.subheader("Preview (first 200 rows)")
            st.dataframe(merged.head(200))
    elif page == "Hourly Averages":
        st.header("Hourly Average Time Series (All Sites)")
        if merged is None or hourly is None or hourly.empty:
            st.info("No hourly data available. Download data first.")
        else:
            st.subheader("Hourly aggregated data (first 200 rows)")
            st.dataframe(hourly.head(200))
            metric_options = [c for c in hourly.columns if c not in ["site_name", "hour", "aqi_category"]]
            if not metric_options:
                st.info("No numeric metrics available for plotting.")
            else:
                default_metric = "pm25" if "pm25" in metric_options else metric_options[0]
                metric = st.selectbox("Metric", metric_options, index=metric_options.index(default_metric))
                fig = px.line(hourly, x="hour", y=metric, color="site_name", title=f"Hourly {metric} for all sites")
                st.plotly_chart(fig, use_container_width=True)
                csv_hourly = hourly.to_csv(index=False).encode("utf-8")
                st.download_button("Download hourly CSV", csv_hourly, file_name="hourly_averages.csv", mime="text/csv")
    elif page == "AQI Charts":
        st.header("AQI Color‑coded Charts")
        if hourly is None or hourly.empty:
            st.info("No hourly data available.")
        else:
            if "aqi_category" not in hourly.columns:
                st.warning("AQI not computed (pm25 missing).")
            else:
                st.subheader("Hourly PM2.5 colored by AQI category")
                fig = px.scatter(hourly, x="hour", y="pm25", color="aqi_category",
                                 color_discrete_map=AQI_CATEGORY_COLORS,
                                 hover_data=["site_name", "aqi"],
                                 title="Hourly PM2.5 colored by AQI category")
                st.plotly_chart(fig, use_container_width=True)
                st.subheader("Hourly PM2.5 colored by AQI continuous")
                if "aqi" in hourly.columns:
                    fig2 = px.scatter(hourly, x="hour", y="pm25", color="aqi",
                                      color_continuous_scale=["#00e400","#ffff00","#ff7e00","#ff0000","#8f3f97","#7e0023"],
                                      title="Hourly PM2.5 colored by AQI value")
                    st.plotly_chart(fig2, use_container_width=True)
    elif page == "Statistics":
        st.header("Statistics")
        if merged is None:
            st.info("No data loaded.")
        else:
            numeric_cols = merged.select_dtypes(include=[np.number]).columns.tolist()
            if not numeric_cols:
                st.info("No numeric columns to compute statistics.")
            else:
                stats = merged.groupby("site_name")[numeric_cols].agg(["count", "mean", "median", "min", "max", "std"])
                st.dataframe(stats)
    elif page == "Map":
        st.header("Site Map (Google Map if API key provided)")
        if merged is None:
            st.info("No data loaded.")
        else:
            # Build map_df with latest hourly AQI per site (current)
            if hourly is None or hourly.empty:
                st.info("Hourly data not available; map will use latest available lat/lon only.")
                map_df = merged[["site_name", "lat", "lon"]].dropna().groupby("site_name", as_index=False).mean()
                map_df["aqi"] = np.nan
                map_df["aqi_category"] = "Unknown"
            else:
                latest = hourly.sort_values(["site_name", "hour"]).groupby("site_name").tail(1)
                coords = merged[["site_name", "lat", "lon"]].dropna().groupby("site_name", as_index=False).mean()
                map_df = latest.merge(coords, on="site_name", how="left")
                map_df = map_df.dropna(subset=["lat", "lon"])
                if "aqi" not in map_df.columns:
                    map_df["aqi"] = np.nan
                if "aqi_category" not in map_df.columns:
                    map_df["aqi_category"] = "Unknown"

            if map_df.empty:
                st.info("No site coordinates available for mapping.")
            else:
                if google_api_key:
                    html = generate_google_map_html(map_df[["site_name","lat","lon","aqi","aqi_category"]], google_api_key, map_height=650)
                    components.html(html, height=700, scrolling=True)
                else:
                    st.info("No Google API key provided — using pydeck fallback.")
                    map_df_py = map_df.copy()
                    def color_from_cat(cat):
                        hexc = AQI_CATEGORY_COLORS.get(cat, "#808080").lstrip("#")
                        return [int(hexc[i:i+2], 16) for i in (0, 2, 4)]
                    map_df_py["color"] = map_df_py["aqi_category"].apply(lambda c: color_from_cat(c))
                    layer = pdk.Layer(
                        "ScatterplotLayer",
                        data=map_df_py,
                        get_position='[lon, lat]',
                        get_fill_color='color',
                        get_radius=300,
                        pickable=True
                    )
                    view = pdk.ViewState(latitude=map_df_py["lat"].mean(), longitude=map_df_py["lon"].mean(), zoom=9)
                    r = pdk.Deck(layers=[layer], initial_view_state=view, tooltip={"text":"{site_name}\nAQI: {aqi}\nCategory: {aqi_category}"})
                    st.pydeck_chart(r)
    elif page == "Export":
        st.header("Export")
        if merged is None:
            st.info("No data to export.")
        else:
            st.subheader("Download merged CSV")
            csv = merged.to_csv(index=False).encode("utf-8")
            st.download_button("Download merged CSV", csv, file_name="merged_data.csv", mime="text/csv")
            if hourly is not None and not hourly.empty:
                csv_h = hourly.to_csv(index=False).encode("utf-8")
                st.download_button("Download hourly CSV", csv_h, file_name="hourly_averages.csv", mime="text/csv")
    elif page == "Settings":
        st.header("Settings & Suggestions")
        st.markdown("""
        **Features included**
        - Daily downloads using `/data-by-date/{YYYY-MM-DD}/` (parallelized).
        - Hourly aggregation and rolling averages (1‑hr, 24‑hr) computed on hourly series.
        - AQI value and category computed from PM2.5 with color mapping.
        - Multi‑page navigation (Overview, Hourly Averages, AQI Charts, Statistics, Map, Export).
        - Google Maps integration: provide a Google Maps JavaScript API key to render colored markers by AQI.
        - CSV export for merged and hourly datasets.
        - Adjustable parallel workers for faster downloads.
        """)
    else:
        st.info("Select a page from the sidebar.")

if __name__ == "__main__":
    main()
