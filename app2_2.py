# app.py
import json
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
def download_device_range(api_key, sn, start_dt, end_dt, max_workers=6):
    """
    Download daily files in parallel for a device between start_dt and end_dt (inclusive).
    Returns concatenated DataFrame or None.
    """
    dates = pd.date_range(start_dt.date(), end_dt.date(), freq="D")
    date_strs = [d.strftime("%Y-%m-%d") for d in dates]
    results = []
    # Use ThreadPoolExecutor but do not call Streamlit UI inside threads
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(download_one_day, api_key, sn, ds): ds for ds in date_strs}
        total = len(futures)
        completed = 0
        # We'll update progress in caller
        for fut in as_completed(futures):
            try:
                df = fut.result()
            except Exception:
                df = None
            if df is not None and not df.empty:
                results.append(df)
            completed += 1
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
    # center
    center_lat = float(map_df["lat"].mean())
    center_lon = float(map_df["lon"].mean())
    # Prepare markers JSON
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

    # JS: create map, add markers with colored circle symbols and info windows
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
# Main app
# -------------------------
def main():
    st.set_page_config(layout="wide", page_title="QuantAQ Analytics (Google Map)")
    st.title("QuantAQ Analytics — single app with Google Map")

    # Sidebar inputs
    st.sidebar.header("Data & Settings")
    api_key = st.sidebar.text_input("QuantAQ API Key", type="password")
    map_file = st.sidebar.file_uploader("sensor_to_site.xlsx (sn, site_name)", type=["xlsx", "xls"])
    dev_file = st.sidebar.file_uploader("Optional: device_list.xlsx (sn column)", type=["xlsx", "xls"])
    google_api_key = st.sidebar.text_input("Optional: Google Maps JS API Key (for Map page)", type="password")
    days_default = 90
    now = datetime.utcnow()
    start_date = st.sidebar.date_input("Start date", now.date() - timedelta(days=days_default))
    end_date = st.sidebar.date_input("End date", now.date())
    max_workers = st.sidebar.slider("Parallel workers", min_value=1, max_value=18, value=6)
    st.sidebar.markdown("---")
    page = st.sidebar.radio("Navigation", ["Overview", "Hourly Averages", "AQI Charts", "Statistics", "Map", "Export", "Settings"])

    # Validate inputs
    if not api_key:
        st.sidebar.info("Enter API key to enable downloads.")
    if map_file is None:
        st.sidebar.info("Upload sensor_to_site.xlsx to continue.")

    # When inputs are present, allow download
    if api_key and map_file:
        try:
            sensor_map_df = pd.read_excel(map_file)
        except Exception as e:
            st.sidebar.error("Failed to read mapping Excel.")
            st.stop()
        if not {"sn", "site_name"}.issubset(sensor_map_df.columns):
            st.sidebar.error("Mapping file must contain columns: sn, site_name")
            st.stop()
        sensor_map = dict(zip(sensor_map_df.sn.astype(str), sensor_map_df.site_name))
        if dev_file:
            try:
                dev_df = pd.read_excel(dev_file)
            except Exception:
                st.sidebar.error("Failed to read device list Excel.")
                st.stop()
            if "sn" not in dev_df.columns:
                st.sidebar.error("device_list.xlsx must contain column: sn")
                st.stop()
            device_list = dev_df["sn"].astype(str).tolist()
        else:
            device_list = sensor_map_df["sn"].astype(str).tolist()

        # Download button
        if st.sidebar.button("Download Data (range)"):
            start_dt = datetime.combine(start_date, datetime.min.time())
            end_dt = datetime.combine(end_date, datetime.max.time())
            all_frames = []
            total_devices = len(device_list)
            dev_completed = 0
            overall_progress = st.progress(0.0)
            status = st.empty()
            for sn in device_list:
                status.text(f"Starting download for {sn} ({dev_completed}/{total_devices})")
                # download_device_range will perform parallel daily downloads internally
                df = download_device_range(api_key, sn, start_dt, end_dt, max_workers=max_workers)
                dev_completed += 1
                overall_progress.progress(dev_completed / total_devices)
                if df is not None and not df.empty:
                    df["device_sn"] = sn
                    df["site_name"] = sensor_map.get(sn, sn)
                    all_frames.append(df)
            status.empty()
            overall_progress.empty()
            if not all_frames:
                st.error("No data downloaded for selected devices/dates.")
            else:
                merged = pd.concat(all_frames, ignore_index=True)
                merged, hourly = prepare_merged_for_analysis(merged)
                st.session_state["merged"] = merged
                st.session_state["hourly"] = hourly
                st.success("Download complete and data prepared.")

    # Load from session if available
    merged = st.session_state.get("merged")
    hourly = st.session_state.get("hourly")

    # Pages
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
                # For each site, take the latest hour row
                latest = hourly.sort_values(["site_name", "hour"]).groupby("site_name").tail(1)
                # ensure lat/lon available: merge with merged site coords
                coords = merged[["site_name", "lat", "lon"]].dropna().groupby("site_name", as_index=False).mean()
                map_df = latest.merge(coords, on="site_name", how="left")
                # if lat/lon still missing, drop
                map_df = map_df.dropna(subset=["lat", "lon"])
                if "aqi" not in map_df.columns:
                    map_df["aqi"] = np.nan
                if "aqi_category" not in map_df.columns:
                    map_df["aqi_category"] = "Unknown"

            if map_df.empty:
                st.info("No site coordinates available for mapping.")
            else:
                if google_api_key:
                    # Generate Google Map HTML and embed
                    html = generate_google_map_html(map_df[["site_name","lat","lon","aqi","aqi_category"]], google_api_key, map_height=600)
                    components.html(html, height=650, scrolling=True)
                else:
                    # Fallback to pydeck
                    st.info("No Google API key provided — using pydeck fallback.")
                    map_df_py = map_df.copy()
                    # color by aqi_category
                    def color_from_cat(cat):
                        return tuple(int(AQI_CATEGORY_COLORS.get(cat, "#808080").lstrip("#")[i:i+2], 16) for i in (0, 2, 4))
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
        - Suggestions for next steps: scheduled daily downloads, cloud upload, correlation heatmaps, forecasting.
        """)
    else:
        st.info("Select a page from the sidebar.")

# -------------------------
# Helper to generate Google Map HTML (reused)
# -------------------------
def generate_google_map_html(map_df, google_api_key, map_height=600):
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

if __name__ == "__main__":
    main()
