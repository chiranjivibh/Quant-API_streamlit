# """
# QuantAQ Multi-Site Air Quality Dashboard  ·  v4
# ────────────────────────────────────────────────
# Run:   streamlit run app.py
#        (or double-click launch.bat on Windows)

# Python 3.8+ compatible. No Mapbox token needed.
# """

# import sys
# import os
# import re
# import math
# import time as _time
# from pathlib import Path
# from datetime import datetime, timedelta
# from typing import Dict, List, Optional, Tuple

# # ── Accidental direct-run guard ───────────────────────────────────────────────
# if not os.environ.get("STREAMLIT_SCRIPT_RUN_CTX") and \
#         not any("streamlit" in a.lower() for a in sys.argv):
#     print("\n" + "=" * 60)
#     print("  QuantAQ Dashboard — Wrong launch method!")
#     print("=" * 60)
#     print("  Use:   streamlit run app.py")
#     print("  Or:    double-click launch.bat\n")
#     sys.exit(0)

# import streamlit as st
# import requests
# import pandas as pd
# import numpy as np
# import plotly.graph_objects as go
# from plotly.subplots import make_subplots
# from scipy import stats
# import folium
# from folium.plugins import MarkerCluster, HeatMap
# from streamlit_folium import st_folium

# # ══════════════════════════════════════════════════════════════════════════════
# #  PAGE CONFIG  — MUST be first Streamlit call
# # ══════════════════════════════════════════════════════════════════════════════
# st.set_page_config(
#     page_title="QuantAQ Air Quality",
#     page_icon="🌿",
#     layout="wide",
#     initial_sidebar_state="expanded",
# )

# # ══════════════════════════════════════════════════════════════════════════════
# #  THEME CSS
# # ══════════════════════════════════════════════════════════════════════════════
# st.markdown("""
# <style>
# @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');

# html,body,[class*="css"]{
#     font-family:'DM Sans',sans-serif!important;
#     background:#0d1117!important;
#     color:#e6edf3!important;
# }
# .stApp{background:#0d1117!important}

# .app-header{
#     background:linear-gradient(135deg,#0d1117 0%,#0a2310 50%,#0d1117 100%);
#     border-bottom:1px solid #30363d;
#     padding:24px 36px 20px;
#     margin:-1rem -1rem 1.5rem -1rem;
#     position:relative;overflow:hidden;
# }
# .app-header::before{
#     content:'';position:absolute;top:-40%;right:-5%;
#     width:380px;height:380px;
#     background:radial-gradient(circle,rgba(0,212,100,.15) 0%,transparent 70%);
#     pointer-events:none;
# }
# .app-header h1{
#     font-family:'Space Mono',monospace!important;
#     font-size:22px;font-weight:700;
#     color:#00d464!important;letter-spacing:-.5px;margin:0 0 4px;
# }
# .app-header p{color:#8b949e;font-size:12px;margin:0;}

# section[data-testid="stSidebar"]{
#     background:#161b22!important;
#     border-right:1px solid #30363d!important;
# }
# section[data-testid="stSidebar"] label{
#     font-family:'Space Mono',monospace!important;
#     font-size:10px!important;font-weight:700!important;
#     letter-spacing:1.5px!important;color:#00d464!important;
#     text-transform:uppercase!important;
# }
# section[data-testid="stSidebar"] input,
# section[data-testid="stSidebar"] textarea{
#     background:#0d1117!important;border:1px solid #30363d!important;
#     color:#e6edf3!important;border-radius:6px!important;
# }

# .stButton>button{
#     background:linear-gradient(135deg,#00d464,#00a84f)!important;
#     color:#0d1117!important;
#     font-family:'Space Mono',monospace!important;
#     font-weight:700!important;font-size:11px!important;
#     letter-spacing:.8px!important;border:none!important;
#     border-radius:10px!important;padding:10px 20px!important;
#     text-transform:uppercase!important;width:100%!important;
#     transition:all .2s!important;
# }
# .stButton>button:hover{
#     transform:translateY(-1px)!important;
#     box-shadow:0 6px 20px rgba(0,212,100,.35)!important;
# }
# .stDownloadButton>button{
#     background:transparent!important;color:#00d464!important;
#     border:1px solid #00d464!important;font-weight:600!important;
#     border-radius:10px!important;width:100%!important;
# }
# .stDownloadButton>button:hover{background:rgba(0,212,100,.1)!important;}

# [data-testid="metric-container"]{
#     background:#161b22!important;border:1px solid #30363d!important;
#     border-radius:10px!important;padding:14px!important;
# }
# [data-testid="metric-container"] label{
#     font-size:10px!important;text-transform:uppercase!important;
#     letter-spacing:1px!important;color:#8b949e!important;
# }
# [data-testid="stMetricValue"]{
#     font-family:'Space Mono',monospace!important;
#     font-size:20px!important;color:#00d464!important;
# }

# .stTabs [data-baseweb="tab-list"]{
#     background:#161b22!important;
#     border-bottom:1px solid #30363d!important;gap:0!important;
# }
# .stTabs [data-baseweb="tab"]{
#     font-size:13px!important;font-weight:500!important;
#     color:#8b949e!important;background:transparent!important;
#     border:none!important;border-bottom:2px solid transparent!important;
#     padding:12px 16px!important;transition:all .15s!important;
# }
# .stTabs [aria-selected="true"]{
#     color:#e6edf3!important;
#     border-bottom:2px solid #00d464!important;
#     background:transparent!important;
# }

# .stProgress>div>div>div{
#     background:linear-gradient(90deg,#00d464,#3b82f6)!important;
# }
# .stAlert{border-radius:8px!important;}
# .stDataFrame{border:1px solid #30363d!important;border-radius:10px!important;}
# hr{border-color:#30363d!important;}

# .section-label{
#     font-family:'Space Mono',monospace;font-size:10px;font-weight:700;
#     letter-spacing:1.5px;color:#00d464;text-transform:uppercase;margin-bottom:6px;
# }
# .kpi-card{
#     background:#161b22;border:1px solid #30363d;border-radius:10px;
#     padding:16px 20px;text-align:center;
# }
# .kpi-value{
#     font-family:'Space Mono',monospace;font-size:22px;font-weight:700;
#     color:#00d464;line-height:1;
# }
# .kpi-label{
#     font-size:10px;color:#8b949e;text-transform:uppercase;
#     letter-spacing:1px;margin-top:4px;
# }

# /* folium iframe dark border */
# .stfolium-container iframe{
#     border:1px solid #30363d!important;
#     border-radius:10px!important;
# }
# </style>
# """, unsafe_allow_html=True)

# # ══════════════════════════════════════════════════════════════════════════════
# #  CONSTANTS
# # ══════════════════════════════════════════════════════════════════════════════
# DATA_DIR = Path("./Delco_data")
# DATA_DIR.mkdir(exist_ok=True)

# SITE_COLORS = [
#     "#00d464","#3b82f6","#f59e0b","#ef4444","#8b5cf6",
#     "#06b6d4","#f97316","#10b981","#e879f9","#facc15",
#     "#64748b","#ec4899",
# ]

# # Plotly color sequences matching SITE_COLORS (rgba friendly)
# SITE_COLORS_PLOTLY = SITE_COLORS

# POLLUTANT_ALIASES = {
#     "pm25": ["pm25","pm2_5","pm2.5","pm25_env"],
#     "pm10": ["pm10","pm1_0","pm10_env"],
#     "temp": ["temp","temperature","temp_manifold"],
#     "rh":   ["rh","humidity","relative_humidity"],
#     "co":   ["co","co_we","co_aux"],
#     "no2":  ["no2","no2_we","no2_aux"],
#     "o3":   ["o3","ox_we"],
#     "no":   ["no","no_we"],
#     "co2":  ["co2"],
# }

# POLLUTANT_UNITS = {
#     "pm25":"µg/m³","pm10":"µg/m³","temp":"°C","rh":"%",
#     "co":"ppb","no2":"ppb","o3":"ppb","no":"ppb","co2":"ppm",
# }

# # WHO / EPA guideline thresholds (24-h avg PM2.5)
# PM25_THRESHOLDS = [
#     (0,   12,  "Good",           "#00d464"),
#     (12,  35,  "Moderate",       "#facc15"),
#     (35,  55,  "Unhealthy (S.G.)","#f97316"),
#     (55,  150, "Unhealthy",      "#ef4444"),
#     (150, 250, "Very Unhealthy", "#8b5cf6"),
#     (250, 9999,"Hazardous",      "#7f1d1d"),
# ]

# DIR_ANGLES = {
#     "N":0,"NNE":22.5,"NE":45,"ENE":67.5,
#     "E":90,"ESE":112.5,"SE":135,"SSE":157.5,
#     "S":180,"SSW":202.5,"SW":225,"WSW":247.5,
#     "W":270,"WNW":292.5,"NW":315,"NNW":337.5,
# }

# DARK = dict(
#     paper_bgcolor="#161b22",
#     plot_bgcolor="#0d1117",
#     font=dict(family="DM Sans,sans-serif", color="#8b949e"),
#     xaxis=dict(gridcolor="#30363d", zerolinecolor="#30363d"),
#     yaxis=dict(gridcolor="#30363d", zerolinecolor="#30363d"),
#     legend=dict(orientation="h", y=-0.22,
#                 bgcolor="rgba(0,0,0,0)", font=dict(color="#8b949e")),
#     margin=dict(l=55, r=20, t=50, b=90),
#     hovermode="x unified",
# )

# # ══════════════════════════════════════════════════════════════════════════════
# #  UTILITY FUNCTIONS
# # ══════════════════════════════════════════════════════════════════════════════

# def hex_rgba(hex_color, alpha=0.25):
#     """'#RRGGBB' → 'rgba(r,g,b,a)' — correct integer conversion."""
#     h = hex_color.lstrip("#")
#     r, g, b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
#     return "rgba({},{},{},{})".format(r,g,b,alpha)


# def normalize_columns(df):
#     df.columns = (df.columns.str.strip().str.lower()
#                   .str.replace(" ","_",regex=False)
#                   .str.replace("-","_",regex=False))
#     return df


# def resolve_col(df, aliases):
#     lc = {c.lower().strip(): c for c in df.columns}
#     for a in aliases:
#         if a.lower() in lc:
#             return lc[a.lower()]
#     return None


# def normalize_pollutants(df):
#     for std, aliases in POLLUTANT_ALIASES.items():
#         if std not in df.columns:
#             src = resolve_col(df, aliases)
#             if src:
#                 df[std] = pd.to_numeric(df[src], errors="coerce")
#     return df


# def extract_lat_lon(geo):
#     if isinstance(geo, dict):
#         if "lat" in geo and "lon" in geo:
#             return float(geo["lat"]), float(geo["lon"])
#         if "coordinates" in geo and len(geo["coordinates"]) >= 2:
#             return float(geo["coordinates"][1]), float(geo["coordinates"][0])
#     if isinstance(geo, (list,tuple)) and len(geo) >= 2:
#         return float(geo[1]), float(geo[0])
#     return np.nan, np.nan


# def pm25_category(val):
#     if pd.isna(val):
#         return ("N/A", "#8b949e")
#     for lo, hi, label, color in PM25_THRESHOLDS:
#         if lo <= val < hi:
#             return (label, color)
#     return ("Hazardous", "#7f1d1d")


# def auto_start_from_csvs():
#     pattern = re.compile(r"to_(\d{4}-\d{2}-\d{2})[_ ](\d{2}-\d{2}-\d{2})")
#     times = []
#     for f in DATA_DIR.glob("*.csv"):
#         m = pattern.search(f.name)
#         if m:
#             try:
#                 dt_str = "{} {}".format(m.group(1), m.group(2).replace("-",":"))
#                 times.append(datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S"))
#             except ValueError:
#                 pass
#     return (max(times) - timedelta(hours=24)) if times else None

# # ══════════════════════════════════════════════════════════════════════════════
# #  API  — chunk + paginate
# # ══════════════════════════════════════════════════════════════════════════════

# def download_chunk(api_key, sn, chunk_start, chunk_end, retries=3):
#     url  = "https://api.quant-aq.com/device-api/v1/devices/{}/data/".format(sn)
#     auth = (api_key, "")
#     frames, page, pages = [], 1, 1

#     while page <= pages:
#         params = {
#             "start":    chunk_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
#             "stop":     chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
#             "page":     page,
#             "per_page": 1000,
#         }
#         resp = None
#         for attempt in range(1, retries+1):
#             try:
#                 resp = requests.get(url, auth=auth, params=params, timeout=60)
#                 resp.raise_for_status()
#                 break
#             except requests.exceptions.HTTPError as exc:
#                 code = exc.response.status_code if exc.response else 0
#                 if code in (401, 403):
#                     return None
#                 if attempt == retries:
#                     return None
#                 _time.sleep(2**attempt)
#             except Exception:
#                 if attempt == retries:
#                     return None
#                 _time.sleep(2**attempt)

#         if resp is None:
#             return None

#         payload = resp.json()
#         try:
#             pages = payload.get("meta",{}).get("pagination",{}).get("pages",1)
#         except Exception:
#             pages = 1

#         rows = payload.get("data",[])
#         if not rows:
#             break
#         frames.append(pd.DataFrame(rows))
#         page += 1

#     if not frames:
#         return None

#     df = pd.concat(frames, ignore_index=True)
#     df = normalize_columns(df)

#     for ts_col in ["timestamp","timestamp_local","datetime"]:
#         if ts_col in df.columns:
#             df["datetime"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
#             break

#     if "datetime" not in df.columns:
#         return None

#     df = df.dropna(subset=["datetime"]).reset_index(drop=True)
#     df = normalize_pollutants(df)
#     return df


# def download_device(api_key, sn, start, end,
#                     progress_cb=None, chunk_days=30):
#     """Download full range as 30-day chunks — handles 3+ months."""
#     chunks = []
#     cur = start
#     while cur < end:
#         nxt = min(cur + timedelta(days=chunk_days), end)
#         chunks.append((cur, nxt))
#         cur = nxt

#     all_frames = []
#     for i, (cs, ce) in enumerate(chunks):
#         if progress_cb:
#             progress_cb(i, len(chunks),
#                         "{} ·chunk {}/{}: {} → {}".format(
#                             sn, i+1, len(chunks),
#                             cs.strftime("%b %d"), ce.strftime("%b %d %Y")))
#         df = download_chunk(api_key, sn, cs, ce)
#         if df is not None and not df.empty:
#             all_frames.append(df)

#     if not all_frames:
#         return None

#     df = pd.concat(all_frames, ignore_index=True)
#     df = df.sort_values("datetime").drop_duplicates(
#         subset=["datetime"]).reset_index(drop=True)
#     return df

# # ══════════════════════════════════════════════════════════════════════════════
# #  STATISTICS
# # ══════════════════════════════════════════════════════════════════════════════

# def describe_site(df):
#     cols = [c for c in POLLUTANT_ALIASES if c in df.columns]
#     rows = []
#     for col in cols:
#         s = df[col].dropna()
#         if len(s) < 2:
#             continue
#         mv = float(s.mean())
#         rows.append({
#             "Pollutant": col.upper(),
#             "Unit":      POLLUTANT_UNITS.get(col,""),
#             "N":         int(len(s)),
#             "Mean":      round(mv, 3),
#             "Median":    round(float(s.median()), 3),
#             "Std Dev":   round(float(s.std()), 3),
#             "Min":       round(float(s.min()), 3),
#             "5th %":     round(float(np.percentile(s,5)), 3),
#             "25th %":    round(float(s.quantile(0.25)), 3),
#             "75th %":    round(float(s.quantile(0.75)), 3),
#             "95th %":    round(float(np.percentile(s,95)), 3),
#             "Max":       round(float(s.max()), 3),
#             "Skewness":  round(float(stats.skew(s)), 3),
#             "Kurtosis":  round(float(stats.kurtosis(s)), 3),
#             "CV%":       round(s.std()/mv*100,2) if mv != 0 else float("nan"),
#         })
#     if not rows:
#         return pd.DataFrame()
#     return pd.DataFrame(rows).set_index("Pollutant")

# # ══════════════════════════════════════════════════════════════════════════════
# #  PLOTLY CHARTS
# # ══════════════════════════════════════════════════════════════════════════════

# def chart_timeseries_site(df, site, color):
#     avail = [c for c in list(POLLUTANT_ALIASES.keys())
#              if c in df.columns and df[c].notna().any()]
#     if not avail:
#         return go.Figure()
#     n = len(avail)
#     titles = ["{} ({})".format(c.upper(), POLLUTANT_UNITS.get(c,"")) for c in avail]
#     fig = make_subplots(rows=n, cols=1, shared_xaxes=True,
#                         subplot_titles=titles, vertical_spacing=0.04)
#     for i, col in enumerate(avail, 1):
#         s = df.dropna(subset=[col])
#         fig.add_trace(go.Scatter(
#             x=s["datetime"], y=s[col], name=col.upper(),
#             mode="lines", line=dict(color=color, width=1.3),
#             fill="tozeroy", fillcolor=hex_rgba(color,0.07),
#             showlegend=(i==1),
#             hovertemplate="%{{y:.2f}} {}<extra>{}</extra>".format(
#                 POLLUTANT_UNITS.get(col,""), col.upper()),
#         ), row=i, col=1)
#     lkw = {k:v for k,v in DARK.items() if k not in ("xaxis","yaxis")}
#     fig.update_layout(
#         title=dict(text="Time Series — {}".format(site),
#                    font=dict(color="#e6edf3",size=14)),
#         height=210*n, **lkw)
#     for i in range(1, n+1):
#         fig.update_xaxes(gridcolor="#30363d", row=i, col=1)
#         fig.update_yaxes(gridcolor="#30363d", row=i, col=1)
#     return fig


# def chart_timeseries_compare(data, pollutant, colors):
#     fig = go.Figure()
#     for site, df in data.items():
#         if df is None or pollutant not in df.columns:
#             continue
#         s = df.dropna(subset=[pollutant])
#         fig.add_trace(go.Scatter(
#             x=s["datetime"], y=s[pollutant], name=site,
#             mode="lines", line=dict(color=colors[site], width=1.6),
#             hovertemplate="%{{y:.2f}} {}<extra>{}</extra>".format(
#                 POLLUTANT_UNITS.get(pollutant,""), site),
#         ))
#     fig.update_layout(
#         title=dict(text="{} — All Sites".format(pollutant.upper()),
#                    font=dict(color="#e6edf3",size=14)),
#         yaxis_title=POLLUTANT_UNITS.get(pollutant,""), **DARK)
#     return fig


# def chart_histogram(df, col, site, color):
#     s = df[col].dropna()
#     if len(s) < 5:
#         return go.Figure()
#     counts, edges = np.histogram(s, bins=50)
#     mids = (edges[:-1]+edges[1:])/2
#     kde  = stats.gaussian_kde(s)
#     xk   = np.linspace(float(s.min()), float(s.max()), 300)
#     yk   = kde(xk)*len(s)*(edges[1]-edges[0])
#     fig  = go.Figure()
#     fig.add_trace(go.Bar(x=mids, y=counts, name="Count",
#                          marker_color=hex_rgba(color,0.6),
#                          marker_line_color=color, marker_line_width=0.5))
#     fig.add_trace(go.Scatter(x=xk, y=yk, name="KDE",
#                              mode="lines", line=dict(color=color,width=2.5)))
#     fig.add_vline(x=float(s.mean()), line_color="#fbbf24", line_dash="dash",
#                   annotation_text="Mean {:.2f}".format(float(s.mean())),
#                   annotation_font_color="#fbbf24")
#     fig.add_vline(x=float(s.median()), line_color="#e879f9", line_dash="dot",
#                   annotation_text="Median {:.2f}".format(float(s.median())),
#                   annotation_font_color="#e879f9")

#     # Add AQI thresholds for PM2.5
#     if col == "pm25":
#         for _, hi, label, tc in PM25_THRESHOLDS[:4]:
#             if hi < float(s.max()):
#                 fig.add_vline(x=hi, line_color=tc, line_dash="longdash",
#                               line_width=1, opacity=0.5,
#                               annotation_text=label,
#                               annotation_font_color=tc)

#     fig.update_layout(
#         title=dict(text="{} Distribution — {}".format(col.upper(),site),
#                    font=dict(color="#e6edf3",size=13)),
#         xaxis_title="{} ({})".format(col.upper(),POLLUTANT_UNITS.get(col,"")),
#         yaxis_title="Count", bargap=0.04, **DARK)
#     return fig


# def chart_diurnal(df, col, site, color):
#     s = df.dropna(subset=[col]).copy()
#     s["hour"] = s["datetime"].dt.hour
#     grp = s.groupby("hour")[col].agg(["mean","std","count"]).reset_index()
#     grp["se"] = grp["std"] / np.sqrt(grp["count"])
#     fig = go.Figure()
#     fig.add_trace(go.Scatter(
#         x=pd.concat([grp["hour"], grp["hour"][::-1]]),
#         y=pd.concat([grp["mean"]+grp["se"], (grp["mean"]-grp["se"])[::-1]]),
#         fill="toself", fillcolor=hex_rgba(color,0.15),
#         line=dict(color="rgba(0,0,0,0)"), showlegend=False))
#     fig.add_trace(go.Scatter(
#         x=grp["hour"], y=grp["mean"], name="Hourly mean",
#         mode="lines+markers",
#         line=dict(color=color,width=2.2), marker=dict(size=7),
#         hovertemplate="Hour %{x}:00 → %{y:.2f}<extra></extra>"))
#     fig.update_layout(
#         title=dict(text="{} Diurnal Pattern — {}".format(col.upper(),site),
#                    font=dict(color="#e6edf3",size=13)),
#         xaxis=dict(title="Hour of Day",tickmode="linear",dtick=3,gridcolor="#30363d"),
#         yaxis_title="{} ({})".format(col.upper(),POLLUTANT_UNITS.get(col,"")),
#         **DARK)
#     return fig


# def chart_weekly(df, col, site, color):
#     """Box-plot per day of week."""
#     s = df.dropna(subset=[col]).copy()
#     s["dow"] = s["datetime"].dt.day_name()
#     order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
#     fig = go.Figure()
#     for day in order:
#         vals = s[s["dow"]==day][col]
#         if vals.empty:
#             continue
#         fig.add_trace(go.Box(
#             y=vals, name=day[:3],
#             marker_color=color, line_color=color,
#             fillcolor=hex_rgba(color,0.2), boxmean="sd",
#             showlegend=False))
#     fig.update_layout(
#         title=dict(text="{} by Day of Week — {}".format(col.upper(),site),
#                    font=dict(color="#e6edf3",size=13)),
#         yaxis_title="{} ({})".format(col.upper(),POLLUTANT_UNITS.get(col,"")),
#         **DARK)
#     return fig


# def chart_monthly(df, col, site, color):
#     """Monthly mean ± std bar chart."""
#     s = df.dropna(subset=[col]).copy()
#     s["month"] = s["datetime"].dt.to_period("M")
#     grp = s.groupby("month")[col].agg(["mean","std"]).reset_index()
#     grp["month_str"] = grp["month"].astype(str)
#     fig = go.Figure()
#     fig.add_trace(go.Bar(
#         x=grp["month_str"], y=grp["mean"],
#         error_y=dict(type="data", array=grp["std"].fillna(0).tolist(), visible=True,
#                      color="#8b949e"),
#         marker_color=hex_rgba(color,0.7),
#         marker_line_color=color, marker_line_width=1,
#         name="Monthly Mean"))
#     fig.update_layout(
#         title=dict(text="{} Monthly Average — {}".format(col.upper(),site),
#                    font=dict(color="#e6edf3",size=13)),
#         xaxis_title="Month",
#         yaxis_title="{} ({})".format(col.upper(),POLLUTANT_UNITS.get(col,"")),
#         **DARK)
#     return fig


# def chart_rolling(df, col, site, color):
#     tmp = df.set_index("datetime")[col].dropna()
#     if len(tmp) < 5:
#         return go.Figure()
#     fig = go.Figure()
#     fig.add_trace(go.Scatter(x=tmp.index, y=tmp.values, name="Raw",
#                              mode="lines",
#                              line=dict(color=hex_rgba(color,0.3),width=0.6)))
#     for label, w, wc, ww in [("1-h",1,"#fbbf24",1.4),
#                                ("3-h",3,"#f97316",1.8),
#                                ("24-h",24,"#ef4444",2.2)]:
#         rolled = tmp.rolling(window=w, min_periods=max(1,w//2)).mean()
#         fig.add_trace(go.Scatter(x=rolled.index, y=rolled.values,
#                                   name=label, mode="lines",
#                                   line=dict(color=wc,width=ww)))
#     fig.update_layout(
#         title=dict(text="{} Rolling Avg — {}".format(col.upper(),site),
#                    font=dict(color="#e6edf3",size=13)),
#         yaxis_title=POLLUTANT_UNITS.get(col,""), **DARK)
#     return fig


# def chart_boxplot_compare(data, col, colors):
#     fig = go.Figure()
#     for site, df in data.items():
#         if df is None or col not in df.columns:
#             continue
#         s = df[col].dropna()
#         if s.empty:
#             continue
#         c = colors[site]
#         fig.add_trace(go.Box(
#             y=s, name=site, marker_color=c, line_color=c,
#             fillcolor=hex_rgba(c,0.2), boxmean="sd"))
#     fig.update_layout(
#         title=dict(text="{} — Site Comparison".format(col.upper()),
#                    font=dict(color="#e6edf3",size=13)),
#         yaxis_title=POLLUTANT_UNITS.get(col,""), **DARK)
#     return fig


# def chart_correlation(df, site):
#     cols = [c for c in POLLUTANT_ALIASES
#             if c in df.columns and df[c].notna().sum() > 10]
#     if len(cols) < 2:
#         return go.Figure()
#     corr = df[cols].corr().round(2)
#     labels = [c.upper() for c in corr.columns]
#     lkw = {k:v for k,v in DARK.items() if k not in ("xaxis","yaxis","hovermode")}
#     fig = go.Figure(go.Heatmap(
#         z=corr.values, x=labels, y=labels,
#         colorscale=[[0,"#3b82f6"],[0.5,"#0d1117"],[1,"#ef4444"]],
#         zmin=-1, zmax=1,
#         text=corr.values.round(2), texttemplate="%{text}",
#         textfont=dict(size=11),
#         hovertemplate="%{y} vs %{x}: %{z:.2f}<extra></extra>"))
#     fig.update_layout(
#         title=dict(text="Correlation Matrix — {}".format(site),
#                    font=dict(color="#e6edf3",size=13)),
#         xaxis=dict(gridcolor="#30363d"),
#         yaxis=dict(gridcolor="#30363d"),
#         height=460, **lkw)
#     return fig


# def chart_scatter_pair(df, col_x, col_y, site, color):
#     """Scatter + linear regression for any two pollutants."""
#     clean = df[[col_x,col_y]].dropna()
#     if len(clean) < 5:
#         return go.Figure()
#     x = clean[col_x].values
#     y = clean[col_y].values
#     coeffs = np.polyfit(x, y, 1)
#     xl = np.linspace(x.min(), x.max(), 200)
#     yl = np.polyval(coeffs, xl)
#     y_hat = np.polyval(coeffs, x)
#     ss_res = np.sum((y-y_hat)**2)
#     ss_tot = np.sum((y-y.mean())**2)
#     r2 = 1 - ss_res/ss_tot if ss_tot > 0 else float("nan")

#     fig = go.Figure()
#     fig.add_trace(go.Scatter(
#         x=x, y=y, mode="markers", name="Obs",
#         marker=dict(color=color, opacity=0.5, size=5),
#         hovertemplate="{}: %{{x:.2f}}<br>{}: %{{y:.2f}}<extra></extra>".format(
#             col_x.upper(), col_y.upper())))
#     fig.add_trace(go.Scatter(
#         x=xl, y=yl, mode="lines", name="Linear fit",
#         line=dict(color="#fbbf24", width=2, dash="dash")))
#     fig.update_layout(
#         title=dict(text="{} vs {}  |  R²={:.3f}  slope={:.3f} — {}".format(
#             col_x.upper(), col_y.upper(), r2, coeffs[0], site),
#                    font=dict(color="#e6edf3",size=13)),
#         xaxis_title="{} ({})".format(col_x.upper(),POLLUTANT_UNITS.get(col_x,"")),
#         yaxis_title="{} ({})".format(col_y.upper(),POLLUTANT_UNITS.get(col_y,"")),
#         **DARK)
#     return fig


# def chart_wind_rose(df, site):
#     wd_col = resolve_col(df, ["wd","wind_dir","wind_direction"])
#     ws_col = resolve_col(df, ["ws","wind_speed","wind_spd"])
#     if not wd_col or not ws_col:
#         return None
#     valid = df[[wd_col,ws_col]].dropna().copy()
#     if len(valid) < 10:
#         return None
#     if valid[wd_col].dtype == object:
#         valid["_angle"] = valid[wd_col].str.upper().str.strip().map(DIR_ANGLES)
#     else:
#         valid["_angle"] = pd.to_numeric(valid[wd_col], errors="coerce")
#     valid = valid.dropna(subset=["_angle"])
#     if valid.empty:
#         return None
#     dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
#             "S","SSW","SW","WSW","W","WNW","NW","NNW"]
#     speed_bins  = [0,2,5,10,15,9999]
#     speed_names = ["0-2","2-5","5-10","10-15","15+"]
#     rose_colors = ["#00d464","#3b82f6","#8b5cf6","#f59e0b","#ef4444"]
#     total = len(valid)
#     valid["_sector"] = pd.cut(valid["_angle"]%360, bins=np.linspace(0,360,17),
#                                labels=dirs, right=False)
#     valid[ws_col] = pd.to_numeric(valid[ws_col], errors="coerce")
#     valid["_sbin"] = pd.cut(valid[ws_col], bins=speed_bins,
#                              labels=speed_names, right=False)
#     fig = go.Figure()
#     for sbin, rc in zip(speed_names, rose_colors):
#         sub  = valid[valid["_sbin"]==sbin]
#         cnts = sub["_sector"].value_counts()
#         fig.add_trace(go.Barpolar(
#             r=[cnts.get(d,0)/total*100 for d in dirs],
#             theta=dirs, name="{} m/s".format(sbin),
#             marker_color=rc, opacity=0.85))
#     fig.update_layout(
#         title=dict(text="Wind Rose — {}".format(site),
#                    font=dict(color="#e6edf3",size=13)),
#         polar=dict(
#             bgcolor="#0d1117",
#             radialaxis=dict(visible=True, gridcolor="#30363d",
#                             tickfont=dict(color="#8b949e",size=9)),
#             angularaxis=dict(gridcolor="#30363d",
#                              tickfont=dict(color="#8b949e",size=10))),
#         paper_bgcolor="#161b22",
#         font=dict(color="#8b949e"),
#         legend=dict(orientation="h",y=-0.15,bgcolor="rgba(0,0,0,0)"),
#         margin=dict(l=30,r=30,t=50,b=60))
#     return fig

# # ══════════════════════════════════════════════════════════════════════════════
# #  FOLIUM MAP  (like R leaflet — no token, multiple tile layers)
# # ══════════════════════════════════════════════════════════════════════════════

# def build_folium_map(geo_df, map_style="Dark", show_heatmap=False):
#     """
#     Interactive Folium map — OpenStreetMap / CartoDB tile layers.
#     Markers coloured by PM2.5 AQI category.
#     Popup shows site stats. Optional HeatMap layer.
#     """
#     valid = geo_df.dropna(subset=["lat","lon"])
#     if valid.empty:
#         return None

#     center_lat = float(valid["lat"].mean())
#     center_lon = float(valid["lon"].mean())

#     # Choose tile layer
#     tiles_map = {
#         "Dark":    "CartoDB dark_matter",
#         "Light":   "CartoDB positron",
#         "Street":  "OpenStreetMap",
#         "Satellite": "https://server.arcgisonline.com/ArcGIS/rest/services/"
#                      "World_Imagery/MapServer/tile/{z}/{y}/{x}",
#     }
#     tile = tiles_map.get(map_style, "CartoDB dark_matter")

#     if map_style == "Satellite":
#         m = folium.Map(location=[center_lat, center_lon], zoom_start=10,
#                        tiles=tile,
#                        attr="Esri World Imagery")
#     else:
#         m = folium.Map(location=[center_lat, center_lon], zoom_start=10,
#                        tiles=tile)

#     # Layer control
#     folium.TileLayer("CartoDB positron",  name="Light").add_to(m)
#     folium.TileLayer("CartoDB dark_matter", name="Dark").add_to(m)
#     folium.TileLayer("OpenStreetMap",     name="Street").add_to(m)

#     marker_cluster = MarkerCluster(name="Sites").add_to(m)

#     for _, row in valid.iterrows():
#         pm25_val = row.get("pm25", np.nan)
#         pm10_val = row.get("pm10", np.nan)
#         label, cat_color = pm25_category(pm25_val)

#         # Custom circle marker coloured by AQI
#         folium.CircleMarker(
#             location=[row["lat"], row["lon"]],
#             radius=14,
#             color="white",
#             weight=1.5,
#             fill=True,
#             fill_color=cat_color,
#             fill_opacity=0.85,
#             tooltip="{} | PM2.5: {} µg/m³".format(
#                 row["site_name"],
#                 "{:.1f}".format(pm25_val) if not pd.isna(pm25_val) else "N/A"),
#             popup=folium.Popup("""
#                 <div style="font-family:monospace;min-width:200px;">
#                 <b style="font-size:14px">{site}</b><br>
#                 <hr style="margin:4px 0">
#                 <table style="width:100%;font-size:12px">
#                   <tr><td>PM2.5</td><td><b>{pm25}</b> µg/m³</td></tr>
#                   <tr><td>PM10</td><td><b>{pm10}</b> µg/m³</td></tr>
#                   <tr><td>AQI Cat.</td>
#                       <td><b style="color:{col}">{label}</b></td></tr>
#                   <tr><td>Lat</td><td>{lat:.4f}</td></tr>
#                   <tr><td>Lon</td><td>{lon:.4f}</td></tr>
#                 </table>
#                 </div>
#             """.format(
#                 site=row["site_name"],
#                 pm25="{:.2f}".format(pm25_val) if not pd.isna(pm25_val) else "N/A",
#                 pm10="{:.2f}".format(pm10_val) if not pd.isna(pm10_val) else "N/A",
#                 col=cat_color, label=label,
#                 lat=row["lat"], lon=row["lon"],
#             ), max_width=280),
#         ).add_to(marker_cluster)

#         # Label
#         folium.Marker(
#             location=[row["lat"], row["lon"]],
#             icon=folium.DivIcon(
#                 html='<div style="font-family:monospace;font-size:10px;'
#                      'font-weight:700;color:white;white-space:nowrap;'
#                      'text-shadow:0 0 3px #000">{}</div>'.format(row["site_name"]),
#                 icon_size=(120, 20),
#                 icon_anchor=(0, -16),
#             )
#         ).add_to(m)

#     # Optional PM2.5 heat-map layer
#     if show_heatmap:
#         heat_data = [
#             [row["lat"], row["lon"], float(row["pm25"])]
#             for _, row in valid.iterrows()
#             if not pd.isna(row.get("pm25", np.nan))
#         ]
#         if heat_data:
#             HeatMap(heat_data, name="PM2.5 Heatmap",
#                     min_opacity=0.3, radius=40, blur=30,
#                     gradient={0.2:"blue",0.4:"cyan",0.6:"lime",
#                                0.8:"yellow",1:"red"}).add_to(m)

#     # AQI legend
#     legend_html = """
#     <div style="position:fixed;bottom:30px;left:30px;z-index:9999;
#          background:rgba(13,17,23,0.92);border:1px solid #30363d;
#          border-radius:8px;padding:12px 16px;font-family:monospace;
#          font-size:11px;color:#e6edf3;min-width:160px;">
#       <b style="color:#00d464;letter-spacing:1px">PM2.5 AQI</b><br><br>
#       {}
#     </div>""".format("".join(
#         '<div style="margin-bottom:3px">'
#         '<span style="display:inline-block;width:12px;height:12px;'
#         'border-radius:50%;background:{};margin-right:6px;'
#         'vertical-align:middle"></span>{} ({}-{})</div>'.format(
#             tc, label, lo, hi if hi < 9999 else "+")
#         for lo, hi, label, tc in PM25_THRESHOLDS
#     ))
#     m.get_root().html.add_child(folium.Element(legend_html))

#     folium.LayerControl().add_to(m)
#     return m

# # ══════════════════════════════════════════════════════════════════════════════
# #  SIDEBAR
# # ══════════════════════════════════════════════════════════════════════════════
# with st.sidebar:
#     st.markdown('<p class="section-label">🔑 API Key</p>', unsafe_allow_html=True)
#     api_key = st.text_input("API Key", type="password",
#                              help="QuantAQ API key — used as Basic Auth username")

#     st.markdown("<hr>", unsafe_allow_html=True)
#     st.markdown('<p class="section-label">📂 Sensor Mapping</p>', unsafe_allow_html=True)
#     st.caption("Upload **sensor_to_site.xlsx** — columns: `sn`, `site_name`  "
#                "(optional: `lat`, `lon`)")
#     map_file = st.file_uploader("sensor_to_site.xlsx", type=["xlsx","xls"],
#                                  label_visibility="collapsed")

#     sensor_map   = {}
#     device_list  = []
#     lat_lon_map  = {}

#     if map_file:
#         try:
#             sensor_df = pd.read_excel(map_file)
#             sensor_df.columns = sensor_df.columns.str.strip().str.lower()
#             if not {"sn","site_name"}.issubset(sensor_df.columns):
#                 st.error("Excel must have: `sn`, `site_name` columns")
#             else:
#                 sensor_map  = dict(zip(sensor_df["sn"].astype(str),
#                                        sensor_df["site_name"].astype(str)))
#                 device_list = sensor_df["sn"].astype(str).tolist()
#                 if {"lat","lon"}.issubset(sensor_df.columns):
#                     for _, row in sensor_df.iterrows():
#                         if not pd.isna(row.get("lat")) and not pd.isna(row.get("lon")):
#                             lat_lon_map[str(row["sn"])] = (float(row["lat"]),
#                                                             float(row["lon"]))
#                 st.success("{} site(s) loaded".format(len(sensor_map)))
#         except Exception as exc:
#             st.error("Excel error: {}".format(exc))

#     # Optional device_list override
#     st.caption("Optional: **device_list.xlsx** (column `sn`) to override devices")
#     dev_file = st.file_uploader("device_list.xlsx", type=["xlsx","xls"],
#                                  label_visibility="collapsed")
#     if dev_file:
#         try:
#             dev_df = pd.read_excel(dev_file)
#             dev_df.columns = dev_df.columns.str.strip().str.lower()
#             device_list = dev_df["sn"].astype(str).tolist()
#             st.success("Override: {} device(s)".format(len(device_list)))
#         except Exception as exc:
#             st.error("device_list error: {}".format(exc))

#     # Manual entry fallback
#     if not map_file:
#         st.markdown('<p class="section-label">📡 Manual Entry</p>', unsafe_allow_html=True)
#         st.caption("Format: SN, Site Name (one per line)")
#         device_text = st.text_area("Devices",
#                                     value="SN000001, Site A\nSN000002, Site B",
#                                     height=100, label_visibility="collapsed")
#         for line in device_text.strip().splitlines():
#             parts = [p.strip() for p in line.split(",",1)]
#             if len(parts)==2 and parts[0]:
#                 sensor_map[parts[0]] = parts[1]
#                 device_list.append(parts[0])
#             elif len(parts)==1 and parts[0]:
#                 sensor_map[parts[0]] = parts[0]
#                 device_list.append(parts[0])

#     st.markdown("<hr>", unsafe_allow_html=True)
#     st.markdown('<p class="section-label">📅 Date & Time Range</p>', unsafe_allow_html=True)
#     st.caption("Any range — auto-splits into 30-day chunks per device.")

#     auto_start = auto_start_from_csvs()
#     default_start = auto_start if auto_start else (datetime.utcnow()-timedelta(days=90))

#     c1, c2 = st.columns(2)
#     with c1:
#         s_date = st.date_input("Start Date", value=default_start.date())
#         s_time = st.time_input("Start Time", value=default_start.time(), key="ts")
#     with c2:
#         e_date = st.date_input("End Date", value=datetime.utcnow().date())
#         e_time = st.time_input("End Time", value=datetime.utcnow().time(), key="te")

#     start_dt = datetime.combine(s_date, s_time)
#     end_dt   = datetime.combine(e_date, e_time)

#     n_days   = max(1,(end_dt-start_dt).days)
#     n_chunks = math.ceil(n_days/30)
#     st.caption("Range: {} days → {} chunk(s) / device".format(n_days, n_chunks))
#     if auto_start:
#         st.caption("🔄 Auto-start: {}".format(auto_start.strftime("%Y-%m-%d %H:%M")))

#     st.markdown("<hr>", unsafe_allow_html=True)
#     st.markdown('<p class="section-label">💾 Save Options</p>', unsafe_allow_html=True)
#     save_individual = st.checkbox("Save individual CSVs", value=False)
#     save_merged     = st.checkbox("Save merged CSV",      value=True)

#     st.markdown("<hr>", unsafe_allow_html=True)
#     fetch_btn   = st.button("⚡ Download Data", use_container_width=True)
#     dl_slot     = st.empty()
#     status_slot = st.empty()
#     status_slot.markdown(
#         '<span style="font-size:12px;color:#8b949e;">● Awaiting request</span>',
#         unsafe_allow_html=True)

# # ══════════════════════════════════════════════════════════════════════════════
# #  HEADER
# # ══════════════════════════════════════════════════════════════════════════════
# st.markdown("""
# <div class="app-header">
#   <h1>🌿 QUANTAQ MULTI-SITE AIR QUALITY DASHBOARD</h1>
#   <p>Multi-device · 30-day chunks · Folium interactive map · Full analytics suite</p>
# </div>
# """, unsafe_allow_html=True)

# # ══════════════════════════════════════════════════════════════════════════════
# #  SESSION STATE
# # ══════════════════════════════════════════════════════════════════════════════
# for _k in ("site_data","geo_df","fetch_msg"):
#     if _k not in st.session_state:
#         st.session_state[_k] = None

# # ══════════════════════════════════════════════════════════════════════════════
# #  FETCH
# # ══════════════════════════════════════════════════════════════════════════════
# if fetch_btn:
#     if not api_key:
#         st.error("Enter your QuantAQ API key.")
#     elif start_dt >= end_dt:
#         st.error("Start must be before End.")
#     elif not device_list:
#         st.error("No devices configured.")
#     else:
#         status_slot.markdown(
#             '<span style="font-size:12px;color:#00d464;">● Downloading…</span>',
#             unsafe_allow_html=True)

#         total_chunks = max(1, n_chunks) * len(device_list)
#         chunk_counter = [0]
#         prog_bar  = st.progress(0, text="Initialising…")
#         prog_text = st.empty()

#         def progress_cb(chunk_idx, total, label):
#             chunk_counter[0] += 1
#             pct = min(chunk_counter[0]/total_chunks, 1.0)
#             prog_bar.progress(pct, text=label)

#         site_data = {}
#         geo_rows  = []
#         failed    = []

#         for sn in device_list:
#             site_name = sensor_map.get(sn, sn)
#             df = download_device(api_key, sn, start_dt, end_dt,
#                                   progress_cb=progress_cb, chunk_days=30)

#             if df is not None and not df.empty:
#                 df["device_sn"]  = sn
#                 df["site_name"]  = site_name
#                 site_data[site_name] = df

#                 # Geo
#                 lat, lon = np.nan, np.nan
#                 if "geo" in df.columns and df["geo"].notna().any():
#                     lat, lon = extract_lat_lon(df["geo"].dropna().iloc[0])
#                 if pd.isna(lat) and sn in lat_lon_map:
#                     lat, lon = lat_lon_map[sn]

#                 pm25_avg = float(df["pm25"].mean()) if "pm25" in df.columns else np.nan
#                 pm10_avg = float(df["pm10"].mean()) if "pm10" in df.columns else np.nan
#                 geo_rows.append({"site_name":site_name,"lat":lat,"lon":lon,
#                                   "pm25":pm25_avg,"pm10":pm10_avg})

#                 if save_individual:
#                     fn = "{}_{}_to_{}.csv".format(
#                         sn, start_dt.strftime("%Y-%m-%d_%H-%M-%S"),
#                         end_dt.strftime("%Y-%m-%d_%H-%M-%S"))
#                     df.to_csv(DATA_DIR/fn, index=False)
#             else:
#                 failed.append(site_name)

#         prog_bar.empty()
#         prog_text.empty()

#         if site_data and save_merged:
#             merged_df = pd.concat(site_data.values(), ignore_index=True)
#             fn = "merged_{}_to_{}.csv".format(
#                 start_dt.strftime("%Y-%m-%d_%H-%M-%S"),
#                 end_dt.strftime("%Y-%m-%d_%H-%M-%S"))
#             merged_df.to_csv(DATA_DIR/fn, index=False)

#         st.session_state.site_data = site_data or None
#         st.session_state.geo_df    = pd.DataFrame(geo_rows) if geo_rows else None

#         ok_n, fail_n = len(site_data), len(failed)
#         msg = "✓ {} site(s) loaded".format(ok_n)
#         if fail_n:
#             msg += " · ⚠ {} failed: {}".format(fail_n, ", ".join(failed))
#         st.session_state.fetch_msg = msg

#         status_slot.markdown(
#             '<span style="font-size:12px;color:{};">● {}</span>'.format(
#                 "#00d464" if ok_n else "#ef4444", msg),
#             unsafe_allow_html=True)

# # ══════════════════════════════════════════════════════════════════════════════
# #  RENDER DASHBOARD
# # ══════════════════════════════════════════════════════════════════════════════
# site_data = st.session_state.site_data or {}

# if not site_data:
#     st.markdown("""
#     <div style="text-align:center;padding:80px 40px;background:#161b22;
#       border:1px dashed #30363d;border-radius:12px;margin-top:20px">
#       <div style="font-size:52px;margin-bottom:16px">🌿</div>
#       <div style="font-family:'Space Mono',monospace;font-size:16px;
#         color:#00d464;font-weight:700;margin-bottom:8px">NO DATA LOADED</div>
#       <div style="color:#8b949e;font-size:13px;max-width:440px;margin:0 auto;">
#         Upload <strong style="color:#e6edf3">sensor_to_site.xlsx</strong> ·
#         enter your API key · pick a date range ·
#         click <strong style="color:#e6edf3">Download Data</strong>
#       </div>
#     </div>""", unsafe_allow_html=True)
#     st.stop()

# sites  = list(site_data.keys())
# colors = {s: SITE_COLORS[i % len(SITE_COLORS)] for i,s in enumerate(sites)}

# if st.session_state.fetch_msg:
#     st.success(st.session_state.fetch_msg)

# # merged CSV download
# merged_all = pd.concat(site_data.values(), ignore_index=True)
# dl_slot.download_button(
#     label="⬇ Download Merged CSV",
#     data=merged_all.to_csv(index=False).encode("utf-8"),
#     file_name="quantaq_merged_{}.csv".format(datetime.now().strftime("%Y%m%d_%H%M")),
#     mime="text/csv",
#     use_container_width=True,
# )

# # ── KPI strip ────────────────────────────────────────────────────────────────
# kpi_cols = st.columns(min(len(sites)+2, 8))
# kpi_cols[0].metric("Sites", len(sites))
# kpi_cols[1].metric("Total Rows", "{:,}".format(len(merged_all)))
# for i,(site,df) in enumerate(site_data.items()):
#     if i+2 >= len(kpi_cols):
#         break
#     pm25 = df["pm25"].mean() if "pm25" in df.columns else float("nan")
#     delta = "PM2.5 {:.1f}".format(pm25) if not math.isnan(pm25) else "—"
#     kpi_cols[i+2].metric(site[:16], "{:,}".format(len(df)), delta=delta)

# dt_min = merged_all["datetime"].min()
# dt_max = merged_all["datetime"].max()
# st.info("📅 {} → {}  |  {:,} rows · {} site(s)".format(
#     dt_min.strftime("%Y-%m-%d %H:%M UTC") if not pd.isna(dt_min) else "?",
#     dt_max.strftime("%Y-%m-%d %H:%M UTC") if not pd.isna(dt_max) else "?",
#     len(merged_all), len(sites)))

# st.markdown("<div style='margin-top:10px'></div>", unsafe_allow_html=True)

# # Site selector
# selected_site = st.selectbox("🔍 Site for detailed analysis:", options=sites)
# sel_df    = site_data[selected_site]
# sel_color = colors[selected_site]
# avail_poll = [c for c in POLLUTANT_ALIASES
#               if c in sel_df.columns and sel_df[c].notna().any()]

# # ════════════════════════ TABS ════════════════════════════════════════════════
# (tab_ts, tab_stats, tab_hist, tab_diurnal, tab_weekly,
#  tab_monthly, tab_rolling, tab_scatter, tab_corr,
#  tab_wind, tab_compare, tab_map, tab_raw) = st.tabs([
#     "📈 Time Series",
#     "📊 Statistics",
#     "📉 Histograms",
#     "🕐 Diurnal",
#     "📆 Weekly",
#     "📅 Monthly",
#     "〰 Rolling Avg",
#     "🔵 Scatter",
#     "🔥 Correlation",
#     "🧭 Wind Rose",
#     "⚖ Site Compare",
#     "🗺 Interactive Map",
#     "📋 Raw Data",
# ])

# # ── Time Series ───────────────────────────────────────────────────────────────
# with tab_ts:
#     st.markdown('<p class="section-label">Site: {}</p>'.format(selected_site),
#                 unsafe_allow_html=True)
#     st.plotly_chart(chart_timeseries_site(sel_df, selected_site, sel_color),
#                     use_container_width=True)
#     if len(sites) > 1 and avail_poll:
#         st.markdown("---")
#         st.markdown('<p class="section-label">All Sites Overlay</p>',
#                     unsafe_allow_html=True)
#         p = st.selectbox("Pollutant", avail_poll, key="ts_p")
#         st.plotly_chart(chart_timeseries_compare(site_data,p,colors),
#                         use_container_width=True)

# # ── Statistics ────────────────────────────────────────────────────────────────
# with tab_stats:
#     st.markdown('<p class="section-label">Descriptive Statistics — {}</p>'.format(
#         selected_site), unsafe_allow_html=True)
#     stats_df = describe_site(sel_df)
#     if not stats_df.empty:
#         st.dataframe(stats_df, use_container_width=True, height=420)
#         st.download_button("⬇ Export Stats CSV",
#             data=stats_df.reset_index().to_csv(index=False).encode("utf-8"),
#             file_name="stats_{}_{}.csv".format(
#                 selected_site, datetime.now().strftime("%Y%m%d")),
#             mime="text/csv")
#     else:
#         st.info("No numeric pollutant data available.")

#     with st.expander("📊 All Sites — PM2.5 & PM10 Quick Summary"):
#         rows = []
#         for site, df in site_data.items():
#             for pol in ["pm25","pm10"]:
#                 if pol in df.columns and df[pol].notna().any():
#                     s = df[pol].dropna()
#                     rows.append({"Site":site,"Pollutant":pol.upper(),
#                         "N":int(len(s)),"Mean":round(float(s.mean()),3),
#                         "Std":round(float(s.std()),3),
#                         "Min":round(float(s.min()),3),
#                         "Max":round(float(s.max()),3),
#                         "95th":round(float(np.percentile(s,95)),3)})
#         if rows:
#             st.dataframe(pd.DataFrame(rows).set_index("Site"),
#                          use_container_width=True)

# # ── Histograms ────────────────────────────────────────────────────────────────
# with tab_hist:
#     if avail_poll:
#         h_p = st.selectbox("Pollutant", avail_poll, key="hist_p")
#         st.plotly_chart(chart_histogram(sel_df,h_p,selected_site,sel_color),
#                         use_container_width=True)
#         if st.checkbox("Show all pollutants (grid)", value=False, key="hist_all"):
#             for pol in avail_poll:
#                 st.plotly_chart(chart_histogram(sel_df,pol,selected_site,sel_color),
#                                 use_container_width=True)
#     else:
#         st.info("No pollutant data.")

# # ── Diurnal ───────────────────────────────────────────────────────────────────
# with tab_diurnal:
#     if avail_poll:
#         d_p = st.selectbox("Pollutant", avail_poll, key="diu_p")
#         st.plotly_chart(chart_diurnal(sel_df,d_p,selected_site,sel_color),
#                         use_container_width=True)
#         st.caption("Shaded band = ±1 SE of the hourly mean.")

#         if len(sites) > 1:
#             st.markdown("---")
#             st.markdown('<p class="section-label">All Sites Diurnal</p>',
#                         unsafe_allow_html=True)
#             d_p2 = st.selectbox("Pollutant", avail_poll, key="diu_p2")
#             fig_d = go.Figure()
#             for site, df in site_data.items():
#                 if d_p2 not in df.columns:
#                     continue
#                 s = df.dropna(subset=[d_p2]).copy()
#                 s["hour"] = s["datetime"].dt.hour
#                 grp = s.groupby("hour")[d_p2].mean().reset_index()
#                 fig_d.add_trace(go.Scatter(
#                     x=grp["hour"], y=grp[d_p2], name=site,
#                     mode="lines+markers",
#                     line=dict(color=colors[site],width=2),
#                     marker=dict(size=5)))
#             fig_d.update_layout(
#                 title=dict(text="{} Diurnal — All Sites".format(d_p2.upper()),
#                            font=dict(color="#e6edf3",size=13)),
#                 xaxis=dict(title="Hour",tickmode="linear",dtick=3,
#                            gridcolor="#30363d"),
#                 yaxis_title=POLLUTANT_UNITS.get(d_p2,""), **DARK)
#             st.plotly_chart(fig_d, use_container_width=True)

# # ── Weekly ───────────────────────────────────────────────────────────────────
# with tab_weekly:
#     if avail_poll:
#         w_p = st.selectbox("Pollutant", avail_poll, key="week_p")
#         st.plotly_chart(chart_weekly(sel_df,w_p,selected_site,sel_color),
#                         use_container_width=True)
#         st.caption("Box-plot shows spread of all observations in that day of week.")
#     else:
#         st.info("No pollutant data.")

# # ── Monthly ───────────────────────────────────────────────────────────────────
# with tab_monthly:
#     if avail_poll:
#         m_p = st.selectbox("Pollutant", avail_poll, key="mon_p")
#         st.plotly_chart(chart_monthly(sel_df,m_p,selected_site,sel_color),
#                         use_container_width=True)
#         st.caption("Error bars = ±1 standard deviation.")
#     else:
#         st.info("No pollutant data.")

# # ── Rolling Avg ───────────────────────────────────────────────────────────────
# with tab_rolling:
#     if avail_poll:
#         r_p = st.selectbox("Pollutant", avail_poll, key="roll_p")
#         st.plotly_chart(chart_rolling(sel_df,r_p,selected_site,sel_color),
#                         use_container_width=True)
#         st.caption("Faint=raw · Yellow=1-h · Orange=3-h · Red=24-h rolling mean")
#     else:
#         st.info("No pollutant data.")

# # ── Scatter ───────────────────────────────────────────────────────────────────
# with tab_scatter:
#     if len(avail_poll) >= 2:
#         col_a, col_b = st.columns(2)
#         with col_a:
#             x_p = st.selectbox("X Axis", avail_poll, index=0, key="sc_x")
#         with col_b:
#             y_p = st.selectbox("Y Axis", avail_poll,
#                                 index=min(1,len(avail_poll)-1), key="sc_y")
#         st.plotly_chart(
#             chart_scatter_pair(sel_df,x_p,y_p,selected_site,sel_color),
#             use_container_width=True)
#     else:
#         st.info("Need at least 2 pollutant columns for scatter plot.")

# # ── Correlation ───────────────────────────────────────────────────────────────
# with tab_corr:
#     st.plotly_chart(chart_correlation(sel_df,selected_site),
#                     use_container_width=True)
#     st.caption("Pearson r.  Blue=negative · Red=positive.")

# # ── Wind Rose ─────────────────────────────────────────────────────────────────
# with tab_wind:
#     wr = chart_wind_rose(sel_df, selected_site)
#     if wr:
#         st.plotly_chart(wr, use_container_width=True)
#     else:
#         st.info("No wind speed/direction columns detected in this dataset.\n\n"
#                 "MOD-PM units don't include anemometers — wind data requires "
#                 "a MOD or external met station.")

# # ── Site Compare ──────────────────────────────────────────────────────────────
# with tab_compare:
#     if avail_poll:
#         c_p = st.selectbox("Pollutant", avail_poll, key="cmp_p")
#         col_bx, col_ts = st.columns([1,2])
#         with col_bx:
#             st.plotly_chart(chart_boxplot_compare(site_data,c_p,colors),
#                             use_container_width=True)
#         with col_ts:
#             st.plotly_chart(chart_timeseries_compare(site_data,c_p,colors),
#                             use_container_width=True)
#         with st.expander("📋 Hourly Average Comparison Table"):
#             hf = []
#             for site, df in site_data.items():
#                 if c_p not in df.columns:
#                     continue
#                 h = (df.set_index("datetime")[[c_p]]
#                        .resample("h").mean()
#                        .rename(columns={c_p:site}))
#                 hf.append(h)
#             if hf:
#                 st.dataframe(pd.concat(hf,axis=1).round(3),
#                              use_container_width=True, height=340)

# # ── Interactive Map ───────────────────────────────────────────────────────────
# with tab_map:
#     geo_df = st.session_state.geo_df
#     if geo_df is not None and not geo_df.dropna(subset=["lat","lon"]).empty:

#         col_m1, col_m2, col_m3 = st.columns([2,2,1])
#         with col_m1:
#             map_style = st.selectbox("Map Style",
#                 ["Dark","Light","Street","Satellite"], index=0)
#         with col_m2:
#             show_heatmap = st.checkbox("Show PM2.5 Heat Map", value=False)
#         with col_m3:
#             st.markdown("<br>", unsafe_allow_html=True)

#         fmap = build_folium_map(geo_df, map_style=map_style,
#                                  show_heatmap=show_heatmap)
#         if fmap:
#             st_folium(fmap, width="100%", height=560,
#                       returned_objects=[])

#         # Site table below map
#         st.markdown('<p class="section-label" style="margin-top:12px">Site Summary</p>',
#                     unsafe_allow_html=True)
#         show_geo = geo_df.copy()
#         for col in ["pm25","pm10"]:
#             if col in show_geo.columns:
#                 show_geo[col] = show_geo[col].round(2)
#         show_geo["AQI Category"] = show_geo.get(
#             "pm25", pd.Series([np.nan]*len(show_geo))
#         ).apply(lambda v: pm25_category(v)[0])
#         st.dataframe(show_geo, use_container_width=True)
#     else:
#         st.info(
#             "No lat/lon data available for the map.\n\n"
#             "**Option 1:** Add `lat` and `lon` columns to your sensor_to_site.xlsx.\n\n"
#             "**Option 2:** Ensure your devices report `geo` in the API response."
#         )

# # ── Raw Data ──────────────────────────────────────────────────────────────────
# with tab_raw:
#     st.markdown('<p class="section-label">Raw Data — {}</p>'.format(selected_site),
#                 unsafe_allow_html=True)
#     show_df = sel_df.copy()
#     for c in show_df.select_dtypes(include="number").columns:
#         show_df[c] = show_df[c].round(3)
#     st.dataframe(show_df, use_container_width=True, height=480)
#     c1, c2 = st.columns(2)
#     with c1:
#         st.download_button("⬇ This Site CSV",
#             data=sel_df.to_csv(index=False).encode("utf-8"),
#             file_name="{}_{}.csv".format(selected_site,
#                 datetime.now().strftime("%Y%m%d_%H%M")),
#             mime="text/csv", use_container_width=True)
#     with c2:
#         st.download_button("⬇ All Sites Merged",
#             data=merged_all.to_csv(index=False).encode("utf-8"),
#             file_name="quantaq_all_{}.csv".format(
#                 datetime.now().strftime("%Y%m%d_%H%M")),
#             mime="text/csv", use_container_width=True)

# # ── Footer ────────────────────────────────────────────────────────────────────
# st.markdown("<hr>", unsafe_allow_html=True)
# st.markdown(
#     "<div style='text-align:center;color:#30363d;font-family:Space Mono,"
#     "monospace;font-size:11px;letter-spacing:1px'>"
#     "QUANTAQ MULTI-SITE DASHBOARD · v4 · PYTHON {}.{}"
#     "</div>".format(sys.version_info.major, sys.version_info.minor),
#     unsafe_allow_html=True)
# app.py
"""
QuantAQ Multi-Site Air Quality Dashboard  ·  v5
Run: streamlit run app.py

This file is the single-file Streamlit app that:
- Provides a polished sidebar UI for API key, sensor mapping, manual entry, date range, save options
- Downloads QuantAQ data using the daily endpoint (/data-by-date/{YYYY-MM-DD}/) in parallel
- Aggregates hourly averages, computes rolling averages and AQI
- Multi-page navigation: Overview, Hourly Averages, AQI Charts, Statistics, Map (Folium), Export, Settings
- CSV export and optional per-device saving
- Folium map with clustered markers colored by AQI category
"""

import os
import re
import math
import json
import time as _time
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from scipy import stats
import folium
from folium.plugins import MarkerCluster, HeatMap
from streamlit_folium import st_folium

# -------------------------
# Page config & CSS (from pasted text)
# -------------------------
st.set_page_config(
    page_title="QuantAQ Air Quality",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');

html,body,[class*="css"]{
    font-family:'DM Sans',sans-serif!important;
    background:#0d1117!important;
    color:#e6edf3!important;
}
.stApp{background:#0d1117!important}

.app-header{
    background:linear-gradient(135deg,#0d1117 0%,#0a2310 50%,#0d1117 100%);
    border-bottom:1px solid #30363d;
    padding:24px 36px 20px;
    margin:-1rem -1rem 1.5rem -1rem;
    position:relative;overflow:hidden;
}
.app-header::before{
    content:'';position:absolute;top:-40%;right:-5%;
    width:380px;height:380px;
    background:radial-gradient(circle,rgba(0,212,100,.15) 0%,transparent 70%);
    pointer-events:none;
}
.app-header h1{
    font-family:'Space Mono',monospace!important;
    font-size:22px;font-weight:700;
    color:#00d464!important;letter-spacing:-.5px;margin:0 0 4px;
}
.app-header p{color:#8b949e;font-size:12px;margin:0;}

section[data-testid="stSidebar"]{
    background:#161b22!important;
    border-right:1px solid #30363d!important;
}
section[data-testid="stSidebar"] label{
    font-family:'Space Mono',monospace!important;
    font-size:10px!important;font-weight:700!important;
    letter-spacing:1.5px!important;color:#00d464!important;
    text-transform:uppercase!important;
}
section[data-testid="stSidebar"] input,
section[data-testid="stSidebar"] textarea{
    background:#0d1117!important;border:1px solid #30363d!important;
    color:#e6edf3!important;border-radius:6px!important;
}

.stButton>button{
    background:linear-gradient(135deg,#00d464,#00a84f)!important;
    color:#0d1117!important;
    font-family:'Space Mono',monospace!important;
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

[data-testid="metric-container"]{
    background:#161b22!important;border:1px solid #30363d!important;
    border-radius:10px!important;padding:14px!important;
}
[data-testid="metric-container"] label{
    font-size:10px!important;text-transform:uppercase!important;
    letter-spacing:1px!important;color:#8b949e!important;
}
[data-testid="stMetricValue"]{
    font-family:'Space Mono',monospace!important;
    font-size:20px!important;color:#00d464!important;
}

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
    color:#e6edf3!important;
    border-bottom:2px solid #00d464!important;
    background:transparent!important;
}

.stProgress>div>div>div{
    background:linear-gradient(90deg,#00d464,#3b82f6)!important;
}
.stAlert{border-radius:8px!important;}
.stDataFrame{border:1px solid #30363d!important;border-radius:10px!important;}
hr{border-color:#30363d!important;}

.section-label{
    font-family:'Space Mono',monospace;font-size:10px;font-weight:700;
    letter-spacing:1.5px;color:#00d464;text-transform:uppercase;margin-bottom:6px;
}
.kpi-card{
    background:#161b22;border:1px solid #30363d;border-radius:10px;
    padding:16px 20px;text-align:center;
}
.kpi-value{
    font-family:'Space Mono',monospace;font-size:22px;font-weight:700;
    color:#00d464;line-height:1;
}
.kpi-label{
    font-size:10px;color:#8b949e;text-transform:uppercase;
    letter-spacing:1px;margin-top:4px;
}

/* folium iframe dark border */
.stfolium-container iframe{
    border:1px solid #30363d!important;
    border-radius:10px!important;
}
</style>
""", unsafe_allow_html=True)

# -------------------------
# Constants and aliases (from pasted text)
# -------------------------
DATA_DIR = Path("./Delco_data")
DATA_DIR.mkdir(exist_ok=True)

SITE_COLORS = [
    "#00d464","#3b82f6","#f59e0b","#ef4444","#8b5cf6",
    "#06b6d4","#f97316","#10b981","#e879f9","#facc15",
    "#64748b","#ec4899",
]

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

PM25_THRESHOLDS = [
    (0,   12,  "Good",           "#00d464"),
    (12,  35,  "Moderate",       "#facc15"),
    (35,  55,  "Unhealthy (S.G.)","#f97316"),
    (55,  150, "Unhealthy",      "#ef4444"),
    (150, 250, "Very Unhealthy", "#8b5cf6"),
    (250, 9999,"Hazardous",      "#7f1d1d"),
]

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

# -------------------------
# Utility functions (from pasted text, adapted)
# -------------------------
def hex_rgba(hex_color, alpha=0.25):
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

# -------------------------
# Replaced download functions (user-provided Copilot functions)
# -------------------------
def download_one_day(api_key, sn, date_str):
    """
    Download a single day using /v1/devices/{serial_number}/data-by-date/{date}/
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

def download_device_range(api_key, sn, start_dt, end_dt, max_workers=6, progress_callback=None):
    """
    Download daily files in parallel for a device between start_dt and end_dt (inclusive).
    progress_callback(completed, total) will be called from the main thread loop to update UI.
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
# Data preparation & analysis
# -------------------------
def prepare_merged_for_analysis(merged):
    merged = merged.copy()
    merged = merged.sort_values("datetime").reset_index(drop=True)
    # extract lat/lon
    if "geo" in merged.columns:
        latlon = merged["geo"].apply(extract_lat_lon)
        merged["lat"] = latlon.apply(lambda x: x[0])
        merged["lon"] = latlon.apply(lambda x: x[1])
    # normalize pollutant names
    merged = normalize_pollutants(merged)
    # ensure pm25 column exists
    if "pm25" not in merged.columns and "pm2_5" in merged.columns:
        merged = merged.rename(columns={"pm2_5": "pm25"})
    # hourly aggregation
    merged["hour"] = merged["datetime"].dt.floor("H")
    numeric_cols = merged.select_dtypes(include=[np.number]).columns.tolist()
    if len(numeric_cols) == 0:
        hourly = pd.DataFrame()
    else:
        hourly = merged.groupby(["site_name", "hour"], as_index=False)[numeric_cols].mean().reset_index(drop=True)
        if "pm25" in hourly.columns:
            hourly = hourly.sort_values(["site_name", "hour"])
            hourly["pm25_1hr"] = hourly.groupby("site_name")["pm25"].transform(lambda x: x.rolling(1, min_periods=1).mean())
            hourly["pm25_24hr"] = hourly.groupby("site_name")["pm25"].transform(lambda x: x.rolling(24, min_periods=1).mean())
            hourly["aqi"] = hourly["pm25"].apply(pm25_to_aqi)
            hourly["aqi_category"] = hourly["aqi"].apply(aqi_category)
    return merged, hourly

# -------------------------
# CSV saving utility
# -------------------------
def save_csvs_for_device(device_sn, df, start_dt, end_dt, save_individual, save_merged):
    base = DATA_DIR / device_sn
    base.mkdir(parents=True, exist_ok=True)
    if save_individual:
        df["date_only"] = df["datetime"].dt.date
        for date, g in df.groupby("date_only"):
            fname = base / f"{date.isoformat()}.csv"
            g.drop(columns=["date_only"], errors="ignore").to_csv(fname, index=False)
    if save_merged:
        s = start_dt.strftime("%Y%m%d")
        e = end_dt.strftime("%Y%m%d")
        fname = base / f"merged_{s}_{e}.csv"
        df.to_csv(fname, index=False)

# -------------------------
# Plot helpers (selected from pasted text)
# -------------------------
def chart_timeseries_site(df, site, color):
    avail = [c for c in list(POLLUTANT_ALIASES.keys())
             if c in df.columns and df[c].notna().any()]
    if not avail:
        return go.Figure()
    n = len(avail)
    titles = [f"{c.upper()} ({POLLUTANT_UNITS.get(c,'')})" for c in avail]
    fig = make_subplots(rows=n, cols=1, shared_xaxes=True,
                        subplot_titles=titles, vertical_spacing=0.04)
    for i, col in enumerate(avail, 1):
        s = df.dropna(subset=[col])
        fig.add_trace(go.Scatter(
            x=s["datetime"], y=s[col], name=col.upper(),
            mode="lines", line=dict(color=color, width=1.3),
            fill="tozeroy", fillcolor=hex_rgba(color,0.07),
            showlegend=(i==1),
            hovertemplate=f"%{{y:.2f}} {POLLUTANT_UNITS.get(col,'')}<extra>{col.upper()}</extra>",
        ), row=i, col=1)
    lkw = {k:v for k,v in DARK.items() if k not in ("xaxis","yaxis")}
    fig.update_layout(
        title=dict(text=f"Time Series — {site}", font=dict(color="#e6edf3",size=14)),
        height=210*n, **lkw)
    for i in range(1, n+1):
        fig.update_xaxes(gridcolor="#30363d", row=i, col=1)
        fig.update_yaxes(gridcolor="#30363d", row=i, col=1)
    return fig

# -------------------------
# Sidebar UI (updated block requested by user)
# -------------------------
def sidebar_inputs():
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
    return {
        "api_key": api_key,
        "sensor_map": sensor_map,
        "device_list": device_list,
        "lat_lon_map": lat_lon_map,
        "start_dt": start_dt,
        "end_dt": end_dt,
        "save_individual": save_individual,
        "save_merged": save_merged,
        "google_api_key": google_api_key,
        "max_workers": max_workers,
        "page": page,
        "fetch_btn": fetch_btn
    }

# -------------------------
# Main application
# -------------------------
def main():
    inputs = sidebar_inputs()

    st.markdown("""
    <div class="app-header">
      <h1>🌿 QUANTAQ MULTI-SITE AIR QUALITY DASHBOARD</h1>
      <p>Multi-device · 30-day chunks · Folium interactive map · Full analytics suite</p>
    </div>
    """, unsafe_allow_html=True)

    api_key = inputs["api_key"]
    sensor_map = inputs["sensor_map"]
    device_list = inputs["device_list"]
    lat_lon_map = inputs["lat_lon_map"]
    start_dt = inputs["start_dt"]
    end_dt = inputs["end_dt"]
    save_individual = inputs["save_individual"]
    save_merged = inputs["save_merged"]
    google_api_key = inputs["google_api_key"]
    max_workers = inputs["max_workers"]
    page = inputs["page"]
    fetch_btn = inputs["fetch_btn"]

    # placeholders for status/progress in main area
    status_slot = st.empty()
    progress_slot = st.empty()

    # Download action
    if fetch_btn:
        if not api_key:
            st.error("API key required.")
            return
        if not device_list:
            st.error("No devices specified.")
            return
        if end_dt < start_dt:
            st.error("End must be after Start.")
            return

        total_devices = len(device_list)
        dev_completed = 0
        overall_progress = progress_slot.progress(0.0)
        status_slot.info(f"Starting downloads for {total_devices} device(s)")

        all_frames = []
        for sn in device_list:
            status_slot.info(f"Downloading {sn} ...")
            # progress callback for per-day parallel downloads
            def progress_cb(completed, total):
                frac = (dev_completed + (completed/total)) / total_devices
                overall_progress.progress(frac)
            df = download_device_range(api_key, sn, start_dt, end_dt, max_workers=max_workers, progress_callback=progress_cb)
            dev_completed += 1
            overall_progress.progress(dev_completed / total_devices)
            if df is not None and not df.empty:
                df["device_sn"] = sn
                df["site_name"] = sensor_map.get(sn, sn)
                all_frames.append(df)
                if save_individual or save_merged:
                    save_csvs_for_device(sn, df, start_dt, end_dt, save_individual, save_merged)
            else:
                st.warning(f"No data for {sn} in the selected range.")
        overall_progress.empty()
        if not all_frames:
            status_slot.error("No data downloaded for any device.")
        else:
            merged = pd.concat(all_frames, ignore_index=True)
            merged, hourly = prepare_merged_for_analysis(merged)
            st.session_state["merged"] = merged
            st.session_state["hourly"] = hourly
            status_slot.success("Download complete and data prepared.")

    # Load session data
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
                stats_df = merged.groupby("site_name")[numeric_cols].agg(["count", "mean", "median", "min", "max", "std"])
                st.dataframe(stats_df)
    elif page == "Map":
        st.header("Site Map (Folium)")
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
                # Create folium map centered on mean coords
                center = [map_df["lat"].mean(), map_df["lon"].mean()]
                m = folium.Map(location=center, zoom_start=10, tiles="CartoDB dark_matter")
                marker_cluster = MarkerCluster().add_to(m)
                for _, row in map_df.iterrows():
                    cat_label, color = pm25_category(row.get("pm25") if "pm25" in row.index else row.get("aqi"))
                    popup_html = f"<b>{row['site_name']}</b><br>AQI: {row.get('aqi', 'N/A')}<br>Category: {row.get('aqi_category','Unknown')}"
                    folium.CircleMarker(
                        location=(row["lat"], row["lon"]),
                        radius=8,
                        color=color,
                        fill=True,
                        fill_color=color,
                        fill_opacity=0.9,
                        popup=folium.Popup(popup_html, max_width=300)
                    ).add_to(marker_cluster)
                st_folium(m, width=1100, height=650)
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
        - Folium map with clustered markers colored by AQI category.
        - CSV export for merged and hourly datasets.
        - Adjustable parallel workers for faster downloads.
        """)
    else:
        st.info("Select a page from the sidebar.")

if __name__ == "__main__":
    main()
