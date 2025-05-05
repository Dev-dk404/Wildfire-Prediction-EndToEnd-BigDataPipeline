import streamlit as st
import pandas as pd
import plotly.express as px
import base64
from pathlib import Path

# Configuration
st.set_page_config(page_title="Wildfire Dashboard", page_icon="🔥", layout="wide")

# Load background image and encode to base64
background_image = "wildfire_theme.png"
with open(background_image, "rb") as f:
    encoded = base64.b64encode(f.read()).decode()

# Custom CSS styling
st.markdown(f"""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;700&display=swap');
        html, body, [class*="css"] {{
            font-family: 'Inter', sans-serif;
            color: white !important;
        }}
        .stApp {{
            background: url("data:image/png;base64,{encoded}") no-repeat center center fixed;
            background-size: cover;
        }}
        .card {{
            padding: 1rem;
            border-radius: 12px;
            background-color: rgba(0, 0, 0, 0.6);
            box-shadow: 0 0 20px rgba(255,255,255,0.2);
            margin-bottom: 1rem;
            color: white !important;
        }}
        .metric-card {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(8px);
            color: #fff;
            padding: 1rem 1.5rem;
            border-radius: 12px;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }}
        h1, h2, h3, h4, h5, h6, p, b, span, label {{
            color: white !important;
        }}
        .footer {{
            text-align: center;
            padding: 1rem;
            color: white;
            font-size: 0.9rem;
        }}
        .block-container .stTabs [role="tab"] {{
            color: white !important;
        }}
        .block-container .stTabs [aria-selected="true"] {{
            border-bottom: 3px solid #FFA500 !important;
            color: #FFA500 !important;
        }}
        .stSlider > div[data-baseweb="slider"] > div > div {{
            color: white !important;
        }}
        .stSlider > label {{
            color: white !important;
        }}
        .stSlider .css-1v0mbdj, .stSlider .css-14xtw13 {{
            background: white !important;
            border-color: white !important;
        }}
        .stButton button {{
            background-color: #FFA500;
            color: black;
            font-weight: bold;
            border-radius: 10px;
            border: none;
            padding: 0.6em 1.2em;
            transition: background-color 0.3s ease;
        }}
        .stButton button:hover {{
            background-color: #FF8C00;
            color: white;
        }}
        .stDownloadButton > button {{
            color: black !important;
            background-color: white !important;
            font-weight: bold !important;
            border-radius: 8px;
            padding: 0.5em 1.2em;
            border: 1px solid #ccc !important;
            transition: all 0.3s ease-in-out;
            display: inline-block !important;
            text-align: center !important;
        }}
        .stDownloadButton > button:hover {{
            background-color: #f5f5f5 !important;
            color: black !important;
            border: 1px solid #999 !important;
        }}
    </style>
""", unsafe_allow_html=True)

# Title section
st.markdown("""
<div class="card">
    <h1>🔥 Wildfire Prediction & Forecast Dashboard</h1>
    <p>Explore interactive forecasts, trends, and maps of wildfire risk across the United States</p>
</div>
""", unsafe_allow_html=True)

# Load data
try:
    forecast_df = pd.read_csv("future_wildfire_forecast.csv")
    forecast_df["Year"] = pd.to_datetime(forecast_df["Month"]).dt.year
    forecast_df["Month_Formatted"] = pd.to_datetime(forecast_df["Month"]).dt.strftime("%Y-%m")
    total_fires = int(forecast_df['Forecasted_Fires'].sum())
    avg_fires = forecast_df['Forecasted_Fires'].mean()
    top_month = forecast_df.sort_values("Forecasted_Fires", ascending=False).iloc[0]["Month_Formatted"]
except:
    forecast_df = pd.DataFrame()
    total_fires = 0
    avg_fires = 0
    top_month = "N/A"

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🌍 US Maps", "📈 Trends", "📂 Model Outputs"])

#TAB 1 
with tab1:
    st.markdown(f"""
        <div class="metric-card">
            <div><b>Total Forecasted Fires</b><br>{total_fires:,}</div>
            <div><b>Average Fires / Month</b><br>{avg_fires:.1f}</div>
            <div><b>Peak Forecast Month</b><br>{top_month}</div>
        </div>
    """, unsafe_allow_html=True)

    st.subheader("🔍 Feature Importance")
    show = Path("feature_importance.html")
    if show.exists():
        with open(show, "r", encoding="utf-8") as f:
            st.components.v1.html(f.read(), height=600, scrolling=True)

    st.subheader("📉 Confusion Matrix")
    show = Path("confusion_matrix.html")
    if show.exists():
        with open(show, "r", encoding="utf-8") as f:
            st.components.v1.html(f.read(), height=500, scrolling=True)

#TAB 2 
with tab2:
    st.subheader("🌎 Forecasted Wildfire Map")
    year_filter = st.slider("Select Forecast Year", 2025, 2030, 2025)
    df_map = forecast_df[forecast_df["Year"] == year_filter]

    if not df_map.empty:
        fig = px.scatter_mapbox(
            df_map, lat="Latitude", lon="Longitude",
            color="Forecasted_Fires", size="Forecasted_Fires",
            hover_name="Month_Formatted",
            color_continuous_scale="OrRd", zoom=3,
            mapbox_style="open-street-map",
        )
        st.plotly_chart(fig, use_container_width=True)
        st.download_button(
            label="📥 Download Wildfire CSV",
            data=forecast_df.to_csv(index=False),
            file_name="wildfire_forecast.csv",
            mime="text/csv",
            key="csv-download"
        )

    st.subheader("🗺️ Historical State-Level Wildfires")
    show = Path("wildfire_us_state_map.html")
    if show.exists():
        with open(show, "r", encoding="utf-8") as f:
            st.components.v1.html(f.read(), height=600, scrolling=True)

#TAB 3
with tab3:
    st.subheader("📆 Monthly Forecast Animation")
    try:
        forecast_df["Month_Formatted"] = pd.to_datetime(forecast_df["Month"])
        fig_anim = px.scatter_mapbox(
            forecast_df,
            lat="Latitude", lon="Longitude",
            color="Forecasted_Fires", size="Forecasted_Fires",
            hover_name="Month",
            animation_frame=forecast_df["Month_Formatted"].dt.strftime("%Y-%m"),
            mapbox_style="open-street-map", zoom=3,
            color_continuous_scale="YlOrRd"
        )
        st.plotly_chart(fig_anim, use_container_width=True)
    except:
        st.warning("Animation failed to load.")

    st.subheader("📈 Trend Line View")
    show = Path("wildfire_future_trend_forecast.html")
    if show.exists():
        with open(show, "r", encoding="utf-8") as f:
            st.components.v1.html(f.read(), height=500, scrolling=True)

#TAB 4
with tab4:
    st.subheader("🌡️ Temperature vs Fire")
    show = Path("temperature_vs_fire.html")
    if show.exists():
        with open(show, "r", encoding="utf-8") as f:
            st.components.v1.html(f.read(), height=500, scrolling=True)

    st.subheader("📊 Confidence Distribution")
    show = Path("confidence_distribution.html")
    if show.exists():
        with open(show, "r", encoding="utf-8") as f:
            st.components.v1.html(f.read(), height=500, scrolling=True)

    st.subheader("☀️ Day vs Night Pie Chart")
    show = Path("daynight_predictions.html")
    if show.exists():
        with open(show, "r", encoding="utf-8") as f:
            st.components.v1.html(f.read(), height=500, scrolling=True)

# Footer
st.markdown(
    "<div class='footer'>© 2025 Wildfire AI | "
    "<a href='https://github.com/Ritesh778' style='color:#FFA500;'>GitHub</a> | "
    "Created by <b>Ritesh Janga</b> using Streamlit</div>",
    unsafe_allow_html=True
)
