import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time
import os

# Configuration
API_URL = os.getenv('API_URL', 'http://localhost:8000')

st.set_page_config(
    page_title="RetailBrain Commerce Intelligence",
    layout="wide",
)

st.title("RetailBrain: Real-Time Commerce Intelligence")
st.caption("Live metrics + anomaly feed + AI assistant for business questions")


def fetch_json(path, params=None, method="get", payload=None):
    try:
        url = f"{API_URL}{path}"
        if method.lower() == "post":
            response = requests.post(url, json=payload or {}, timeout=8)
        else:
            response = requests.get(url, params=params or {}, timeout=8)
        if response.status_code == 200:
            return response.json()
        st.warning(f"{path} returned status {response.status_code}")
        return None
    except Exception as e:
        st.error(f"Error connecting to API for {path}: {e}")
        return None


auto_refresh = st.sidebar.checkbox("Auto refresh", value=True)
refresh_seconds = st.sidebar.slider("Refresh interval (seconds)", min_value=5, max_value=30, value=5)
time_window = st.sidebar.selectbox("Analytics window", options=[30, 60, 120], index=1)

sales = fetch_json("/sales", params={"limit": 120}) or []
overview = fetch_json("/metrics/overview", params={"minutes": time_window}) or {}
alerts = fetch_json("/alerts/recent", params={"limit": 10}) or []
trending = fetch_json("/metrics/trending-products", params={"limit": 10}) or []

if sales:
    df = pd.DataFrame(sales)
    df['minute'] = pd.to_datetime(df['minute'])
    df = df.sort_values('minute')
else:
    df = pd.DataFrame(columns=['minute', 'total_sales', 'purchase_count'])

col1, col2, col3, col4 = st.columns(4)
col1.metric("Revenue", f"${overview.get('total_revenue', 0):,.2f}")
col2.metric("Purchases", f"{overview.get('total_purchases', 0)}")
col3.metric("Conversion", f"{overview.get('conversion_rate', 0) * 100:.1f}%")
col4.metric("Abandonment", f"{overview.get('abandonment_rate', 0) * 100:.1f}%")

tab_live, tab_alerts, tab_ai = st.tabs(["Live Analytics", "Anomaly Feed", "AI Analyst"])

with tab_live:
    if not df.empty:
        fig_sales = px.line(df, x='minute', y='total_sales', title='Sales per Minute')
        st.plotly_chart(fig_sales, use_container_width=True)

        fig_orders = px.bar(df, x='minute', y='purchase_count', title='Orders per Minute')
        st.plotly_chart(fig_orders, use_container_width=True)
    else:
        st.info("No sales data available yet. Waiting for events...")

    st.subheader("Trending Products")
    if trending:
        trending_df = pd.DataFrame(trending)
        st.dataframe(trending_df, use_container_width=True)
    else:
        st.info("Trending products will appear after purchases are processed.")

with tab_alerts:
    st.subheader("Recent Anomalies")
    if alerts:
        for alert in alerts:
            severity = (alert.get("severity") or "info").upper()
            minute = alert.get("minute", "")
            st.markdown(f"**[{severity}] {alert.get('alert_type', 'unknown')}** at {minute}")
            st.write(alert.get("message", ""))
            details = alert.get("details")
            if details:
                st.json(details)
            st.divider()
    else:
        st.success("No anomalies detected in the current feed.")

    if st.button("Generate Executive Summary"):
        summary = fetch_json("/reports/executive-summary", params={"minutes": time_window})
        if summary:
            st.markdown("### Executive Summary")
            for line in summary.get("highlights", []):
                st.write(f"- {line}")
            st.markdown("### Recommendations")
            for action in summary.get("recommendations", []):
                st.write(f"- {action}")

with tab_ai:
    st.subheader("Ask RetailBrain")
    st.write("Example questions: Why did purchases drop? Which products are trending? Summarize cart abandonment today.")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    for msg in st.session_state.chat_history:
        role = msg.get("role", "assistant")
        with st.chat_message(role):
            st.markdown(msg.get("content", ""))

    question = st.chat_input("Ask a business question...")
    if question:
        st.session_state.chat_history.append({"role": "user", "content": question})
        response = fetch_json("/assistant/query", method="post", payload={"question": question})
        if response:
            answer = response.get("answer", "No response")
            evidence = response.get("evidence", [])
            actions = response.get("next_actions", [])

            response_text = answer
            if evidence:
                response_text += "\n\nEvidence:\n" + "\n".join([f"- {item}" for item in evidence])
            if actions:
                response_text += "\n\nNext actions:\n" + "\n".join([f"- {item}" for item in actions])

            st.session_state.chat_history.append({"role": "assistant", "content": response_text})
            st.rerun()

    if st.button("Run Multi-Agent Business Update"):
        update = fetch_json("/agent/business-update", params={"minutes": time_window})
        if update and update.get("agents"):
            st.json(update["agents"])

if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()
