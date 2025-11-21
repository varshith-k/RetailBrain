import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import time
import os

# Configuration
API_URL = os.getenv('API_URL', 'http://localhost:8000')

st.set_page_config(
    page_title="Real-Time E-Commerce Dashboard",
    layout="wide",
)

st.title("🛒 Real-Time E-Commerce Analytics")

def fetch_data():
    try:
        response = requests.get(f"{API_URL}/sales")
        if response.status_code == 200:
            return response.json()
        else:
            st.error("Failed to fetch data from API")
            return []
    except Exception as e:
        st.error(f"Error connecting to API: {e}")
        return []

# Placeholder for auto-refresh
placeholder = st.empty()

while True:
    data = fetch_data()
    
    with placeholder.container():
        if data:
            df = pd.DataFrame(data)
            df['minute'] = pd.to_datetime(df['minute'])
            df = df.sort_values('minute')

            # Metrics
            total_revenue = df['total_sales'].sum()
            total_orders = df['purchase_count'].sum()
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Revenue (Last 100 mins)", f"${total_revenue:,.2f}")
            col2.metric("Total Orders (Last 100 mins)", f"{total_orders}")
            
            # Charts
            fig_sales = px.line(df, x='minute', y='total_sales', title='Sales per Minute')
            st.plotly_chart(fig_sales, use_container_width=True)

            fig_orders = px.bar(df, x='minute', y='purchase_count', title='Orders per Minute')
            st.plotly_chart(fig_orders, use_container_width=True)
            
            st.dataframe(df.sort_values('minute', ascending=False).head(10))
        else:
            st.warning("No data available yet. Waiting for events...")

    time.sleep(5)
