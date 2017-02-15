#!/usr/bin/env python3
"""
Real-Time Streaming Dashboard
Displays live metrics from the streaming ETL pipeline
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
import time
from datetime import datetime, timedelta
import json

# Page configuration
st.set_page_config(
    page_title="Streaming ETL Dashboard",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded"
)

class StreamingDashboard:
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'streaming_etl',
            'user': 'etl_user',
            'password': 'etl_password'
        }
    
    def get_connection(self):
        """Get database connection"""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            st.error(f"Database connection failed: {e}")
            return None
    
    def query_data(self, query):
        """Execute query and return DataFrame"""
        conn = self.get_connection()
        if conn:
            try:
                df = pd.read_sql_query(query, conn)
                conn.close()
                return df
            except Exception as e:
                st.error(f"Query failed: {e}")
                conn.close()
                return pd.DataFrame()
        return pd.DataFrame()
    
    def get_real_time_metrics(self):
        """Get real-time metrics"""
        query = """
        SELECT 
            COUNT(*) as total_orders,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(CASE WHEN is_valid = false THEN 1 END) as invalid_orders
        FROM raw_orders 
        WHERE order_date >= NOW() - INTERVAL '1 hour'
        """
        return self.query_data(query)
    
    def get_hourly_trends(self):
        """Get hourly trends"""
        query = """
        SELECT 
            DATE_TRUNC('hour', order_date) as hour,
            COUNT(*) as order_count,
            SUM(total_amount) as revenue,
            AVG(total_amount) as avg_order_value
        FROM raw_orders 
        WHERE order_date >= NOW() - INTERVAL '24 hours'
        AND is_valid = true
        GROUP BY DATE_TRUNC('hour', order_date)
        ORDER BY hour
        """
        return self.query_data(query)
    
    def get_regional_performance(self):
        """Get regional performance"""
        query = """
        SELECT 
            region,
            COUNT(*) as order_count,
            SUM(total_amount) as revenue,
            AVG(total_amount) as avg_order_value
        FROM raw_orders 
        WHERE order_date >= NOW() - INTERVAL '1 hour'
        AND is_valid = true
        GROUP BY region
        ORDER BY revenue DESC
        """
        return self.query_data(query)
    
    def get_customer_segments(self):
        """Get customer segment analysis"""
        query = """
        SELECT 
            segment,
            COUNT(*) as order_count,
            SUM(total_amount) as revenue,
            AVG(total_amount) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM raw_orders 
        WHERE order_date >= NOW() - INTERVAL '1 hour'
        AND is_valid = true
        GROUP BY segment
        ORDER BY revenue DESC
        """
        return self.query_data(query)
    
    def get_data_quality_metrics(self):
        """Get data quality metrics"""
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN is_valid = true THEN 1 END) as valid_records,
            COUNT(CASE WHEN is_valid = false THEN 1 END) as invalid_records,
            ROUND(
                COUNT(CASE WHEN is_valid = true THEN 1 END) * 100.0 / COUNT(*), 2
            ) as quality_score
        FROM raw_orders 
        WHERE order_date >= NOW() - INTERVAL '1 hour'
        """
        return self.query_data(query)
    
    def get_processing_latency(self):
        """Get processing latency metrics"""
        query = """
        SELECT 
            AVG(EXTRACT(EPOCH FROM (processing_time - order_date))) as avg_latency_seconds,
            MAX(EXTRACT(EPOCH FROM (processing_time - order_date))) as max_latency_seconds,
            MIN(EXTRACT(EPOCH FROM (processing_time - order_date))) as min_latency_seconds
        FROM raw_orders 
        WHERE order_date >= NOW() - INTERVAL '1 hour'
        """
        return self.query_data(query)

def main():
    """Main dashboard function"""
    st.title("🌊 Real-Time Streaming ETL Dashboard")
    st.markdown("---")
    
    dashboard = StreamingDashboard()
    
    # Sidebar controls
    st.sidebar.header("Dashboard Controls")
    auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)", value=True)
    refresh_button = st.sidebar.button("Refresh Now")
    
    if auto_refresh:
        # Auto-refresh every 30 seconds
        placeholder = st.empty()
        while True:
            with placeholder.container():
                render_dashboard(dashboard)
            time.sleep(30)
    else:
        render_dashboard(dashboard)

def render_dashboard(dashboard):
    """Render the dashboard content"""
    
    # Real-time metrics
    st.header("📊 Real-Time Metrics (Last Hour)")
    metrics_df = dashboard.get_real_time_metrics()
    
    if not metrics_df.empty:
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Total Orders",
                f"{metrics_df.iloc[0]['total_orders']:,}",
                delta=None
            )
        
        with col2:
            st.metric(
                "Total Revenue",
                f"${metrics_df.iloc[0]['total_revenue']:,.2f}",
                delta=None
            )
        
        with col3:
            st.metric(
                "Avg Order Value",
                f"${metrics_df.iloc[0]['avg_order_value']:.2f}",
                delta=None
            )
        
        with col4:
            st.metric(
                "Unique Customers",
                f"{metrics_df.iloc[0]['unique_customers']:,}",
                delta=None
            )
        
        with col5:
            invalid_orders = metrics_df.iloc[0]['invalid_orders']
            total_orders = metrics_df.iloc[0]['total_orders']
            error_rate = (invalid_orders / total_orders * 100) if total_orders > 0 else 0
            st.metric(
                "Error Rate",
                f"{error_rate:.1f}%",
                delta=f"{invalid_orders} invalid"
            )
    
    st.markdown("---")
    
    # Charts row 1
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Hourly Trends (24h)")
        trends_df = dashboard.get_hourly_trends()
        
        if not trends_df.empty:
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('Order Count', 'Revenue'),
                vertical_spacing=0.1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=trends_df['hour'],
                    y=trends_df['order_count'],
                    mode='lines+markers',
                    name='Orders',
                    line=dict(color='#1f77b4')
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=trends_df['hour'],
                    y=trends_df['revenue'],
                    mode='lines+markers',
                    name='Revenue',
                    line=dict(color='#ff7f0e')
                ),
                row=2, col=1
            )
            
            fig.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data available")
    
    with col2:
        st.subheader("🌍 Regional Performance")
        regional_df = dashboard.get_regional_performance()
        
        if not regional_df.empty:
            fig = px.bar(
                regional_df,
                x='region',
                y='revenue',
                title='Revenue by Region',
                color='order_count',
                color_continuous_scale='viridis'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No regional data available")
    
    # Charts row 2
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("👥 Customer Segments")
        segments_df = dashboard.get_customer_segments()
        
        if not segments_df.empty:
            fig = px.pie(
                segments_df,
                values='revenue',
                names='segment',
                title='Revenue by Customer Segment'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No segment data available")
    
    with col2:
        st.subheader("🔍 Data Quality")
        quality_df = dashboard.get_data_quality_metrics()
        
        if not quality_df.empty:
            quality_score = quality_df.iloc[0]['quality_score']
            valid_records = quality_df.iloc[0]['valid_records']
            invalid_records = quality_df.iloc[0]['invalid_records']
            
            # Quality gauge
            fig = go.Figure(go.Indicator(
                mode = "gauge+number+delta",
                value = quality_score,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "Data Quality Score (%)"},
                delta = {'reference': 95},
                gauge = {
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 70], 'color': "lightgray"},
                        {'range': [70, 90], 'color': "yellow"},
                        {'range': [90, 100], 'color': "green"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 95
                    }
                }
            ))
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Quality details
            st.write(f"✅ Valid Records: {valid_records:,}")
            st.write(f"❌ Invalid Records: {invalid_records:,}")
        else:
            st.info("No quality data available")
    
    # Processing metrics
    st.markdown("---")
    st.subheader("⚡ Processing Performance")
    
    latency_df = dashboard.get_processing_latency()
    
    if not latency_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Avg Latency",
                f"{latency_df.iloc[0]['avg_latency_seconds']:.2f}s"
            )
        
        with col2:
            st.metric(
                "Max Latency",
                f"{latency_df.iloc[0]['max_latency_seconds']:.2f}s"
            )
        
        with col3:
            st.metric(
                "Min Latency",
                f"{latency_df.iloc[0]['min_latency_seconds']:.2f}s"
            )
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"**Status:** 🟢 Active"
    )

if __name__ == "__main__":
    main()