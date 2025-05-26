"""
CAISO Generation Interconnection Queue Dashboard
"""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os
import sys

# Import data loader with multiple fallback approaches
# This handles different environment contexts (local dev, Docker, etc.)
try:
    # Try direct import first (same directory)
    from data_loader import DataLoader
except ImportError:
    try:
        # Try with current directory in path
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from data_loader import DataLoader
    except ImportError:
        # Try with absolute import from dashboard package
        from dashboard.data_loader import DataLoader

# Set page config
st.set_page_config(
    page_title="CAISO Generation Interconnection Queue Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Constants
DEFAULT_DB_PATH = 'data/caiso_queue.db'
KPI_OPTIONS = [
    "Overview",
    "Capacity by Fuel Type", 
    "Project Status", 
    "Top ISO Zones", 
    "Lead Time Analysis", 
    "Timeline Delays",
    "Top Projects"
]

@st.cache_resource
def get_data_loader():
    """Create and cache the data loader"""
    try:
        return DataLoader()
    except FileNotFoundError as e:
        st.error(f"Database not found: {e}")
        return None

def format_mw(mw_value):
    """Format MW value as GW if appropriate"""
    if mw_value >= 1000:
        return f"{mw_value/1000:.2f} GW"
    return f"{mw_value:.0f} MW"

def create_overview():
    """Create overview metrics dashboard"""
    loader = get_data_loader()
    if not loader:
        return
    
    # Get key metrics
    status_df = loader.project_count_by_status()
    cancellation_df = loader.cancellation_rate()
    lead_time_df = loader.average_lead_time()
    
    # Extract metrics
    active_count = status_df[status_df['status'] == 'Active']['project_count'].iloc[0]
    active_mw = status_df[status_df['status'] == 'Active']['total_mw'].iloc[0]
    completed_count = status_df[status_df['status'] == 'Completed']['project_count'].iloc[0]
    completed_mw = status_df[status_df['status'] == 'Completed']['total_mw'].iloc[0]
    cancellation_rate = cancellation_df['cancellation_rate'].iloc[0]
    avg_lead_time = lead_time_df['average_lead_time_days'].iloc[0]
    
    # Create metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Active Projects", f"{active_count:,}")
        st.metric("Active Capacity", format_mw(active_mw))
    with col2:
        st.metric("Completed Projects", f"{completed_count:,}")
        st.metric("Completed Capacity", format_mw(completed_mw))
    with col3:
        st.metric("Cancellation Rate", f"{cancellation_rate:.1%}")
        total_projects = active_count + completed_count + status_df[status_df['status'] == 'Withdrawn']['project_count'].iloc[0]
        st.metric("Total Projects", f"{total_projects:,}")
    with col4:
        st.metric("Avg. Lead Time", f"{avg_lead_time:.1f} days")
        total_mw = status_df['total_mw'].sum()
        st.metric("Total Capacity", format_mw(total_mw))
    
    # Create two charts side by side
    col1, col2 = st.columns(2)
    
    # Project count by status
    with col1:
        fig1 = px.pie(
            status_df, 
            values='project_count', 
            names='status',
            title='Project Count by Status',
            color='status',
            color_discrete_map={
                'Active': '#2ecc71', 
                'Completed': '#3498db', 
                'Withdrawn': '#e74c3c'
            }
        )
        fig1.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig1, use_container_width=True)
    
    # Capacity by status
    with col2:
        fig2 = px.bar(
            status_df, 
            x='status', 
            y='total_mw',
            title='Capacity by Status (MW)',
            color='status',
            color_discrete_map={
                'Active': '#2ecc71', 
                'Completed': '#3498db', 
                'Withdrawn': '#e74c3c'
            }
        )
        fig2.update_yaxes(title_text='Capacity (MW)')
        st.plotly_chart(fig2, use_container_width=True)

def show_capacity_by_fuel():
    """Show capacity by fuel type visualization"""
    loader = get_data_loader()
    if not loader:
        return
    
    try:    
        df = loader.capacity_by_fuel()
        
        # Check if dataframe is empty or has the expected columns
        if df is None or df.empty or 'fuel' not in df.columns or 'total_mw' not in df.columns:
            st.warning("No fuel capacity data available or data is in an unexpected format.")
            return
        
        # Create options for filtering
        fuel_categories = []
        for fuel in df['fuel']:
            for f in fuel.split('/'):
                if f not in fuel_categories:
                    fuel_categories.append(f)
        
        fuel_filter = st.multiselect(
            "Filter by fuel type (contains):",
            options=sorted(fuel_categories),
            default=[]
        )
        
        # Apply filters
        filtered_df = df
        if fuel_filter:
            filtered_df = df[df['fuel'].apply(lambda x: any(f in x for f in fuel_filter))]
            
        # Sort by capacity
        filtered_df = filtered_df.sort_values('total_mw', ascending=False)
        
        # Create visualization
        st.subheader("Capacity by Fuel Type")
        
        # Show in both table and chart format
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.dataframe(
                filtered_df,
                column_config={
                    "fuel": "Fuel Type",
                    "total_mw": st.column_config.NumberColumn("Total Capacity (MW)", format="%.2f")
                },
                hide_index=True
            )
        
        with col2:
            try:
                fig = px.bar(
                    filtered_df,
                    x='fuel',
                    y='total_mw',
                    color='fuel',
                    title='Generation Capacity by Fuel Type'
                )
                fig.update_layout(
                    xaxis_title="Fuel Type",
                    yaxis_title="Capacity (MW)",
                    xaxis=dict(tickangle=45)
                )
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"Error creating fuel capacity visualization: {str(e)}")
    except Exception as e:
        st.error(f"Error loading fuel capacity data: {str(e)}")
    
    # Additional visualization - Pie chart
    try:
        fig2 = px.pie(
            filtered_df,
            values='total_mw',
            names='fuel',
            title='Capacity Distribution by Fuel Type'
        )
        fig2.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig2, use_container_width=True)
    except Exception as e:
        st.error(f"Error creating fuel distribution visualization: {str(e)}")

def show_project_status():
    """Show project status visualization"""
    loader = get_data_loader()
    if not loader:
        return
    
    try:
        df = loader.project_count_by_status()
        
        # Create visualizations
        st.subheader("Project Status")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig1 = px.pie(
                df, 
                values='project_count', 
                names='status',
                title='Project Count by Status',
                color='status',
                color_discrete_map={
                    'Active': '#2ecc71', 
                    'Completed': '#3498db', 
                    'Withdrawn': '#e74c3c'
                }
            )
            fig1.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            fig2 = px.bar(
                df, 
                x='status', 
                y='total_mw',
                title='Total Capacity by Status (MW)',
                color='status',
                color_discrete_map={
                    'Active': '#2ecc71', 
                    'Completed': '#3498db', 
                    'Withdrawn': '#e74c3c'
                }
            )
            fig2.update_layout(xaxis_title="Status", yaxis_title="Capacity (MW)")
            st.plotly_chart(fig2, use_container_width=True)
        
        # Additional metrics
        total_projects = df['project_count'].sum()
        total_capacity = df['total_mw'].sum()
        
        st.metric("Total Projects in Database", f"{total_projects:,}")
        st.metric("Total Capacity", format_mw(total_capacity))
        
        # Cancellation rate
        cancellation_df = loader.cancellation_rate()
        rate = cancellation_df['cancellation_rate'].iloc[0]
        st.metric("Cancellation Rate", f"{rate:.1%}")
    except Exception as e:
        st.error(f"Error in project status visualization: {str(e)}")

def show_top_iso_zones():
    """Show top ISO zones visualization"""
    loader = get_data_loader()
    if not loader:
        return
        
    try:
        df = loader.top5_iso_zones()
        
        # Check if dataframe is empty or has the expected columns
        if df is None or df.empty or 'iso_zone' not in df.columns or 'total_mw' not in df.columns:
            st.warning("No ISO zone data available or data is in an unexpected format.")
            return
            
        # Create visualization
        st.subheader("Top ISO Zones by Capacity")
        
        # Create the Plotly figure with error handling
        try:
            fig = px.bar(
                df,
                x='iso_zone',
                y='total_mw',
                color='iso_zone',
                title='Top 5 ISO Zones by Generation Capacity'
            )
            fig.update_layout(
                xaxis_title="ISO Zone", 
                yaxis_title="Capacity (MW)",
                xaxis=dict(tickangle=45)
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating ISO zones visualization: {str(e)}")
            
        # Show the data table
        st.dataframe(
            df,
            column_config={
                "iso_zone": "ISO Zone",
                "total_mw": st.column_config.NumberColumn("Total Capacity (MW)", format="%.2f")
            },
            hide_index=True
        )
    except Exception as e:
        st.error(f"Error loading ISO zones data: {str(e)}")

def show_lead_time_analysis():
    """Show lead time analysis"""
    loader = get_data_loader()
    if not loader:
        return
    
    try:    
        lead_time_df = loader.average_lead_time()
        avg_lead_time = lead_time_df['average_lead_time_days'].iloc[0]
        
        st.subheader("Interconnection Request Lead Time")
        st.metric("Average Lead Time", f"{avg_lead_time:.1f} days")
        
        # Create a gauge chart for lead time
        fig = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = avg_lead_time,
            title = {'text': "Average Lead Time (days)"},
            gauge = {
                'axis': {'range': [0, max(180, avg_lead_time * 1.5)]},
                'bar': {'color': "#1f77b4"},
                'steps': [
                    {'range': [0, 60], 'color': "#2ecc71"},
                    {'range': [60, 120], 'color': "#f1c40f"},
                    {'range': [120, max(180, avg_lead_time * 1.5)], 'color': "#e74c3c"}
                ]
            }
        ))
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.info(
            "Lead time is measured as the number of days between when an " +
            "interconnection request is received and when a queue position is assigned."
        )
    except Exception as e:
        st.error(f"Error in lead time analysis: {str(e)}")

def show_timeline_delays():
    """Show timeline delays visualization"""
    loader = get_data_loader()
    if not loader:
        return
        
    try:
        df = loader.timeline_delay_by_fuel()
        
        # Check if dataframe is empty or has the expected columns
        if df is None or df.empty or 'fuel' not in df.columns or 'avg_delay_days' not in df.columns:
            st.warning("No timeline delay data available or data is in an unexpected format.")
            return
        
        # Create visualization
        st.subheader("Project Timeline Delays by Fuel Type")
        
        # Create options for filtering
        fuel_categories = []
        for fuel in df['fuel']:
            for f in fuel.split('/'):
                if f not in fuel_categories:
                    fuel_categories.append(f)
        
        fuel_filter = st.multiselect(
            "Filter by fuel type (contains):",
            options=sorted(fuel_categories),
            default=[]
        )
        
        # Apply filters
        filtered_df = df
        if fuel_filter:
            filtered_df = df[df['fuel'].apply(lambda x: any(f in x for f in fuel_filter))]
        
        # Sort by delay
        filtered_df = filtered_df.sort_values('avg_delay_days', ascending=False)
        
        try:
            fig = px.bar(
                filtered_df,
                x='fuel',
                y='avg_delay_days',
                color='fuel',
                title='Average Timeline Delay by Fuel Type (days)'
            )
            fig.update_layout(
                xaxis_title="Fuel Type", 
                yaxis_title="Average Delay (days)",
                xaxis=dict(tickangle=45)
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating timeline delay visualization: {str(e)}")
        
        # Show the data table
        st.dataframe(
            filtered_df,
            column_config={
                "fuel": "Fuel Type",
                "avg_delay_days": st.column_config.NumberColumn("Average Delay (days)", format="%.1f")
            },
            hide_index=True
        )
        
        st.info(
            "Timeline delay is measured as the difference in days between " +
            "the planned online date and the actual online date."
        )
    except Exception as e:
        st.error(f"Timeline delay data is not available: {str(e)}")

def show_top_projects():
    """Show top projects visualization"""
    loader = get_data_loader()
    if not loader:
        return
    
    try:    
        df = loader.top_projects_by_net_mw()
        
        # Check if dataframe is empty or has the expected columns
        if df is None or df.empty or 'project_name' not in df.columns or 'net_mw' not in df.columns or 'fuel_types' not in df.columns:
            st.warning("No top projects data available or data is in an unexpected format.")
            return
        
        # Create visualization
        st.subheader("Top Projects by Net MW")
        
        # Add filtering options
        fuel_types = []
        for fuel in df['fuel_types']:
            for f in str(fuel).split('/'):
                if f not in fuel_types:
                    fuel_types.append(f)
        
        fuel_filter = st.multiselect(
            "Filter by fuel type (contains):",
            options=sorted(fuel_types),
            default=[]
        )
        
        # Apply filters
        filtered_df = df
        if fuel_filter:
            filtered_df = df[df['fuel_types'].apply(lambda x: any(f in str(x) for f in fuel_filter))]
        
        try:
            # Bar chart for top projects
            fig = px.bar(
                filtered_df,
                x='project_name',
                y='net_mw',
                color='fuel_types',
                title='Top Projects by Net MW Contribution',
                hover_data=['queue_position', 'county', 'state']
            )
            fig.update_layout(
                xaxis_title="Project", 
                yaxis_title="Net MW",
                xaxis=dict(tickangle=45)
            )
            st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating top projects visualization: {str(e)}")
            
        # Show the data table
        st.dataframe(
            filtered_df,
            column_config={
                "project_name": "Project Name",
                "queue_position": "Queue Position",
                "net_mw": st.column_config.NumberColumn("Net MW", format="%.2f"),
                "fuel_types": "Fuel Types",
                "county": "County",
                "state": "State"
            },
            hide_index=True
        )
    except Exception as e:
        st.error(f"Error loading top projects data: {str(e)}")

def main():
    """Main function to render the Streamlit dashboard"""
    # Title and introduction
    st.title("CAISO Generation Interconnection Queue Dashboard")
    
    # Add current date 
    st.markdown("Data as of: **May 25, 2025**")
    
    # Sidebar
    st.sidebar.image("dashboard/gen_queue_img.png", width=200)
    st.sidebar.title("Dashboard Controls")
    
    # Select KPI to display
    selected_kpi = st.sidebar.selectbox(
        "Select Dashboard:",
        options=KPI_OPTIONS
    )
    
    # About section
    with st.sidebar.expander("About this Dashboard"):
        st.markdown("""
        This dashboard visualizes data from the CAISO Generation 
        Interconnection Queue, providing insights into renewable 
        energy projects in California.
        
        Data is sourced directly from CAISO's public queue reports 
        and updated weekly.
        """)
    
    # Display the selected KPI visualization
    if selected_kpi == "Overview":
        create_overview()
    elif selected_kpi == "Capacity by Fuel Type":
        show_capacity_by_fuel()
    elif selected_kpi == "Project Status":
        show_project_status()
    elif selected_kpi == "Top ISO Zones":
        show_top_iso_zones()
    elif selected_kpi == "Lead Time Analysis":
        show_lead_time_analysis()
    elif selected_kpi == "Timeline Delays":
        show_timeline_delays()
    elif selected_kpi == "Top Projects":
        show_top_projects()

if __name__ == "__main__":
    main()
