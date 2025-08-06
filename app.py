# app.py

import streamlit as st
import pandas as pd
import pymongo
from datetime import datetime as dt_class, date as date_class, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
MONGO_DB_URL = os.getenv("MONGO_DB_URL", "mongodb://localhost:27017/")
DB_NAME = "indian_market_scanner_db"
NEWS_COLLECTION_NAME = "news_articles"
INSIGHTS_COLLECTION_NAME = "insights"
PAGE_TITLE = "Sentiment-Driven Indian Market Scanner"
PAGE_ICON = "📈"

# Set Streamlit page configuration
st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout="wide")


# --- Database Connection ---
@st.cache_resource
def get_db_collections():
    """Establishes a connection to MongoDB and returns the required collections."""
    try:
        client = pymongo.MongoClient(MONGO_DB_URL, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        news_collection = db[NEWS_COLLECTION_NAME]
        insights_collection = db[INSIGHTS_COLLECTION_NAME]
        st.success("Successfully connected to MongoDB.")
        return news_collection, insights_collection
    except Exception as e:
        st.error(f"Failed to connect to MongoDB. Please ensure your MongoDB server is running. Error: {e}")
        return None, None


# --- Data Fetching and Processing ---
@st.cache_data(ttl=3600)  # Cache data for 1 hour to prevent constant DB reads
def fetch_and_process_data(news_collection, insights_collection):
    """Fetches all necessary data and performs pre-processing for display."""

    news_data = list(news_collection.find().sort("publication_date", pymongo.DESCENDING))
    insights_data = list(insights_collection.find().sort("date", pymongo.DESCENDING))

    if not news_data or not insights_data:
        return None, None, None

    news_df = pd.DataFrame(news_data).drop(columns=['_id'])
    insights_df = pd.DataFrame(insights_data).drop(columns=['_id'])

    # Convert date columns to datetime.date objects for Streamlit's slider
    insights_df['date'] = pd.to_datetime(insights_df['date']).dt.date
    insights_df['avg_sentiment'] = pd.to_numeric(insights_df['avg_sentiment'], errors='coerce')
    insights_df['signal'].fillna('Neutral', inplace=True)

    news_df['publication_date'] = pd.to_datetime(news_df['publication_date']).dt.date

    # Get a list of unique sectors for the dropdown menu
    sectors = sorted(insights_df['sector'].unique().tolist())

    return news_df, insights_df, sectors


# --- Visualization Functions ---
def create_sentiment_price_chart(insights_df, selected_sector, date_range):
    """Creates a chart with price and sentiment data overlaid."""
    filtered_df = insights_df[
        (insights_df['sector'] == selected_sector) &
        (insights_df['date'] >= date_range[0]) &
        (insights_df['date'] <= date_range[1])
        ].copy()

    if filtered_df.empty:
        st.warning(f"No data available for {selected_sector} in the selected date range.")
        return go.Figure()

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add Price (Close) trace on primary y-axis
    fig.add_trace(
        go.Scatter(
            x=filtered_df['date'], y=filtered_df['close'], name=f"{selected_sector} Price",
            mode='lines', line=dict(color='lightgray', width=2),
            hovertemplate="Date: %{x}<br>Price: %{y:.2f}<extra></extra>"
        ),
        secondary_y=False,
    )

    # Add SMA traces
    fig.add_trace(
        go.Scatter(x=filtered_df['date'], y=filtered_df['sma_20'], name='20-Day SMA',
                   line=dict(color='orange', dash='dash')),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=filtered_df['date'], y=filtered_df['sma_50'], name='50-Day SMA',
                   line=dict(color='purple', dash='dash')),
        secondary_y=False
    )

    # Add Sentiment trace on secondary y-axis
    fig.add_trace(
        go.Scatter(
            x=filtered_df['date'], y=filtered_df['avg_sentiment'], name='Avg Sentiment',
            mode='lines', line=dict(color='#1f77b4', width=2),
            hovertemplate="Date: %{x}<br>Sentiment: %{y:.2f}<extra></extra>"
        ),
        secondary_y=True,
    )

    # Add signal markers
    buy_signals = filtered_df[filtered_df['signal'] == 'Buy']
    sell_signals = filtered_df[filtered_df['signal'] == 'Sell']

    if not buy_signals.empty:
        fig.add_trace(
            go.Scatter(
                x=buy_signals['date'], y=buy_signals['close'], mode='markers',
                marker=dict(size=10, color='green', symbol='triangle-up'),
                name='Buy Signal',
                hovertemplate="Date: %{x}<br>Signal: Buy<extra></extra>"
            ),
            secondary_y=False
        )

    if not sell_signals.empty:
        fig.add_trace(
            go.Scatter(
                x=sell_signals['date'], y=sell_signals['close'], mode='markers',
                marker=dict(size=10, color='red', symbol='triangle-down'),
                name='Sell Signal',
                hovertemplate="Date: %{x}<br>Signal: Sell<extra></extra>"
            ),
            secondary_y=False
        )

    # Update layout
    fig.update_layout(
        title_text=f"Sentiment and Price Correlation for {selected_sector}",
        hovermode="x unified",
        xaxis_title="Date",
        yaxis_title="Price (Close)",
        yaxis2_title="Sentiment Score",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    # Set y-axis ranges
    fig.update_yaxes(range=[0, 1], secondary_y=True)  # Sentiment score is always 0 to 1

    return fig


def display_latest_news(news_df, selected_sector, num_articles=15):
    """Displays a table of the latest news articles for the selected sector."""
    st.subheader(f"Latest News for {selected_sector}")

    # Filter news to only include the selected sector and articles with content (from APIs)
    filtered_news_df = news_df[
        news_df['sectors_mentioned'].apply(lambda x: selected_sector in x if isinstance(x, list) else False)
    ].sort_values(by='publication_date', ascending=False).head(num_articles)

    if filtered_news_df.empty:
        st.info(f"No recent news articles with content found for {selected_sector}.")
        return

    # Use markdown to create a clickable link for the title
    def create_clickable_title(row):
        return f"[{row['title']}]({row['url']})"

    filtered_news_df['title_link'] = filtered_news_df.apply(create_clickable_title, axis=1)

    # Display in a table, showing key information
    st.dataframe(
        filtered_news_df[['title_link', 'source', 'publication_date', 'sentiment_score']],
        column_config={
            "title_link": st.column_config.Column("Headline", width="medium"),
            "source": st.column_config.Column("Source", width="small"),
            "publication_date": st.column_config.Column("Date", width="small", format="YYYY-MM-DD"),
            "sentiment_score": st.column_config.ProgressColumn("Sentiment", width="small", format="%.2f", min_value=0,
                                                               max_value=1)
        },
        hide_index=True
    )


# --- Main Application Logic ---
def main():
    st.title(PAGE_TITLE)

    # Get DB connections
    news_collection, insights_collection = get_db_collections()
    if news_collection is None or insights_collection is None:
        st.stop()

    # Fetch and process data
    news_df, insights_df, sectors = fetch_and_process_data(news_collection, insights_collection)
    if insights_df is None or sectors is None:
        st.warning("No insights data found. Please run the full pipeline to generate insights.")
        return

    # --- Sidebar for user input ---
    with st.sidebar:
        st.header("Settings")
        selected_sector = st.selectbox("Choose a Sector", options=sectors)

        # Date range slider
        min_date = insights_df['date'].min() if not insights_df.empty else dt_class.today().date()
        max_date = insights_df['date'].max() if not insights_df.empty else dt_class.today().date()
        date_range = st.slider(
            "Select Date Range",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="YYYY-MM-DD"
        )

        # Display latest signals as a summary in the sidebar
        st.subheader("Latest Signals")
        latest_signals = insights_df.sort_values(by='date', ascending=False).drop_duplicates('sector', keep='first')

        # Display signals in a table-like format
        for _, row in latest_signals.iterrows():
            signal = row['signal']
            color = 'green' if signal == 'Buy' else 'red' if signal == 'Sell' else 'orange'
            st.markdown(
                f"<div style='border-left: 5px solid {color}; padding-left: 10px; margin-bottom: 5px;'><b>{row['sector']}</b>: {signal}</div>",
                unsafe_allow_html=True
            )

    # --- Main Content Area ---
    if selected_sector:
        # Create and display the price/sentiment chart
        fig = create_sentiment_price_chart(insights_df, selected_sector, date_range)
        st.plotly_chart(fig, use_container_width=True)

        # Display latest news for the selected sector
        display_latest_news(news_df, selected_sector, num_articles=15)

    st.markdown("---")
    st.info(
        "This dashboard is for informational and educational purposes only and does not constitute financial advice. All data and signals are for analytical demonstration.")


if __name__ == "__main__":
    main()