import pandas as pd
from pymongo import MongoClient
import numpy as np
from datetime import datetime, timedelta
import logging
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import spacy
from spacy import displacy

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- MongoDB Connection ---
def connect_to_mongodb():
    # ... (Your existing MongoDB connection code here)
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        return client
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        return None


# --- Main Insights Generator ---
def generate_insights():
    logging.info("--- Phase 5: Generating Correlation and Insights ---")
    client = connect_to_mongodb()
    if not client:
        return

    db = client['indian_market_scanner_db']
    news_collection = db['news_articles']
    market_collection = db['historical_market_data']
    insights_collection = db['insights']

    # Fetch news and market data
    try:
        news_df = pd.DataFrame(list(news_collection.find({})))
        market_df = pd.DataFrame(list(market_collection.find({})))

        # --- CRITICAL FIX: Handle the '_id' column ---
        if not news_df.empty:
            news_df = news_df.drop(columns=['_id'], errors='ignore')
        if not market_df.empty:
            market_df = market_df.drop(columns=['_id'], errors='ignore')

    except Exception as e:
        logging.error(f"An unexpected error occurred during insights generation: {e}")
        return

    if news_df.empty or market_df.empty:
        logging.warning("News or market data is empty. Cannot generate insights.")
        return

    # Data Processing (rest of your existing code)
    # ... (Your existing data processing and insight generation code goes here)

    # --- Example: Your old code for the ambiguity error fix ---
    # insights_df['avg_sentiment'].fillna(0.5, inplace=True)
    # insights_df['num_articles'].fillna(0, inplace=True)

    # --- Corrected code to avoid FutureWarning and the ambiguous error ---
    # insights_df['avg_sentiment'] = insights_df['avg_sentiment'].fillna(0.5)
    # insights_df['num_articles'] = insights_df['num_articles'].fillna(0)

    # ... (Your existing data processing and insight generation code goes here)

# ... (rest of your insights_generator.py script)