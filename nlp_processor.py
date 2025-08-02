import spacy
from collections import defaultdict
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from scipy.special import softmax
import numpy as np
import time

# --- Global FinBERT Model Variables ---
finbert_tokenizer = None
finbert_model = None
finbert_labels = ['negative', 'neutral', 'positive']

# --- Global spaCy Model Variable ---
nlp_spacy = None

# --- Sector Keywords Mapping (keep as is) ---
SECTOR_KEYWORDS = {
    'bank': 'Banking & Financial Services', 'finance': 'Banking & Financial Services',
    'nbfc': 'Banking & Financial Services', 'fincorp': 'Banking & Financial Services',
    'it': 'Information Technology', 'software': 'Information Technology', 'tech': 'Information Technology',
    'technology': 'Information Technology', 'tcs': 'Information Technology', 'infosys': 'Information Technology',
    'wipro': 'Information Technology',
    'pharma': 'Healthcare & Pharma', 'healthcare': 'Healthcare & Pharma', 'hospital': 'Healthcare & Pharma',
    'dr reddy': 'Healthcare & Pharma', 'sun pharma': 'Healthcare & Pharma',
    'energy': 'Energy', 'oil': 'Energy', 'gas': 'Energy', 'power': 'Energy', 'reliance industries': 'Energy',
    'adani green': 'Energy',
    'auto': 'Automobile', 'automobile': 'Automobile', 'tata motors': 'Automobile', 'mahindra': 'Automobile',
    'maruti suzuki': 'Automobile',
    'telecom': 'Telecommunication', 'airtel': 'Telecommunication', 'jio': 'Telecommunication',
    'vodafone': 'Telecommunication',
    'infra': 'Infrastructure', 'construction': 'Infrastructure', 'cement': 'Infrastructure', 'l&t': 'Infrastructure',
    'metal': 'Metals & Mining', 'steel': 'Metals & Mining', 'mining': 'Metals & Mining',
    'tata steel': 'Metals & Mining',
    'fmcg': 'FMCG', 'consumer': 'FMCG', 'hul': 'FMCG', 'itc': 'FMCG',
    'real estate': 'Real Estate', 'property': 'Real Estate', 'dlf': 'Real Estate',
    'media': 'Media & Entertainment', 'entertainment': 'Media & Entertainment',
    'chemical': 'Chemicals', 'paint': 'Chemicals',
    'capital goods': 'Capital Goods',
    'textile': 'Textiles',
    'logistics': 'Logistics',
    'psu': 'Public Sector Undertakings'
}


# --- FinBERT Sentiment Analysis Functions ---

def load_finbert_model():
    """
    Loads the pre-trained FinBERT tokenizer and model.
    Loads only once to save memory and time.
    """
    global finbert_tokenizer, finbert_model

    if finbert_tokenizer is None or finbert_model is None:
        print("Loading FinBERT model and tokenizer. This may take a moment...")
        model_name = "ProsusAI/finbert"
        try:
            finbert_tokenizer = AutoTokenizer.from_pretrained(model_name)
            finbert_model = AutoModelForSequenceClassification.from_pretrained(model_name)
            # --- DEBUG PRINT ---
            print("DEBUG: FinBERT model loaded successfully into memory.")
            # --- END DEBUG ---
        except Exception as e:
            print(f"Error loading FinBERT model: {e}")
            finbert_tokenizer = None
            finbert_model = None
            # --- DEBUG PRINT ---
            print("DEBUG: FinBERT model failed to load.")
            # --- END DEBUG ---


def get_sentiment_score(text):
    """
    Analyzes the sentiment of the given text using the loaded FinBERT model.
    Returns a sentiment score (e.g., probability of positive sentiment) or None on error.
    """
    if finbert_tokenizer is None or finbert_model is None:
        print("FinBERT model not loaded. Cannot perform sentiment analysis.")
        return None

    try:
        # --- DEBUG PRINT ---
        print(f"DEBUG: Processing text for sentiment: '{text[:100]}...'")
        # --- END DEBUG ---
        inputs = finbert_tokenizer(text, return_tensors='pt', padding=True, truncation=True, max_length=512)
        outputs = finbert_model(**inputs)
        scores = softmax(outputs.logits.detach().numpy())[0]

        positive_prob = scores[finbert_labels.index('positive')]
        # --- DEBUG PRINT ---
        print(f"DEBUG: Sentiment score calculated: {float(positive_prob):.4f} for text starting: '{text[:50]}...'")
        # --- END DEBUG ---
        return float(positive_prob)

    except Exception as e:
        print(f"Error during sentiment analysis for text (first 100 chars: '{text[:100]}...'): {e}")
        # --- DEBUG PRINT ---
        print("DEBUG: get_sentiment_score returning None due to error.")
        # --- END DEBUG ---
        return None


# --- spaCy Entity and Sector Recognition Functions (keep as is) ---

def load_spacy_model():
    """
    Loads the spaCy English model for NER. Loads only once.
    """
    global nlp_spacy
    if nlp_spacy is None:
        print("Loading spaCy 'en_core_web_sm' model. This may take a moment...")
        try:
            nlp_spacy = spacy.load("en_core_web_sm")
            print("spaCy model loaded successfully.")
        except OSError:
            print("spaCy model 'en_core_web_sm' not found. Please run: python -m spacy download en_core_web_sm")
            nlp_spacy = None
        except Exception as e:
            print(f"Error loading spaCy model: {e}")
            nlp_spacy = None


def extract_companies_and_sectors(text):
    """
    Extracts potential company names (ORG entities) and infers sectors from text.
    Returns a list of unique company names and unique sector names.
    """
    if nlp_spacy is None:
        print("spaCy model not loaded. Cannot perform entity extraction.")
        return [], []

    doc = nlp_spacy(text)
    companies = set()
    sectors = set()

    for ent in doc.ents:
        if ent.label_ == "ORG":
            if len(ent.text.split()) > 1 and len(ent.text) > 3 and not any(
                    word in ent.text.lower() for word in ['ltd', 'inc', 'corp', 'group', 'india', 'pvt', 'public']):
                companies.add(ent.text.strip())
            elif any(suffix in ent.text.lower() for suffix in
                     ['ltd', 'inc', 'corp', 'group', 'industries', 'bank', 'solutions', 'tech']):
                companies.add(ent.text.strip())

    text_lower = text.lower()
    for keyword, sector_name in SECTOR_KEYWORDS.items():
        if keyword in text_lower:
            sectors.add(sector_name)

    return list(companies), list(sectors)


# --- Processing and Update Functions ---

def process_and_update_sentiment(mongo_collection):
    """
    Fetches articles from MongoDB, performs sentiment analysis,
    and updates the documents with the sentiment scores.
    """
    print("\nStarting sentiment analysis and database update...")

    if mongo_collection is None:
        print("MongoDB collection not provided for sentiment analysis. Aborting.")
        return

    load_finbert_model()  # Ensure FinBERT model is loaded

    if finbert_model is None:  # This check relies on the global variable being set
        print("FinBERT model failed to load. Cannot perform sentiment analysis.")
        return

    # Fetch articles that do not yet have a sentiment_score
    query = {
        "sentiment_score": None,
        "content": {"$ne": "Failed to scrape full content from article page"}
    }

    articles_to_process = mongo_collection.find(query)

    processed_count = 0
    updated_count = 0

    for article in articles_to_process:
        article_id = article['_id']
        article_url = article.get('url', 'N/A')
        article_content = article.get('content', '')

        if not article_content or len(article_content) < 50:
            print(f"Skipping sentiment analysis for article {article_url} due to insufficient content.")
            continue

        print(f"Analyzing sentiment for: {article.get('title', article_url)[:70]}...")

        sentiment_score = get_sentiment_score(article_content)

        if sentiment_score is not None:
            update_result = mongo_collection.update_one(
                {'_id': article_id},
                {'$set': {'sentiment_score': sentiment_score}}
            )
            if update_result.modified_count > 0:
                print(f"Updated sentiment for {article_url}: {sentiment_score:.4f}")
                updated_count += 1
            else:
                # --- DEBUG PRINT ---
                print(
                    f"DEBUG: MongoDB update_one for sentiment for {article_url} resulted in no modification. Matched: {update_result.matched_count}, Modified: {update_result.modified_count}")
                # --- END DEBUG ---
        else:
            print(f"Could not get sentiment for {article_url}.")

        processed_count += 1
        time.sleep(0.1)

    print(f"\nSentiment analysis complete. Processed {processed_count} articles, updated {updated_count} documents.")


def process_and_update_entities(mongo_collection):
    """
    Fetches articles from MongoDB that lack 'companies_mentioned' or 'sectors_mentioned',
    performs entity/sector recognition, and updates the documents.
    """
    print("\nStarting entity and sector recognition and database update...")

    if mongo_collection is None:
        print("MongoDB collection not provided for entity recognition. Aborting.")
        return

    load_spacy_model()

    if nlp_spacy is None:
        print("spaCy model failed to load. Cannot perform entity recognition.")
        return

    query = {
        "$or": [
            {"companies_mentioned": {"$exists": False}},
            {"companies_mentioned": []},
            {"sectors_mentioned": {"$exists": False}},
            {"sectors_mentioned": []}
        ],
        "content": {"$ne": "Failed to scrape full content from article page"}
    }

    articles_to_process = mongo_collection.find(query)

    processed_count = 0
    updated_count = 0

    for article in articles_to_process:
        article_id = article['_id']
        article_url = article.get('url', 'N/A')
        article_content = article.get('content', '')

        if not article_content or len(article_content) < 100:
            print(f"Skipping entity recognition for article {article_url} due to insufficient content.")
            continue

        print(f"Extracting entities for: {article.get('title', article_url)[:70]}...")

        companies, sectors = extract_companies_and_sectors(article_content)

        update_fields = {}
        if companies:
            update_fields['companies_mentioned'] = companies
        if sectors:
            update_fields['sectors_mentioned'] = sectors

        if update_fields:
            update_result = mongo_collection.update_one(
                {'_id': article_id},
                {'$set': update_fields}
            )
            if update_result.modified_count > 0:
                print(f"Updated entities/sectors for {article_url}: Companies={len(companies)}, Sectors={len(sectors)}")
                updated_count += 1
        else:
            print(f"No significant companies or sectors found for {article_url}.")

        processed_count += 1
        time.sleep(0.1)

    print(
        f"\nEntity/Sector recognition complete. Processed {processed_count} articles, updated {updated_count} documents.")