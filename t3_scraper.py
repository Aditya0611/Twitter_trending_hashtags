import requests
from bs4 import BeautifulSoup
import re
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from textblob import TextBlob

# --- Step 1: Load Environment Variables ---
load_dotenv()
print("Attempting to load environment variables from .env file...")

# --- Step 2: Supabase Configuration ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("FATAL: Supabase URL and Key must be set in your .env file.")
else:
    print("Environment variables loaded successfully.")

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Supabase client initialized successfully.")
except Exception as e:
    raise RuntimeError(f"FATAL: Could not initialize Supabase client. Error: {e}")

# Generate a unique version_id for this scrape run
SCRAPE_VERSION_ID = str(uuid.uuid4())

def is_indian_text(text):
    """Checks if the text is likely related to India."""
    devanagari_pattern = re.compile(r'[\u0900-\u097F]')
    indian_terms = ['india', 'bharat', 'hindu', 'delhi', 'mumbai', 'bangalore', 'chennai', 'kolkata']
    
    if devanagari_pattern.search(text):
        return True
    
    text_lower = text.lower()
    if any(term in text_lower for term in indian_terms):
        return True
    
    return False

def generate_twitter_search_link(topic):
    """Generate a Twitter search link for the given topic."""
    return f"https://twitter.com/search?q={topic.replace('#', '%23')}"

def get_trending_topics():
    """Scrapes trending topics from trends24.in for India."""
    urls_to_try = [
        "https://trends24.in/india/",
        "http://trends24.in/india/",
        "https://www.trends24.in/india/"
    ]
    
    for url in urls_to_try:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            }
            
            print(f"\nTrying to fetch data from {url}...")
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            print("Successfully connected! Parsing HTML content...")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            trend_links = soup.find_all('a', class_='trend-link')
            
            if not trend_links:
                print("No trend links found. Trying next URL...")
                continue
            
            trending_topics = []
            seen_topics = set()  
            
            print(f"Found {len(trend_links)} potential trends. Filtering for unique Indian topics...")
            for link in trend_links:
                trend_text = link.get_text().strip()
                
                if trend_text in seen_topics:
                    continue
                
                if trend_text.startswith('#') and (is_indian_text(trend_text) or len(trending_topics) < 5):
                    tweet_count = "N/A"
                    
                    twitter_link = generate_twitter_search_link(trend_text)
                    
                    parent_li = link.find_parent('li')
                    if parent_li:
                        count_span = parent_li.find('span', class_='tweet-count')
                        if count_span and count_span.get_text().strip():
                            tweet_count = count_span.get_text().strip()
                    
                    trending_topics.append({
                        "topic": trend_text,
                        "count": tweet_count,
                        "twitter_link": twitter_link
                    })
                    seen_topics.add(trend_text) 
                    print(f"  -> Added: {trend_text} (Count: {tweet_count})")
                    
                    if len(trending_topics) >= 9:
                        break
            
            return trending_topics
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching trends: {str(e)}")
            continue
        except Exception as e:
            print(f"An unexpected error occurred during scraping: {str(e)}")
            continue
    
    print("All URLs failed. Returning empty list.")
    return []

def get_hashtag_post_content(hashtag):
    """Get sample post content for a hashtag."""
    try:
        hashtag_clean = hashtag.replace('#', '')
        
        sample_contents = {
            'election': f"Breaking: Major developments in {hashtag_clean}. Citizens actively participating in democratic process.",
            'flood': f"Emergency update on {hashtag_clean}. Relief operations underway, stay safe and follow official guidelines.",
            'dharma': f"Spiritual discourse on {hashtag_clean}. Ancient wisdom for modern times.",
            'football': f"Exciting match updates for {hashtag_clean}. Team performance analysis and fan reactions.",
            'bollywood': f"Latest entertainment news about {hashtag_clean}. Celebrity updates and movie reviews.",
            'batra': f"Tribute to heroes in {hashtag_clean}. Remembering courage and sacrifice for the nation.",
            'default': f"Trending discussion about {hashtag_clean}. Join the conversation and share your thoughts."
        }
        
        hashtag_lower = hashtag_clean.lower()
        for key, content in sample_contents.items():
            if key in hashtag_lower:
                return content
        
        return sample_contents['default']
        
    except Exception as e:
        print(f"Error generating content for {hashtag}: {e}")
        return f"Trending topic: {hashtag}. Join the discussion."

def analyze_hashtag_sentiment(hashtag):
    """Analyze the sentiment of a hashtag using TextBlob."""
    try:
        clean_text = hashtag.replace('#', '').replace('_', ' ')
        blob = TextBlob(clean_text)
        polarity = blob.sentiment.polarity
        
        print(f"DEBUG: Sentiment for '{hashtag}' -> polarity: {polarity}")
        
        # Determine sentiment label
        if polarity > 0.05:
            sentiment = "Positive"
        elif polarity < -0.05:
            sentiment = "Negative"
        else:
            sentiment = "Neutral"
            
        print(f"DEBUG: Final sentiment: {sentiment}")
        return sentiment, polarity
        
    except Exception as e:
        print(f"ERROR analyzing sentiment for {hashtag}: {e}")
        return "Neutral", 0.0

def calculate_engagement_score(topic_data):
    """Calculate engagement score for a trending topic (1-10 scale)."""
    try:
        topic = topic_data.get("topic", "")
        count_str = topic_data.get("count", "N/A")
        
        engagement_score = 1.0
        
        if count_str != "N/A" and count_str:
            try:
                count_clean = count_str.replace('K', '000').replace('M', '000000').replace(',', '')
                count_clean = ''.join(filter(str.isdigit, count_clean))
                if count_clean:
                    tweet_count = int(count_clean)
                    if tweet_count > 0:
                        engagement_score += min(5, max(0, (tweet_count / 10000) * 2))
            except:
                pass
        
        topic_lower = topic.lower()
        
        trending_keywords = ['election', 'breaking', 'urgent', 'live', 'update', 'news']
        if any(keyword in topic_lower for keyword in trending_keywords):
            engagement_score += 1.5
        
        indian_keywords = ['india', 'indian', 'bharath', 'delhi', 'mumbai', 'modi', 'bjp', 'congress']
        if any(keyword in topic_lower for keyword in indian_keywords):
            engagement_score += 1.0
        
        if len(topic) > 15:
            engagement_score += 0.5
        
        if any(char in topic for char in ['2024', '2023', '!', '@']):
            engagement_score += 0.5
        
        engagement_score = max(1, min(10, round(engagement_score, 2)))
        return float(engagement_score)
        
    except Exception as e:
        print(f"Error calculating engagement score: {e}")
        return 1.0

def parse_post_count(count_str):
    """Convert count string like '25K' or '2.1M' to actual number."""
    if count_str == "N/A" or not count_str:
        return 0
    
    try:
        count_clean = count_str.replace(',', '')
        
        if 'M' in count_clean.upper():
            return int(float(count_clean.upper().replace('M', '')) * 1000000)
        elif 'K' in count_clean.upper():
            return int(float(count_clean.upper().replace('K', '')) * 1000)
        else:
            return int(''.join(filter(str.isdigit, count_clean)))
    except:
        return 0

def clear_all_supabase_data():
    """Clear all existing data from twitter table for Twitter platform."""
    try:
        print("🗑️  CLEARING ALL EXISTING TWITTER DATA from twitter table...")
        
        # Delete only Twitter platform records
        result = supabase.table('twitter').delete().eq('platform', 'Twitter').execute()
        
        print("✅ ALL PREVIOUS TWITTER DATA CLEARED from twitter table.")
        print("📊 Ready to insert fresh data.")
        
    except Exception as e:
        print(f"❌ ERROR clearing data: {e}")

def insert_fresh_data_only(topics_list):
    """Insert only fresh trending topics data after clearing existing data."""
    if not topics_list:
        print("No topics to store in Supabase.")
        return
    
    # Step 1: Clear existing Twitter data
    clear_all_supabase_data()
    
    try:
        # Step 2: Process and insert fresh data
        processed_topics = []
        print(f"\n📥 PROCESSING {len(topics_list)} FRESH TOPICS:")
        
        for i, topic in enumerate(topics_list):
            print(f"  {i+1}. Processing: {topic['topic']}")
            
            # Calculate engagement score
            engagement_score = calculate_engagement_score(topic)
            
            # Calculate sentiment
            sentiment_label, sentiment_polarity = analyze_hashtag_sentiment(topic["topic"])
            
            # Get Twitter link
            twitter_link = topic.get("twitter_link", generate_twitter_search_link(topic["topic"]))
            
            # Get post content
            post_content = get_hashtag_post_content(topic["topic"])
            
            # Parse post count
            posts_count = parse_post_count(topic["count"])
            
            # Prepare metadata
            metadata = {
                "twitter_link": twitter_link,
                "post_content": post_content,
                "raw_count": topic["count"]
            }
            
            # Create record matching new schema
            processed_topic = {
                "platform": "Twitter",
                "topic_hashtag": topic["topic"],
                "engagement_score": float(engagement_score),
                "sentiment_polarity": float(sentiment_polarity),
                "sentiment_label": str(sentiment_label),
                "posts": int(posts_count),
                "views": 0,  # Twitter doesn't provide view counts
                "metadata": metadata,
                "version_id": SCRAPE_VERSION_ID
            }
            
            processed_topics.append(processed_topic)
            print(f"     ✓ Engagement: {engagement_score} - Sentiment: {sentiment_label} ({sentiment_polarity})")
            print(f"     ✓ Posts: {posts_count}")
        
        # Step 3: Insert fresh data
        print(f"\n💾 INSERTING {len(processed_topics)} FRESH RECORDS...")
        data, count = supabase.table('twitter').insert(processed_topics).execute()
        
        if data and len(data[1]) > 0:
            print(f"🎉 SUCCESS: {len(data[1])} fresh records inserted!")
            print(f"📋 Scrape Version ID: {SCRAPE_VERSION_ID}")
            print("📋 Your Supabase now contains ONLY current trending topics.")
        else:
            print("⚠️  WARNING: Data insertion may have failed.")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")

def main():
    """Main function to orchestrate the scraping and storing process."""
    print("\n--- Starting Twitter Trend Scraper ---")
    print(f"Scrape Version ID: {SCRAPE_VERSION_ID}")
    
    trending_topics = get_trending_topics()
    
    if not trending_topics:
        print("\nNo trending Indian hashtags found from trends24.in.")
        print("This could be due to network issues or site unavailability.")
    else:
        print(f"\nSuccessfully found {len(trending_topics)} unique trending Indian hashtags!")
        
        insert_fresh_data_only(trending_topics)

if __name__ == "__main__":
    main()