import pandas as pd
import os
import sys
from dotenv import load_dotenv
import requests
import json
import warnings
import time
from google import genai
from google.genai import types
from datetime  import datetime
warnings.filterwarnings("ignore")
from prompt import SYSTEM_PROMPT

#connfig  and env loading
load_dotenv()
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL="gemini-3-flash-preview"

TARGET_CITIES = ["Mumbai", "Bangalore", "Chennai"] 

INPUT_FILE = r"C:\Users\Ingit.Paul.in\Desktop\llm_googel review\place_ids.csv"
OUTPUT_FILE =r"C:\Users\Ingit.Paul.in\Desktop\llm_googel review\output1.csv"

#helper functions

def fetch_reviews(place_id, api_key):
    """Fetches reviews from Google Places API v1."""
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "displayName,reviews"
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if "reviews" in data:
            return data["reviews"]
        else:
            print(f"No reviews found or API error for {place_id}: {data}")
            return []
    except Exception as e:
        print(f"Error fetching reviews for {place_id}: {e}")
        return []

def chunk_reviews(reviews, size=5):
    """Yields successive n-sized chunks from reviews."""
    for i in range(0, len(reviews), size):
        yield reviews[i:i + size]

def build_prompt(reviews_batch):
    """the prompt for Gemini."""
    reviews_text = []
    for r in reviews_batch:
        text_content = r.get("text", {}).get("text") if isinstance(r.get("text"), dict) else r.get("text")
        author_name = r.get("authorAttribution", {}).get("displayName") if "authorAttribution" in r else r.get("author_name")
        relative_time = r.get("relativePublishTimeDescription") or r.get("relative_time_description")
        
        reviews_text.append({
            "author_name": author_name,
            "rating": r.get("rating"),
            "text": text_content,
            "relative_time_description": relative_time
        })
    
    return f"{SYSTEM_PROMPT}\n\nReviews Data:\n{json.dumps(reviews_text, indent=2)}"

def analyze_with_gemini(reviews_batch, api_key):
    """Sends a batch of reviews to Gemini for analysis."""
    client = genai.Client(api_key=api_key)
    
    prompt = build_prompt(reviews_batch)
    
    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=prompt
        )
        content = response.text.strip()
        
        # Clean up markdown code blocks if present
        if content.startswith("```json"):
            content = content[7:]
        if content.startswith("```"):
            content = content[3:]
        if content.endswith("```"):
            content = content[:-3]
            
        return json.loads(content.strip())
    except Exception as e:
        print(f"Error analyzing batch with Gemini: {e}")
        return []

def main():
    # Validation
    if not GOOGLE_MAPS_API_KEY or not GEMINI_API_KEY:
        print("Error: API keys not found in environment variables.")
        return 

    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file '{INPUT_FILE}' not found. Please create it with 'place_id', 'city', and 'office_name' columns.")
        return
    try:
        places_df = pd.read_csv(INPUT_FILE)
        # Normalize column names to lowercase 
        places_df.columns = places_df.columns.str.lower().str.strip()
        
        if 'place_id' not in places_df.columns:
             print(f"Error: Column 'place_id' not found in '{INPUT_FILE}'.")
             return
        if 'city' not in places_df.columns:
             print(f"Error: Column 'city' not found in '{INPUT_FILE}'.")
             return
        if 'office_name' not in places_df.columns:
             print(f"Error: Column 'office_name' not found in '{INPUT_FILE}'.")
             return
             
        # City filters
        unique_cities = places_df['city'].dropna().unique()
        
        global TARGET_CITIES # Ensure we use the global variable if modified
        if not TARGET_CITIES:
            print("\nAvailable cities found in CSV:")
            for city in unique_cities:
                print(f"- {city}")
            
            # Interactive input if not in env
            selected_input = input("\nEnter cities to analyze (comma-separated, or press Enter for all): ").strip()
            if selected_input:
                TARGET_CITIES = [c.strip() for c in selected_input.split(',')]
        
        if TARGET_CITIES:
            print(f"Filtering for cities: {TARGET_CITIES}")
            target_cities_lower = [c.lower() for c in TARGET_CITIES]
            places_df = places_df[places_df['city'].str.lower().isin(target_cities_lower)]
            
            if places_df.empty:
                print(f"No places found for cities: {TARGET_CITIES}")
                return
        else:
            print("No cities selected. Processing all cities.")

    except Exception as e:
        print(f"Error reading '{INPUT_FILE}': {e}")
        return

    all_places_data = []

    print(f"Found {len(places_df)} places to process in {TARGET_CITIES}.")

    for index, row in places_df.iterrows():
        place_id = row['place_id']
        office_name = row.get('office_name', 'Unknown Branch')
        
        print(f"\n--- Processing Place: {office_name} (City: {row.get('city', 'Unknown')}) ({index + 1}/{len(places_df)}) ---")
        
        reviews = fetch_reviews(place_id, GOOGLE_MAPS_API_KEY)
        
        if not reviews:
            print(f"No reviews found for {place_id}. Skipping.")
            continue
            
        print(f"Fetched {len(reviews)} reviews.")
        
        place_results = []
        print("Starting Gemini analysis...")
        for i, batch in enumerate(chunk_reviews(reviews)):
            batch_results = analyze_with_gemini(batch, GEMINI_API_KEY)
            
            if batch_results:
                for review_data, analysis_result in zip(batch, batch_results):
                    author_name = review_data.get("authorAttribution", {}).get("displayName") if "authorAttribution" in review_data else review_data.get("author_name", "Anonymous")
                    #time extraction
                    text_content = review_data.get("text", {}).get("text") if isinstance(review_data.get("text"), dict) else review_data.get("text")
                    
                    publish_time = review_data.get("publishTime")
                    
                    formatted_time = publish_time
                    if publish_time:
                        try:
                            dt = datetime.strptime(publish_time, "%Y-%m-%dT%H:%M:%SZ")
                            formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            pass # Keep original if parsing fails

                    analysis_result['who_gave_the_review'] = author_name
                    analysis_result['review_text'] = text_content
                    analysis_result['review_date'] = formatted_time
                
                place_results.extend(batch_results)
            time.sleep(1.2) 
        for result in place_results:
            result['place_id'] = place_id
            result['branch_name'] = office_name 
            result['city'] = row.get('city')
        
        all_places_data.extend(place_results)
        print(f"Finished processing {place_id}.")

    if all_places_data:
        df = pd.DataFrame(all_places_data)

        desired_columns = [
            'city', 
            'branch_name',
            'review_date',
            'review_text',
            'who_gave_the_review',
            'overall_sentiment', 
            'sentiment_score', 
            'primary_issue', 
            'severity', 
            'summary']      
         

        for col in desired_columns:
            if col not in df.columns:
                df[col] = None
                
        # Select only the desired columns
        final_df = df[desired_columns]
        
        print("\nAnalysis Complete. Saving results...")
        try:
            final_df.to_csv(OUTPUT_FILE, index=False)
            print(f"Results saved to '{OUTPUT_FILE}'.")
            print(final_df.head())
        except Exception as e:
            print(f"Error saving results: {e}")
            print(final_df)
    else:
        print("Analysis yielded no results across all places.")

if __name__ == "__main__":
    main()
