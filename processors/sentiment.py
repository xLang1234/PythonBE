import nltk
from textblob import TextBlob
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import re
from loguru import logger
import pandas as pd
import asyncio
import aiohttp
import os
from dotenv import load_dotenv
import json
import statistics

from database.postgres import get_db, RawContent, ProcessedContent

# Load environment variables from .env file if it exists
load_dotenv()

# Get API key from environment variable
API_KEY = os.environ.get('OPENROUTER_API_KEY')
if not API_KEY:
    logger.warning("OPENROUTER_API_KEY environment variable is not set. Advanced sentiment analysis will be disabled.")

API_URL = 'https://openrouter.ai/api/v1/chat/completions'

# Download necessary NLTK data
try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
except Exception as e:
    logger.error(f"Error downloading NLTK data: {str(e)}")

class SentimentAnalyzer:
    def __init__(self):
        self.crypto_keywords = {
            'bitcoin': ['bitcoin', 'btc', 'satoshi', 'sats', 'bitcoin core'],
            'ethereum': ['ethereum', 'eth', 'vitalik', 'buterin', 'ether', 'evm'],
            'binance': ['binance', 'bnb', 'cz', 'changpeng', 'zhao'],
            'coinbase': ['coinbase', 'cb', 'brian', 'armstrong', 'cbpro'],
            'xrp': ['xrp', 'ripple', 'garlinghouse', 'brad', 'sec lawsuit'],
            'solana': ['solana', 'sol', 'anatoly', 'yakovenko', 'solend'],
            'cardano': ['cardano', 'ada', 'hoskinson', 'charles', 'iohk'],
            'dogecoin': ['dogecoin', 'doge', 'shiba', 'musk', 'elon'],
            'defi': ['defi', 'yield farming', 'liquidity', 'swap', 'amm', 'dex'],
            'nft': ['nft', 'non-fungible', 'opensea', 'bored ape', 'cryptopunks']
        }
        self.positive_words = set([
            'bullish', 'buy', 'moon', 'skyrocket', 'soar', 'profit', 'gain', 'success', 'rally', 'surge',
            'breakthrough', 'confident', 'partnership', 'adopt', 'adoption', 'launch', 'release',
            'positive', 'good', 'great', 'excellent', 'amazing', 'impressive', 'record', 'high',
            'opportunity', 'potential', 'growth', 'innovation', 'solution', 'upgrade', 'improve'
        ])
        self.negative_words = set([
            'bearish', 'sell', 'crash', 'plummet', 'dump', 'loss', 'scam', 'hack', 'failure', 'dip',
            'decline', 'drop', 'investigation', 'regulation', 'ban', 'illegal', 'fraud', 'lawsuit',
            'negative', 'bad', 'poor', 'terrible', 'disappointing', 'record', 'low', 'risk',
            'problem', 'issue', 'bug', 'vulnerability', 'exploit', 'delay', 'postpone'
        ])
        self.stop_words = set(stopwords.words('english'))
        
        # OpenRouter settings
        self.use_ai = API_KEY is not None
        # List of models to use for sentiment analysis
        self.models = [
            "deepseek/deepseek-chat:free",
            "anthropic/claude-3-haiku:free",
            "mistralai/mistral-7b-instruct:free",
            "meta-llama/llama-3-8b-instruct:free"
        ]
        # Number of models to use per analysis (can be adjusted)
        self.models_per_analysis = 2
        
    def clean_text(self, text):
        """Clean and prepare text for analysis"""
        # Remove URLs
        text = re.sub(r'http\S+', '', text)
        # Remove mentions
        text = re.sub(r'@\w+', '', text)
        # Remove hashtags
        text = re.sub(r'#\w+', '', text)
        # Remove special characters
        text = re.sub(r'[^\w\s]', '', text)
        # Convert to lowercase
        text = text.lower()
        return text
    
    def analyze_sentiment(self, text):
        """Analyze sentiment of a text"""
        cleaned_text = self.clean_text(text)
        blob = TextBlob(cleaned_text)
        
        # Get sentiment polarity (-1 to 1)
        sentiment_score = blob.sentiment.polarity
        
        # Custom crypto-specific sentiment adjustment
        tokens = word_tokenize(cleaned_text)
        tokens = [w for w in tokens if w not in self.stop_words]
        
        # Adjust sentiment based on crypto-specific terms
        positive_count = sum(1 for word in tokens if word in self.positive_words)
        negative_count = sum(1 for word in tokens if word in self.negative_words)
        
        # Simple adjustment formula
        adjustment = (positive_count - negative_count) * 0.1
        adjusted_score = max(-1.0, min(1.0, sentiment_score + adjustment))
        
        return adjusted_score
    
    async def query_model(self, session, model_name, text):
        """Query a specific model for sentiment analysis"""
        # Define the prompt template for crypto tweet analysis
        prompt = f"""Evaluate the newsworthiness of the following crypto-related tweet on a scale from 1 to 10, 
where 10 is extremely newsworthy and 1 is not newsworthy. If it's not crypto-related, rate it 0.
Respond in JSON format with two fields only:
{{
  "score": [integer 0-10],
  "tags": [array of relevant crypto tags]
}}

Tweet: "{text}"
"""

        headers = {
            'Authorization': f'Bearer {API_KEY}',
            'Content-Type': 'application/json'
        }
        
        data = {
            "model": model_name,
            "messages": [{"role": "user", "content": prompt}]
        }
        
        try:
            async with session.post(API_URL, json=data, headers=headers) as response:
                if response.status == 200:
                    response_json = await response.json()
                    content = response_json['choices'][0]['message']['content']
                    
                    # Try to parse the JSON response
                    try:
                        result = json.loads(content)
                        score = result.get("score", 5)  # Default to 5 if parsing fails
                        tags = result.get("tags", [])
                        
                        # Normalize score to -1 to 1 range for sentiment
                        if score == 0:  # Not crypto-related
                            normalized_score = 0  # Neutral for non-crypto
                        else:
                            normalized_score = ((score / 10) * 2) - 1  # Convert 1-10 to -1 to 1
                            
                        return {
                            "model": model_name,
                            "score": normalized_score,
                            "tags": tags,
                            "status": "success"
                        }
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse AI response from {model_name}: {content}")
                        return {
                            "model": model_name,
                            "score": None,
                            "tags": [],
                            "status": "parse_error"
                        }
                else:
                    logger.error(f"Error from OpenRouter API for {model_name}: {response.status}")
                    return {
                        "model": model_name,
                        "score": None,
                        "tags": [],
                        "status": "api_error"
                    }
        except Exception as e:
            logger.error(f"Exception in AI analysis with {model_name}: {str(e)}")
            return {
                "model": model_name,
                "score": None,
                "tags": [],
                "status": "exception"
            }
    
    async def analyze_with_ai(self, text):
        """Use multiple AI models via OpenRouter to analyze crypto tweet"""
        if not self.use_ai:
            return None, []
        
        # Select a subset of models to use for this analysis
        # This helps to distribute the load and reduce API costs
        # It also provides redundancy in case one model fails
        import random
        models_to_use = random.sample(self.models, min(self.models_per_analysis, len(self.models)))
        
        logger.debug(f"Using models for analysis: {models_to_use}")
        
        try:
            async with aiohttp.ClientSession() as session:
                tasks = []
                for model in models_to_use:
                    task = self.query_model(session, model, text)
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks)
                
                # Filter out failed results
                valid_results = [r for r in results if r["score"] is not None]
                
                if not valid_results:
                    logger.warning(f"All AI models failed to analyze: {text[:50]}...")
                    return None, []
                
                # Aggregate results
                scores = [r["score"] for r in valid_results]
                all_tags = []
                for r in valid_results:
                    all_tags.extend(r["tags"])
                
                # Calculate median score for robustness
                median_score = statistics.median(scores)
                
                # Count tag frequency
                tag_counts = {}
                for tag in all_tags:
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
                
                # Keep tags mentioned by multiple models or by a single model if only one succeeded
                threshold = 1 if len(valid_results) == 1 else 2
                consensus_tags = [tag for tag, count in tag_counts.items() if count >= threshold]
                
                return median_score, consensus_tags
                
        except Exception as e:
            logger.error(f"Exception in multi-model AI analysis: {str(e)}")
            return None, []
    
    def extract_keywords(self, text):
        """Extract keywords from text"""
        cleaned_text = self.clean_text(text)
        tokens = word_tokenize(cleaned_text)
        tokens = [w for w in tokens if w not in self.stop_words and len(w) > 2]
        
        # Get frequency distribution
        freq_dist = nltk.FreqDist(tokens)
        
        # Get top keywords
        keywords = [word for word, freq in freq_dist.most_common(10)]
        return keywords
    
    def extract_entities_mentioned(self, text):
        """Extract cryptocurrency entities mentioned in text"""
        cleaned_text = self.clean_text(text)
        entities = []
        
        for entity, keywords in self.crypto_keywords.items():
            if any(keyword in cleaned_text for keyword in keywords):
                entities.append(entity)
                
        return entities
    
    def calculate_impact_score(self, sentiment_score, engagement_metrics):
        """Calculate potential market impact score"""
        # Simple formula: sentiment strength * engagement reach
        if not engagement_metrics:
            return abs(sentiment_score) * 0.5
            
        # Calculate engagement reach
        likes = engagement_metrics.get('likes', 0)
        retweets = engagement_metrics.get('retweets', 0) * 2  # Retweets have more impact
        replies = engagement_metrics.get('replies', 0) * 1.5
        quotes = engagement_metrics.get('quotes', 0) * 1.8
        
        engagement_reach = (likes + retweets + replies + quotes) / 100
        
        # Cap the reach component
        engagement_factor = min(5.0, engagement_reach)
        
        # Calculate impact: sentiment strength * engagement reach
        impact = abs(sentiment_score) * (1 + engagement_factor)
        
        # Scale to 0-1 range
        scaled_impact = min(1.0, impact / 5.0)
        
        return scaled_impact
    
    def categorize_content(self, text, entities_mentioned, ai_tags=None):
        """Categorize content into topics, optionally using AI-generated tags"""
        cleaned_text = self.clean_text(text)
        categories = []
        
        # Use AI tags if available
        if ai_tags and len(ai_tags) > 0:
            return ai_tags
        
        # Define category keywords
        category_keywords = {
            "market": ["price", "market", "trading", "buy", "sell", "bull", "bear", "trend"],
            "technology": ["protocol", "blockchain", "code", "update", "fork", "layer", "smart contract"],
            "regulation": ["sec", "regulation", "law", "legal", "compliance", "government", "policy"],
            "security": ["hack", "exploit", "security", "vulnerability", "attack", "breach", "scam"],
            "adoption": ["adoption", "partnership", "integration", "mainstream", "institutional"],
            "defi": ["defi", "yield", "farming", "liquidity", "pool", "amm", "dex", "swap"],
            "nft": ["nft", "collectible", "art", "auction", "mint", "sale"]
        }
        
        for category, keywords in category_keywords.items():
            if any(keyword in cleaned_text for keyword in keywords):
                categories.append(category)
        
        # Add mentioned crypto entities as categories
        for entity in entities_mentioned:
            if entity not in categories and entity not in ["defi", "nft"]:  # Avoid duplicates
                categories.append(entity)
                
        return categories if categories else ["general"]
    
    async def process_unprocessed_content(self, limit=100):
        """Process unprocessed content from the database"""
        db = next(get_db())
        try:
            # Get unprocessed content - only English content
            unprocessed = db.query(RawContent).outerjoin(
                ProcessedContent, 
                RawContent.id == ProcessedContent.raw_content_id
            ).filter(
                ProcessedContent.id == None,
                # Only process English content or content where language is not specified
                (RawContent.language == 'en') | (RawContent.language == None) | (RawContent.language == 'unknown')
            ).limit(limit).all()
            
            logger.info(f"Found {len(unprocessed)} unprocessed English content items")
            
            processed_count = 0
            tasks = []
            content_items = []
            
            # Create tasks for AI analysis if enabled
            if self.use_ai:
                for content in unprocessed:
                    tasks.append(self.analyze_with_ai(content.content))
                    content_items.append(content)
                
                # Run all AI analyses concurrently
                if tasks:
                    logger.info(f"Running multi-model AI analysis on {len(tasks)} content items")
                    ai_results = await asyncio.gather(*tasks)
                else:
                    ai_results = []
            else:
                ai_results = [(None, []) for _ in unprocessed]
                content_items = unprocessed
            
            # Process contents with AI results
            for i, content in enumerate(content_items):
                try:
                    # Get AI results if available
                    ai_sentiment, ai_tags = ai_results[i] if i < len(ai_results) else (None, [])
                    
                    # Analyze sentiment (use AI sentiment if available and valid)
                    sentiment_score = ai_sentiment if ai_sentiment is not None else self.analyze_sentiment(content.content)
                    
                    # Extract keywords
                    keywords = self.extract_keywords(content.content)
                    
                    # Extract entities mentioned
                    entities = self.extract_entities_mentioned(content.content)
                    
                    # Calculate impact score
                    impact_score = self.calculate_impact_score(
                        sentiment_score, 
                        content.engagement_metrics
                    )
                    
                    # Categorize content (use AI tags if available)
                    categories = self.categorize_content(content.content, entities, ai_tags)
                    
                    # Create processed content record
                    processed = ProcessedContent(
                        raw_content_id=content.id,
                        sentiment_score=sentiment_score,
                        impact_score=impact_score,
                        categories=categories,
                        keywords=keywords,
                        entities_mentioned=entities,
                        summary=content.content[:100] + "..." if len(content.content) > 100 else content.content
                    )
                    
                    db.add(processed)
                    db.commit()
                    processed_count += 1
                    
                except Exception as e:
                    db.rollback()
                    logger.error(f"Error processing content {content.id}: {str(e)}")
                    continue
            
            logger.info(f"Processed {processed_count} content items")
            return processed_count
            
        except Exception as e:
            logger.error(f"Error in process_unprocessed_content: {str(e)}")
            return 0
        finally:
            db.close()
