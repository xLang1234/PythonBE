import asyncio
import aiohttp
import json
import os
import random
import statistics
from loguru import logger
from dotenv import load_dotenv

from database.postgres import get_db, RawContent, ProcessedContent

# Load environment variables from .env file if it exists
load_dotenv()

# Get API key from environment variable
API_KEY = os.environ.get('OPENROUTER_API_KEY')
if not API_KEY:
    logger.error("OPENROUTER_API_KEY environment variable is not set. Sentiment analysis will not function.")

API_URL = 'https://openrouter.ai/api/v1/chat/completions'

class SentimentAnalyzer:
    def __init__(self):
        # Check if API key is available
        if not API_KEY:
            logger.error("Cannot initialize SentimentAnalyzer: OPENROUTER_API_KEY is missing")
            raise ValueError("OPENROUTER_API_KEY environment variable must be set")
            
        # List of models to use for sentiment analysis
        self.models = [
            "deepseek/deepseek-chat:free",
            "anthropic/claude-3-haiku:free",
            "mistralai/mistral-7b-instruct:free",
            "meta-llama/llama-3-8b-instruct:free"
        ]
        # Number of models to use per analysis (can be adjusted)
        self.models_per_analysis = 2
        
    async def query_model(self, session, model_name, text):
        """Query a specific model for sentiment analysis"""
        # Define the prompt template for crypto tweet analysis - expanded for full AI analysis
        prompt = f"""Analyze the following crypto-related tweet and provide a comprehensive assessment:

Tweet: "{text}"

Return your analysis in this exact JSON format:
{{
  "sentiment_score": [float between -1.0 and 1.0 where -1 is very negative, 0 is neutral, and 1 is very positive],
  "impact_score": [float between 0.0 and 1.0 representing potential market impact],
  "categories": [array of categories/topics like "market", "technology", "regulation", "security", etc.],
  "keywords": [array of up to 8 important keywords from the text],
  "entities_mentioned": [array of cryptocurrencies or crypto entities mentioned],
  "is_crypto_related": [boolean - true if crypto-related, false if not]
}}
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
                        
                        return {
                            "model": model_name,
                            "sentiment_score": result.get("sentiment_score", 0),
                            "impact_score": result.get("impact_score", 0.5),
                            "categories": result.get("categories", []),
                            "keywords": result.get("keywords", []),
                            "entities_mentioned": result.get("entities_mentioned", []),
                            "is_crypto_related": result.get("is_crypto_related", True),
                            "status": "success"
                        }
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse AI response from {model_name}: {content}")
                        return {
                            "model": model_name,
                            "status": "parse_error"
                        }
                else:
                    logger.error(f"Error from OpenRouter API for {model_name}: {response.status}")
                    return {
                        "model": model_name,
                        "status": "api_error"
                    }
        except Exception as e:
            logger.error(f"Exception in AI analysis with {model_name}: {str(e)}")
            return {
                "model": model_name,
                "status": "exception"
            }
    
    async def analyze_content(self, text):
        """Use multiple AI models to comprehensively analyze crypto-related content"""
        # Select a subset of models to use for this analysis
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
                valid_results = [r for r in results if r.get("status") == "success"]
                
                if not valid_results:
                    logger.warning(f"All AI models failed to analyze: {text[:50]}...")
                    # Return default values
                    return {
                        "sentiment_score": 0,
                        "impact_score": 0.5,
                        "categories": ["general"],
                        "keywords": [],
                        "entities_mentioned": [],
                        "is_crypto_related": True
                    }
                
                # Aggregate results from multiple models
                sentiment_scores = [r["sentiment_score"] for r in valid_results]
                impact_scores = [r["impact_score"] for r in valid_results]
                
                # Gather all categories, keywords, and entities
                all_categories = []
                all_keywords = []
                all_entities = []
                crypto_related_votes = []
                
                for r in valid_results:
                    all_categories.extend(r["categories"])
                    all_keywords.extend(r["keywords"])
                    all_entities.extend(r["entities_mentioned"])
                    crypto_related_votes.append(r["is_crypto_related"])
                
                # Use median for scores (more robust than mean)
                median_sentiment = statistics.median(sentiment_scores)
                median_impact = statistics.median(impact_scores)
                
                # Count frequencies for lists
                def get_top_items(items, max_count=5):
                    if not items:
                        return []
                    counts = {}
                    for item in items:
                        counts[item] = counts.get(item, 0) + 1
                    # Sort by count and return top items
                    return [item for item, _ in sorted(counts.items(), key=lambda x: x[1], reverse=True)[:max_count]]
                
                # Get consensus results
                top_categories = get_top_items(all_categories)
                top_keywords = get_top_items(all_keywords, 8)
                top_entities = get_top_items(all_entities)
                
                # Determine if content is crypto-related by majority vote
                is_crypto_related = sum(crypto_related_votes) >= len(crypto_related_votes)/2
                
                return {
                    "sentiment_score": median_sentiment,
                    "impact_score": median_impact,
                    "categories": top_categories if top_categories else ["general"],
                    "keywords": top_keywords,
                    "entities_mentioned": top_entities,
                    "is_crypto_related": is_crypto_related
                }
                
        except Exception as e:
            logger.error(f"Exception in multi-model AI analysis: {str(e)}")
            # Return default values
            return {
                "sentiment_score": 0,
                "impact_score": 0.5,
                "categories": ["general"],
                "keywords": [],
                "entities_mentioned": [],
                "is_crypto_related": True
            }
    
    async def process_unprocessed_content(self, limit=100):
        """Process unprocessed content from the database using AI analysis only"""
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
            
            # Process each content item
            for content in unprocessed:
                try:
                    # Analyze content using AI models
                    analysis_result = await self.analyze_content(content.content)
                    
                    # Skip non-crypto content if detected
                    if not analysis_result["is_crypto_related"]:
                        logger.info(f"Skipping non-crypto content: {content.id}")
                        continue
                    
                    # Create processed content record
                    processed = ProcessedContent(
                        raw_content_id=content.id,
                        sentiment_score=analysis_result["sentiment_score"],
                        impact_score=analysis_result["impact_score"],
                        categories=analysis_result["categories"],
                        keywords=analysis_result["keywords"],
                        entities_mentioned=analysis_result["entities_mentioned"],
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
