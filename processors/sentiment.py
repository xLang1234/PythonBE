# processors/sentiment.py
import asyncio
import aiohttp
import json
import os
import random
import statistics
import re
import time
from loguru import logger
from dotenv import load_dotenv

from database.postgres import get_db, RawContent, ProcessedContent
from utils.api_utils import ApiKeyManager  # Import our new utility
from api.openrouter import OpenRouterAPI  # Import our OpenRouter client

# Load environment variables from .env file
load_dotenv()

class SentimentAnalyzer:
    def __init__(self):
        # Initialize the OpenRouter API client with automatic key rotation
        try:
            self.api_client = OpenRouterAPI()
            logger.info("SentimentAnalyzer initialized with OpenRouter API client")
        except ValueError as e:
            logger.error(f"Cannot initialize SentimentAnalyzer: {str(e)}")
            raise
            
        # List of models to use for sentiment analysis
        self.models = [
            "deepseek/deepseek-chat:free",
            "anthropic/claude-3-haiku:free",
            "mistralai/mistral-7b-instruct:free",
            "meta-llama/llama-3-8b-instruct:free",
            "qwen/qwq-32b:free"
        ]
        # Number of models to use per analysis (can be adjusted)
        self.models_per_analysis = len(self.models)
        
    async def query_model(self, session, model_name, text):
        """Query a specific model for sentiment analysis"""
        # Define the prompt template with strict format requirements
        prompt = f"""You are a cryptocurrency analysis algorithm. Your only task is to analyze the following crypto-related tweet and output a standardized JSON object. 

Tweet: "{text}"

IMPORTANT: You must ONLY output valid JSON. Do not include any explanations, notes, or text outside the JSON object. Your entire response must be parseable as JSON.

Return this exact JSON structure with appropriate values:
{{
  "sentiment_score": [number between -1.0 and 1.0 where -1 is very negative, 0 is neutral, and 1 is very positive],
  "impact_score": [number between 0.0 and 1.0 representing potential market impact],
  "categories": [array of string categories like "market", "technology", "regulation", "security", etc.],
  "keywords": [array of up to 8 important string keywords from the text],
  "entities_mentioned": [array of string cryptocurrencies or crypto entities mentioned],
  "is_crypto_related": [boolean - true if crypto-related, false if not]
}}

REMINDER: Output ONLY the JSON object without any markdown formatting, explanations, or additional text.
"""

        # Use our OpenRouter client to make the request with auto key rotation
        result = await self.api_client.extract_json_from_completion(session, model_name, prompt)
        
        if not result:
            logger.warning(f"Failed to get valid response from {model_name}")
            return {
                "model": model_name,
                "status": "api_error"
            }
            
        # Validate required fields exist
        if not all(k in result for k in ["sentiment_score", "impact_score", "categories", "keywords", "entities_mentioned", "is_crypto_related"]):
            missing = [k for k in ["sentiment_score", "impact_score", "categories", "keywords", "entities_mentioned", "is_crypto_related"] if k not in result]
            logger.warning(f"Missing fields in response from {model_name}: {missing}")
            
            # Add missing fields with default values
            for field in missing:
                if field == "sentiment_score":
                    result[field] = 0
                elif field == "impact_score":
                    result[field] = 0.5
                elif field in ["categories", "keywords", "entities_mentioned"]:
                    result[field] = []
                elif field == "is_crypto_related":
                    result[field] = True
        
        return {
            "model": model_name,
            "sentiment_score": float(result["sentiment_score"]),  # Ensure numeric
            "impact_score": float(result["impact_score"]),  # Ensure numeric
            "categories": result["categories"],
            "keywords": result["keywords"],
            "entities_mentioned": result["entities_mentioned"],
            "is_crypto_related": bool(result["is_crypto_related"]),  # Ensure boolean
            "status": "success"
        }
            
    async def generate_summary(self, session, text, analysis_result):
        """Generate a professional summary using an AI model"""
        model_name = "deepseek/deepseek-chat:free"  
        
        # Define the prompt template for summary generation - updated for brevity
        prompt = f"""You are a financial analyst writing concise crypto market intelligence.

Content: "{text}"

Analysis data (for context only):
- Sentiment: {analysis_result["sentiment_score"]}
- Impact: {analysis_result["impact_score"]}
- Categories: {', '.join(analysis_result["categories"])}
- Entities: {', '.join(analysis_result["entities_mentioned"])}
- Keywords: {', '.join(analysis_result["keywords"][:3])}

Write ONE SHORT SENTENCE that begins with "Market Intelligence:" capturing the most essential insight.
Be extremely concise (under 80 characters if possible).
Focus on the most significant aspect of the content.
NO explanations, markdown, or trailing dots.
"""

        # Use the chat completion method directly
        messages = [{"role": "user", "content": prompt}]
        response = await self.api_client.chat_completion(session, model_name, messages)
        
        if not response or 'choices' not in response:
            logger.error("Failed to generate summary")
            return ""
            
        summary = response['choices'][0]['message']['content'].strip()
        
        # Ensure the summary starts with "Market Intelligence:"
        if not summary.startswith("Market Intelligence:"):
            summary = "Market Intelligence: " + summary
        
        # Remove any markdown or quotes
        summary = re.sub(r'^"|"$', '', summary)
        
        return summary
        
    # The rest of the class remains unchanged...
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
                        "impact_score": 0,
                        "categories": [],
                        "keywords": [],
                        "entities_mentioned": [],
                        "is_crypto_related": False
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
            async with aiohttp.ClientSession() as session:
                for content in unprocessed:
                    try:
                        # Analyze content using AI models
                        analysis_result = await self.analyze_content(content.content)
                        
                        # Skip non-crypto content if detected
                        if not analysis_result["is_crypto_related"]:
                            logger.info(f"Skipping non-crypto content: {content.id}")
                            continue
                        
                        # Generate AI summary
                        professional_summary = await self.generate_summary(session, content.content, analysis_result)
                        
                        # Create processed content record
                        processed = ProcessedContent(
                            raw_content_id=content.id,
                            sentiment_score=analysis_result["sentiment_score"],
                            impact_score=analysis_result["impact_score"],
                            categories=analysis_result["categories"],
                            keywords=analysis_result["keywords"],
                            entities_mentioned=analysis_result["entities_mentioned"],
                            summary=professional_summary
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
