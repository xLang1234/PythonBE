import nltk
from textblob import TextBlob
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import re
from loguru import logger
import pandas as pd

from database.postgres import get_db, RawContent, ProcessedContent

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
    
    def categorize_content(self, text, entities_mentioned):
        """Categorize content into topics"""
        cleaned_text = self.clean_text(text)
        categories = []
        
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
    
    def process_unprocessed_content(self, limit=100):
        """Process unprocessed content from the database"""
        db = next(get_db())
        try:
            # Get unprocessed content
            unprocessed = db.query(RawContent).outerjoin(
                ProcessedContent, 
                RawContent.id == ProcessedContent.raw_content_id
            ).filter(
                ProcessedContent.id == None
            ).limit(limit).all()
            
            logger.info(f"Found {len(unprocessed)} unprocessed content items")
            
            processed_count = 0
            for content in unprocessed:
                try:
                    # Analyze sentiment
                    sentiment_score = self.analyze_sentiment(content.content)
                    
                    # Extract keywords
                    keywords = self.extract_keywords(content.content)
                    
                    # Extract entities mentioned
                    entities = self.extract_entities_mentioned(content.content)
                    
                    # Calculate impact score
                    impact_score = self.calculate_impact_score(
                        sentiment_score, 
                        content.engagement_metrics
                    )
                    
                    # Categorize content
                    categories = self.categorize_content(content.content, entities)
                    
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