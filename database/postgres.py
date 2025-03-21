from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, Text, TIMESTAMP, ForeignKey, Float, JSON, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from loguru import logger
import json

from config.settings import DB_URL, MIN_TWEET_WORD_COUNT

# Create SQLAlchemy engine
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define models that match your PostgreSQL schema
class Source(Base):
    __tablename__ = "sources"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    type = Column(String(50), nullable=False)
    api_endpoint = Column(String(255))
    credentials_id = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default="now()")
    updated_at = Column(TIMESTAMP(timezone=True), server_default="now()")

class Entity(Base):
    __tablename__ = "entities"
    
    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey("sources.id"))
    entity_external_id = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    username = Column(String(100))
    description = Column(Text)
    followers_count = Column(Integer)
    relevance_score = Column(Float)
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default="now()")
    updated_at = Column(TIMESTAMP(timezone=True), server_default="now()")

class RawContent(Base):
    __tablename__ = "raw_content"
    
    id = Column(Integer, primary_key=True)
    entity_id = Column(Integer, ForeignKey("entities.id"))
    external_id = Column(String(100), nullable=False)
    content_type = Column(String(50), nullable=False)
    content = Column(Text, nullable=False)
    published_at = Column(TIMESTAMP(timezone=True), nullable=False)
    collected_at = Column(TIMESTAMP(timezone=True), server_default="now()")
    engagement_metrics = Column(JSON)
    raw_data = Column(JSON)

class ProcessedContent(Base):
    __tablename__ = "processed_content"
    
    id = Column(Integer, primary_key=True)
    raw_content_id = Column(Integer, ForeignKey("raw_content.id"))
    sentiment_score = Column(Float)
    impact_score = Column(Float)
    categories = Column(ARRAY(Text))
    keywords = Column(ARRAY(Text))
    entities_mentioned = Column(ARRAY(Text))
    processed_at = Column(TIMESTAMP(timezone=True), server_default="now()")
    summary = Column(Text)

# Database operations
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def save_tweet(db, entity_id, tweet, raw_data_tweet):
    print("🚀 ~ save_tweet ~ tweet:", tweet)
    """Save a tweet to the raw_content table"""
    try:
        if hasattr(raw_data_tweet, "text") and raw_data_tweet.text.startswith("RT @"):
            logger.info(f"Skipping retweet: {raw_data_tweet.id}")
            return None
        
        # Alternatively, check for retweeted_status attribute which Twitter API uses to indicate retweets
        if hasattr(raw_data_tweet, "retweeted_status"):
            logger.info(f"Skipping retweet: {raw_data_tweet.id}")
            return None
        
        word_count = len(raw_data_tweet.text.split())
        if word_count < MIN_TWEET_WORD_COUNT:
            logger.info(f"Skipping tweet with fewer than {MIN_TWEET_WORD_COUNT} words: {raw_data_tweet.id}")
            return None
        
        # Format engagement metrics
        engagement = {
            "likes": tweet.public_metrics.get("like_count", 0) if hasattr(tweet, "public_metrics") else 0,
            "retweets": tweet.public_metrics.get("retweet_count", 0) if hasattr(tweet, "public_metrics") else 0,
            "replies": tweet.public_metrics.get("reply_count", 0) if hasattr(tweet, "public_metrics") else 0,
            "quotes": tweet.public_metrics.get("quote_count", 0) if hasattr(tweet, "public_metrics") else 0
        }
        print("check1")
        # Check if tweet already exists
        existing = db.query(RawContent).filter(
            RawContent.entity_id == entity_id,
            RawContent.external_id == raw_data_tweet.id
        ).first()
        
        print("check2")
        if existing:
            logger.debug(f"Tweet {raw_data_tweet.id} already exists in database")
            return existing.id
        
        print("check3")
        # Create new raw_content record
        raw_content = RawContent(
            entity_id=entity_id,
            external_id=raw_data_tweet.id,
            content_type="tweet",
            content=raw_data_tweet.text,
            published_at=raw_data_tweet.created_at,
            engagement_metrics=engagement,
            raw_data=raw_data_tweet._json if hasattr(raw_data_tweet, "_json") else json.dumps(raw_data_tweet.__dict__, default=str)
        )
        
        print("check4")
        db.add(raw_content)
        db.commit()
        db.refresh(raw_content)
        logger.info(f"Saved tweet {raw_data_tweet.id} to database")
        return raw_content.id
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error saving tweet: {str(e)}")
        raise

def get_active_twitter_entities(db):
    """Get all active Twitter entities to collect data from"""
    twitter_source = db.query(Source).filter(Source.type == "twitter", Source.is_active == True).first()
    
    if not twitter_source:
        logger.warning("No active Twitter source found")
        return []
        
    entities = db.query(Entity).filter(
        Entity.source_id == twitter_source.id,
        Entity.is_active == True
    ).all()
    
    return entities

def get_or_create_twitter_source(db):
    """Get or create the Twitter source record"""
    twitter_source = db.query(Source).filter(Source.type == "twitter").first()
    
    if not twitter_source:
        twitter_source = Source(
            name="Twitter",
            type="twitter",
            api_endpoint="https://api.twitter.com/2",
            is_active=True
        )
        db.add(twitter_source)
        db.commit()
        db.refresh(twitter_source)
        logger.info("Created Twitter source record")
        
    return twitter_source
