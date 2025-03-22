import asyncio
from datetime import datetime, timedelta
from loguru import logger
import time
import os
from twikit import Client

from config.settings import MAX_TWEETS_PER_COLLECTION, TWITTER_USERNAME, TWITTER_EMAIL, TWITTER_PASSWORD, COOKIES_FILE
from database.postgres import get_db, get_active_twitter_entities, save_tweet, get_or_create_twitter_source, Entity

# Create a global event loop
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

class TwitterScraper:
    def __init__(self):
        self.client = None
        self.initialize_client()
        
    def initialize_client(self):
        """Initialize the Twikit scraper client"""
        try:
            self.client = Client('en-US')
            logger.info("Twikit Twitter scraper initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Twikit Twitter scraper: {str(e)}")
            raise
    
    async def login(self):
        """Login to Twitter using credentials from settings"""
        try:
            await self.client.login(
                auth_info_1=TWITTER_USERNAME,
                auth_info_2=TWITTER_EMAIL,
                password=TWITTER_PASSWORD,
                cookies_file=COOKIES_FILE
            )
            logger.info(f"Successfully logged in as {TWITTER_USERNAME}")
            return True
        except Exception as e:
            logger.error(f"Failed to login to Twitter: {str(e)}")
            return False
            
    async def get_user_by_username(self, username):
        """Get a Twitter user by username"""
        try:
            user = await self.client.get_user_by_screen_name(username)
            
            user_data = {
                'id': user.id,
                'name': user.name,
                'username': user.screen_name,
                'description': user.description,
                'public_metrics': {
                    'followers_count': user.followers_count,
                    'following_count': user.following_count,
                }
            }
            return user_data
        except Exception as e:
            logger.error(f"Unexpected error fetching user {username}: {str(e)}")
            return None
            
    async def add_entity_to_db(self, db, username):
        """Add a Twitter user as an entity to the database"""
        try:
            twitter_source = get_or_create_twitter_source(db)
            user_info = await self.get_user_by_username(username)
            if not user_info:
                logger.error(f"Could not find Twitter user: {username}")
                return None
                
            existing = db.query(Entity).filter(
                Entity.source_id == twitter_source.id,
                Entity.entity_external_id == str(user_info['id'])
            ).first()
            
            if existing:
                logger.info(f"Entity {username} already exists in database")
                return existing.id
                
            entity = Entity(
                source_id=twitter_source.id,
                entity_external_id=str(user_info['id']),
                name=user_info['name'],
                username=username,
                description=user_info['description'],
                followers_count=user_info['public_metrics']['followers_count'],
                relevance_score=1.0,
                is_active=True
            )
            
            db.add(entity)
            db.commit()
            db.refresh(entity)
            logger.info(f"Added entity {username} to database")
            return entity.id
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error adding entity {username}: {str(e)}")
            return None
    
    async def collect_user_tweets(self, entity_id, user_id, db, days_back=1):
        """Collect tweets from a specific user"""
        try:
            start_time = datetime.now() - timedelta(days=days_back)
            tweets = await self.client.get_user_tweets(user_id, 'Tweets', count=MAX_TWEETS_PER_COLLECTION)
            
            if not tweets:
                logger.info(f"No new tweets found for user {user_id}")
                return 0
                
            count = 0
            for tweet in tweets:
                try:
                    tweet_created_at = tweet.created_at
                    tweet_created_at = datetime.strptime(tweet_created_at, '%a %b %d %H:%M:%S %z %Y')
                    # if tweet_created_at < start_time:
                    #     continue
                        
                    # Create tweet data structure matching the save_tweet function expectations
                    tweet_data = {
                        'id': tweet.id,
                        'text': tweet.text,
                        'created_at': tweet_created_at,
                        'public_metrics': {
                        }
                    }
                    
                    save_tweet(db, entity_id, tweet_data, tweet)
                    count += 1
                except Exception as e:
                    logger.error(f"Error saving tweet {tweet.id}: {str(e)}")
                    continue
                    
            logger.info(f"Collected {count} tweets for user {user_id}")
            return count
            
        except Exception as e:
            logger.error(f"Error collecting tweets for {user_id}: {str(e)}")
            if "429" in str(e):
                logger.warning("Rate limit hit. Sleeping for 15 minutes.")
                await asyncio.sleep(900)
            return 0
    
    async def collect_all(self):
        """Collect tweets from all active entities"""
        db = next(get_db())
        try:
            # First ensure we're logged in
            await self.login()
            
            entities = get_active_twitter_entities(db)
            logger.info(f"Found {len(entities)} active Twitter entities")
            
            total_collected = 0
            for entity in entities:
                try:
                    count = await self.collect_user_tweets(
                        entity.id, 
                        entity.entity_external_id,
                        db
                    )
                    total_collected += count
                    await asyncio.sleep(2)  # Add a small delay between requests to avoid rate limits
                except Exception as e:
                    logger.error(f"Error collecting tweets for entity {entity.username}: {str(e)}")
                    continue
                    
            logger.info(f"Collection completed. Total tweets collected: {total_collected}")
            return total_collected
            
        except Exception as e:
            logger.error(f"Error in collect_all: {str(e)}")
            return 0
        finally:
            db.close()

def run_async(coro):
    """Run a coroutine in the global event loop without closing it"""
    return loop.run_until_complete(coro)

def add_default_crypto_accounts():
    scraper = TwitterScraper()
    db = next(get_db())
    try:
        # First login to Twitter
        run_async(scraper.login())
        
        default_accounts = [
            "coinbase",
            "binance", 
            "cz_binance",
            "ethereum",
            "saylor",
            "elonmusk",
            "SECGov",
        ]
        
        added = 0
        for username in default_accounts:
            try:
                entity_id = run_async(scraper.add_entity_to_db(db, username))
                if entity_id:
                    added += 1
                time.sleep(3)  # Add a small delay between requests
            except Exception as e:
                logger.error(f"Error adding account {username}: {str(e)}")
                continue
                
        logger.info(f"Added {added} default crypto accounts")
        return added
        
    except Exception as e:
        logger.error(f"Error in add_default_crypto_accounts: {str(e)}")
        return 0
    finally:
        db.close()

def collect_twitter_data():
    scraper = TwitterScraper()
    return run_async(scraper.collect_all())
