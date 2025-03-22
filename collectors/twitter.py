import asyncio
from datetime import datetime, timedelta
from loguru import logger
import time
import os
from twikit import Client

from config.settings import MAX_TWEETS_PER_COLLECTION, TWITTER_USERNAME, TWITTER_EMAIL, TWITTER_PASSWORD
from database.postgres import get_db, get_active_twitter_entities, save_tweet, get_or_create_twitter_source, Entity
from cookie_manager import TwitterCookieManager

# Create a global event loop
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

class TwitterScraperWithRotation:
    def __init__(self):
        self.client = None
        self.cookie_manager = TwitterCookieManager(cookies_dir="twitterCookies")
        self.initialize_client()
        
    def initialize_client(self):
        """Initialize the Twikit scraper client"""
        try:
            self.client = Client('en-US')
            logger.info("Twikit Twitter scraper initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Twikit Twitter scraper: {str(e)}")
            raise
    
    async def login(self, force_new_login=False):
        """Login to Twitter using credentials with cookie rotation"""
        try:
            if not force_new_login:
                # Try to use an existing cookie file first
                cookie_file = self.cookie_manager.get_next_cookie_file()
                
                if cookie_file and os.path.exists(cookie_file):
                    try:
                        logger.info(f"Attempting to login with cookie file: {os.path.basename(cookie_file)}")
                        await self.client.login(auth_info_1=TWITTER_USERNAME, password=TWITTER_PASSWORD, cookies_file=cookie_file)
                        logger.info(f"Successfully logged in using cookie file")
                        return True
                    except Exception as e:
                        logger.warning(f"Failed to login with cookie file: {str(e)}")
                        # Mark cookie as invalid and try a fresh login
                        self.cookie_manager.mark_cookie_invalid(cookie_file)
            
            # If no cookie file or cookie login failed, try credentials login
            logger.info(f"Attempting to login with credentials as {TWITTER_USERNAME}")
            
            # Generate a new cookie filename for this session
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_cookie_file = os.path.join("twitterCookies", f"{TWITTER_USERNAME}_{timestamp}.json")
            
            await self.client.login(
                auth_info_1=TWITTER_USERNAME,
                auth_info_2=TWITTER_EMAIL,
                password=TWITTER_PASSWORD,
                cookies_file=new_cookie_file
            )
            
            logger.info(f"Successfully logged in as {TWITTER_USERNAME} and saved new cookie file")
            
            # Add the new cookie file to the rotation
            if os.path.exists(new_cookie_file):
                self.cookie_manager.cookie_files.append(new_cookie_file)
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to login to Twitter: {str(e)}")
            return False
    
    async def collect_user_tweets(self, entity_id, user_id, db, days_back=1, retry_with_new_cookie=True):
        """Collect tweets from a specific user with cookie rotation support"""
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
                        
                    # Create tweet data structure 
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
            
            if "429" in str(e) and retry_with_new_cookie:
                logger.warning("Rate limit hit. Attempting to rotate cookie and retry.")
                
                # Rotate to a new cookie
                cookie_file = self.cookie_manager.get_next_cookie_file(force_rotate=True)
                
                if cookie_file:
                    try:
                        # Re-login with the new cookie
                        await self.client.login(cookies_file=cookie_file)
                        logger.info(f"Re-logged in with rotated cookie file")
                        
                        # Try again with new cookie (but don't allow another retry to prevent infinite recursion)
                        return await self.collect_user_tweets(entity_id, user_id, db, days_back, retry_with_new_cookie=False)
                    except Exception as cookie_error:
                        logger.error(f"Failed to use rotated cookie: {str(cookie_error)}")
                        self.cookie_manager.mark_cookie_invalid(cookie_file)
            
            # If retry failed or not requested, or if error wasn't a rate limit
            if "429" in str(e):
                # If we hit a rate limit even after cookie rotation (or couldn't rotate)
                logger.warning("Rate limit hit. Sleeping for 15 minutes before continuing.")
                await asyncio.sleep(900)
                
            return 0
    
    async def collect_all(self):
        """Collect tweets from all active entities with cookie rotation"""
        db = next(get_db())
        try:
            # First ensure we're logged in
            await self.login()
            
            entities = get_active_twitter_entities(db)
            logger.info(f"Found {len(entities)} active Twitter entities")
            
            # Randomize the order of entities to avoid patterns
            import random
            random.shuffle(entities)
            
            total_collected = 0
            consecutive_failures = 0
            max_consecutive_failures = 3
            
            for entity in entities:
                try:
                    # If we've had too many consecutive failures, try a fresh login
                    if consecutive_failures >= max_consecutive_failures:
                        logger.warning(f"Had {consecutive_failures} consecutive failures, attempting fresh login")
                        await self.login(force_new_login=True)
                        consecutive_failures = 0
                    
                    count = await self.collect_user_tweets(
                        entity.id, 
                        entity.entity_external_id,
                        db
                    )
                    
                    if count > 0:
                        consecutive_failures = 0
                        total_collected += count
                    else:
                        consecutive_failures += 1
                    
                    # Add a randomized delay between requests
                    delay = 2 + random.uniform(0, 2)  # 2-4 seconds
                    await asyncio.sleep(delay)
                    
                except Exception as e:
                    logger.error(f"Error collecting tweets for entity {entity.username}: {str(e)}")
                    consecutive_failures += 1
                    continue
                    
            logger.info(f"Collection completed. Total tweets collected: {total_collected}")
            return total_collected
            
        except Exception as e:
            logger.error(f"Error in collect_all: {str(e)}")
            return 0
        finally:
            db.close()
            
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

def run_async(coro):
    """Run a coroutine in the global event loop without closing it"""
    return loop.run_until_complete(coro)

def add_default_crypto_accounts_with_rotation():
    scraper = TwitterScraperWithRotation()
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

def collect_twitter_data_with_rotation():
    scraper = TwitterScraperWithRotation()
    return run_async(scraper.collect_all())
