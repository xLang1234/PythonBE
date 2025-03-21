import schedule
import time
from loguru import logger
import sys
import os
import asyncio

from collectors.twitter import TwitterScraperWithRotation, add_default_crypto_accounts_with_rotation, collect_twitter_data_with_rotation
from processors.sentiment import SentimentAnalyzer
from config.settings import COLLECTION_INTERVAL_MINUTES, LOG_LEVEL

# Configure logger
logger.remove()
logger.add(
    sys.stdout,
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)
logger.add("logs/collector.log", rotation="1 day", retention="7 days", level=LOG_LEVEL)

# Create global event loop
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

def process_data():
    """Process collected data"""
    logger.info("Starting data processing")
    analyzer = SentimentAnalyzer()
    
    # Run the async function in the event loop
    processed_count = loop.run_until_complete(analyzer.process_unprocessed_content())
    
    logger.info(f"Data processing completed, processed {processed_count} items")

def setup_scheduled_jobs():
    """Setup scheduled jobs"""
    # Schedule Twitter data collection
    schedule.every(COLLECTION_INTERVAL_MINUTES).minutes.do(collect_twitter_data_with_rotation)
    
    # Schedule data processing
    schedule.every(COLLECTION_INTERVAL_MINUTES * 2).minutes.do(process_data)
    
    logger.info(f"Scheduled jobs set up. Collection interval: {COLLECTION_INTERVAL_MINUTES} minutes")

def initialize_database():
    """Initialize database with default data if needed"""
    try:
        add_default_crypto_accounts_with_rotation()
        logger.info("Database initialized with default accounts")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")

def main():
    """Main entry point"""
    logger.info("Starting Crypto News Twitter Collector (Twikit)")
    
    # Initialize database with default data
    initialize_database()
    
    # Set up scheduled jobs
    setup_scheduled_jobs()
    
    # Immediate first run
    collect_twitter_data_with_rotation()
    process_data()
    
    # Run the scheduler
    logger.info("Starting scheduler loop")
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
            
            # Properly close the event loop when shutting down
            loop.close()
            
            break
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    main()
