import schedule
import time
import sys
import os
import asyncio

from collectors.twitter import TwitterScraperWithRotation, add_default_crypto_accounts_with_rotation, collect_twitter_data_with_rotation
from processors.sentiment import SentimentAnalyzer
from config.settings import COLLECTION_INTERVAL_MINUTES
from constants.log_messages import *
from utils.logging_config import setup_logging

# Setup logging
logger = setup_logging()

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


def process_data():
    logger.info(DATA_PROCESSING_START)
    analyzer = SentimentAnalyzer()

    processed_count = loop.run_until_complete(
        analyzer.process_unprocessed_content())

    logger.info(DATA_PROCESSING_COMPLETE.format(count=processed_count))


def setup_scheduled_jobs():
    schedule.every(COLLECTION_INTERVAL_MINUTES).minutes.do(
        collect_twitter_data_with_rotation)

    schedule.every(COLLECTION_INTERVAL_MINUTES * 2).minutes.do(process_data)

    logger.info(SCHEDULED_JOBS_SETUP.format(
        interval=COLLECTION_INTERVAL_MINUTES))


def initialize_database():
    try:
        add_default_crypto_accounts_with_rotation()
        logger.info(DB_INITIALIZED)
    except Exception as e:
        logger.error(DB_INIT_ERROR.format(error=str(e)))


def main():
    logger.info(APP_STARTING)

    initialize_database()

    setup_scheduled_jobs()

    collect_twitter_data_with_rotation()
    process_data()

    logger.info(SCHEDULER_STARTING)
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.info(SHUTDOWN_MESSAGE)

            loop.close()

            break
        except Exception as e:
            logger.error(MAIN_LOOP_ERROR.format(error=str(e)))
            time.sleep(60)


if __name__ == "__main__":
    main()
