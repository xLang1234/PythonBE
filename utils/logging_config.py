import sys
from loguru import logger
from config.settings import LOG_LEVEL


def setup_logging():

    logger.remove()

    logger.add(
        sys.stdout,
        level=LOG_LEVEL,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )

    logger.add(
        "logs/collector.log",
        rotation="1 day",
        retention="7 days",
        level=LOG_LEVEL
    )

    return logger
