import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database settings
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "cryptonews")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Collection settings
COLLECTION_INTERVAL_MINUTES = int(os.getenv("COLLECTION_INTERVAL_MINUTES", "5"))
MAX_TWEETS_PER_COLLECTION = int(os.getenv("MAX_TWEETS_PER_COLLECTION", "100"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")