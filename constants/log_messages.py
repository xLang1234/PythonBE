"""
Log message constants for the Crypto News Twitter Collector (Twikit).
This file centralizes all log messages to ensure consistency and easier maintenance.
"""

# App startup messages
APP_STARTING = "Starting Crypto News Twitter Collector (Twikit)"
SCHEDULER_STARTING = "Starting scheduler loop"
DB_INITIALIZED = "Database initialized with default accounts"
SCHEDULED_JOBS_SETUP = "Scheduled jobs set up. Collection interval: {interval} minutes"

# Process messages
DATA_PROCESSING_START = "Starting data processing"
DATA_PROCESSING_COMPLETE = "Data processing completed, processed {count} items"
COLLECTION_START = "Starting Twitter data collection"
COLLECTION_COMPLETE = "Twitter data collection completed, collected {count} tweets"

# Key rotation and API messages
API_KEY_ROTATION = "Rotated to next API key"
API_KEY_COOLDOWN = "API key marked as rate limited, cooling down for {seconds} seconds"
API_KEY_INITIALIZED = "ApiKeyManager initialized with {count} keys"
API_KEY_COOLDOWN_ACTIVE = "Current key is cooling down, rotating to next"
API_MANAGER_INITIALIZED = "SentimentAnalyzer initialized with OpenRouter API client"

# Database operations
TWEET_SAVED = "Saved tweet {id} to database (language: {lang})"
TWEET_EXISTS = "Tweet {id} already exists in database"
TWITTER_SOURCE_CREATED = "Created Twitter source record"

# Skipping messages
SKIP_RETWEET = "Skipping retweet: {id}"
SKIP_NON_ENGLISH = "Skipping non-English tweet: {id}"
SKIP_SHORT_TWEET = "Skipping tweet with fewer than {count} words: {id}"
SKIP_NON_CRYPTO = "Skipping non-crypto content: {id}"

# Warning messages
NO_TWITTER_SOURCE = "No active Twitter source found"
LANG_DETECTION_FAILED = "Language detection failed for text: {text}..."
MISSING_FIELDS = "Missing fields in response from {model}: {fields}"
ALL_MODELS_FAILED = "All AI models failed to analyze: {text}..."

# Error messages
DB_INIT_ERROR = "Error initializing database: {error}"
MAIN_LOOP_ERROR = "Error in main loop: {error}"
SAVE_TWEET_ERROR = "Error saving tweet: {error}"
ANALYZE_ERROR = "Error processing content {id}: {error}"
PROCESS_ERROR = "Error in process_unprocessed_content: {error}"
COMPLETION_ERROR = "Failed to generate summary"
NO_API_KEYS = "No API keys found with prefix '{prefix}'"
API_KEYS_REQUIRED = "At least one API key with prefix '{prefix}' must be provided"
MODEL_RESPONSE_ERROR = "Failed to get valid response from {model}"
MULTI_MODEL_ERROR = "Exception in multi-model AI analysis: {error}"

# Process info
CONTENT_PROCESSED = "Processed content {id} with source link: {url}"
UNPROCESSED_FOUND = "Found {count} unprocessed English content items"
MODELS_SELECTED = "Using models for analysis: {models}"
SHUTDOWN_MESSAGE = "Received keyboard interrupt, shutting down..."
