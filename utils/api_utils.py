# utils/api_utils.py
import os
import time
from collections import deque
from loguru import logger

class ApiKeyManager:
    """
    Manages rotation of API keys to handle rate limits.
    Can be used with any API that requires key rotation.
    """
    def __init__(self, api_keys=None, env_prefix="OPENROUTER_API_KEY", cooldown_seconds=60):
        """
        Initialize the API key manager.
        
        Args:
            api_keys: List of API keys (if None, will load from environment variables)
            env_prefix: Prefix for environment variables to load keys from
            cooldown_seconds: How long to cooldown a rate-limited key
        """
        self.cooldown_seconds = cooldown_seconds
        self.rate_limited_keys = {}  # Track keys that hit rate limits and when they can be used again
        
        # Load API keys if not provided
        if api_keys is None:
            self.api_keys = self._load_keys_from_env(env_prefix)
        else:
            self.api_keys = deque(api_keys) if api_keys else deque()
            
        if not self.api_keys:
            logger.error(f"No API keys found with prefix '{env_prefix}'")
            raise ValueError(f"At least one API key with prefix '{env_prefix}' must be provided")
            
        self.current_key = self.api_keys[0]
        logger.info(f"ApiKeyManager initialized with {len(self.api_keys)} keys")
        
    def _load_keys_from_env(self, prefix):
        """Load API keys from environment variables with the given prefix"""
        keys = deque()
        
        # First try the main key (without index)
        main_key = os.environ.get(prefix)
        if main_key:
            keys.append(main_key)
            
        # Then try indexed keys (PREFIX_1, PREFIX_2, etc.)
        i = 1
        while True:
            key = os.environ.get(f"{prefix}_{i}")
            if key:
                keys.append(key)
                i += 1
            else:
                break
                
        return keys
        
    def get_current_key(self):
        """Get the current usable API key"""
        # First check if current key is still in cooldown
        if self.current_key in self.rate_limited_keys:
            current_time = time.time()
            if current_time < self.rate_limited_keys[self.current_key]:
                # Current key is still cooling down, try another
                logger.debug(f"Current key is cooling down, rotating to next")
                self.rotate_key()
                
        return self.current_key
        
    def rotate_key(self, reason=None):
        """
        Rotate to the next available API key
        
        Args:
            reason: Optional reason for rotation (e.g. "rate_limit")
        
        Returns:
            The new current API key
        """
        # If the current key is rate limited, mark it
        if reason == "rate_limit":
            self.mark_rate_limited(self.current_key)
            
        # If we only have one key, clear it from cooling and return
        if len(self.api_keys) == 1:
            logger.warning("Only one API key available, removing cooldown")
            if self.current_key in self.rate_limited_keys:
                del self.rate_limited_keys[self.current_key]
            return self.current_key
            
        # Rotate the keys
        self.api_keys.rotate(-1)
        self.current_key = self.api_keys[0]
        
        # Check if the new key is still cooling down
        if self.current_key in self.rate_limited_keys:
            current_time = time.time()
            if current_time < self.rate_limited_keys[self.current_key]:
                logger.info(f"Next key is also cooling down, trying another")
                return self.rotate_key()  # Recursively try the next key
            else:
                # Key has cooled down, remove from cooling list
                del self.rate_limited_keys[self.current_key]
                
        logger.info(f"Rotated to next API key")
        return self.current_key
        
    def mark_rate_limited(self, key):
        """
        Mark a specific key as rate limited
        
        Args:
            key: The API key to mark as rate limited
        """
        self.rate_limited_keys[key] = time.time() + self.cooldown_seconds
        logger.warning(f"API key marked as rate limited, cooling down for {self.cooldown_seconds} seconds")
