import os
import time
from collections import deque
from loguru import logger
from constants.log_messages import *


class ApiKeyManager:
    def __init__(self, api_keys=None, env_prefix="OPENROUTER_API_KEY", cooldown_seconds=60):
        self.cooldown_seconds = cooldown_seconds
        self.rate_limited_keys = {}

        if api_keys is None:
            self.api_keys = self._load_keys_from_env(env_prefix)
        else:
            self.api_keys = deque(api_keys) if api_keys else deque()

        if not self.api_keys:
            logger.error(NO_API_KEYS.format(prefix=env_prefix))
            raise ValueError(API_KEYS_REQUIRED.format(prefix=env_prefix))

        self.current_key = self.api_keys[0]
        logger.info(API_KEY_INITIALIZED.format(count=len(self.api_keys)))

    def _load_keys_from_env(self, prefix):
        keys = deque()

        main_key = os.environ.get(prefix)
        if main_key:
            keys.append(main_key)

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
        if self.current_key in self.rate_limited_keys:
            current_time = time.time()
            if current_time < self.rate_limited_keys[self.current_key]:
                logger.debug(API_KEY_COOLDOWN_ACTIVE)
                self.rotate_key()

        return self.current_key

    def rotate_key(self, reason=None):
        if reason == "rate_limit":
            self.mark_rate_limited(self.current_key)

        if len(self.api_keys) == 1:
            logger.warning("Only one API key available, removing cooldown")
            if self.current_key in self.rate_limited_keys:
                del self.rate_limited_keys[self.current_key]
            return self.current_key

        self.api_keys.rotate(-1)
        self.current_key = self.api_keys[0]

        if self.current_key in self.rate_limited_keys:
            current_time = time.time()
            if current_time < self.rate_limited_keys[self.current_key]:
                logger.info("Next key is also cooling down, trying another")
                return self.rotate_key()
            else:
                del self.rate_limited_keys[self.current_key]

        logger.info(API_KEY_ROTATION)
        return self.current_key

    def mark_rate_limited(self, key):
        self.rate_limited_keys[key] = time.time() + self.cooldown_seconds
        logger.warning(API_KEY_COOLDOWN.format(seconds=self.cooldown_seconds))
