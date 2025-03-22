# api/openrouter.py
import aiohttp
import json
import re
from loguru import logger

from utils.api_utils import ApiKeyManager

class OpenRouterAPI:
    """
    Client for interacting with the OpenRouter API with automatic key rotation
    """
    API_URL = 'https://openrouter.ai/api/v1/chat/completions'
    
    def __init__(self, api_key_manager=None):
        """
        Initialize the OpenRouter API client
        
        Args:
            api_key_manager: Optional ApiKeyManager instance. If None, a new one will be created.
        """
        # Initialize or use provided API key manager
        self.key_manager = api_key_manager or ApiKeyManager(env_prefix="OPENROUTER_API_KEY")
        
    async def _make_request(self, session, data):
        """
        Make a request to the OpenRouter API with automatic key rotation on rate limits
        
        Args:
            session: aiohttp ClientSession
            data: Request data
            
        Returns:
            Response data or None if failed after all retries
        """
        max_retries = min(len(self.key_manager.api_keys) * 2, 10)  # Try at most twice per key, up to 10 times
        retries = 0
        
        while retries < max_retries:
            # Get current API key
            api_key = self.key_manager.get_current_key()
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            }
            
            try:
                async with session.post(self.API_URL, json=data, headers=headers) as response:
                    response_status = response.status
                    response_json = await response.json()
                    
                    # Check for rate limit (either in status code or in response body)
                    if response_status == 429 or (isinstance(response_json, dict) and response_json.get('code') == 429):
                        logger.warning(f"Rate limit hit (429). Rotating API key and retrying.")
                        self.key_manager.rotate_key(reason="rate_limit")
                        retries += 1
                        continue
                    
                    if response_status == 200:
                        return response_json
                    else:
                        logger.error(f"API error: {response_status} - {response_json}")
                        # For non-rate-limit errors, try with a different key
                        self.key_manager.rotate_key()
                        retries += 1
                
            except Exception as e:
                logger.error(f"Request exception: {str(e)}")
                self.key_manager.rotate_key()
                retries += 1
                
        logger.error(f"Failed to get successful response after {max_retries} retries")
        return None
    
    async def chat_completion(self, session, model, messages, temperature=0.7, max_tokens=None):
        """
        Send a chat completion request to OpenRouter
        
        Args:
            session: aiohttp ClientSession
            model: Model ID to use
            messages: List of message objects
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate
            
        Returns:
            Completion response or None if failed
        """
        data = {
            "model": model,
            "messages": messages,
            "temperature": temperature
        }
        
        if max_tokens is not None:
            data["max_tokens"] = max_tokens
            
        response = await self._make_request(session, data)
        return response
        
    async def extract_json_from_completion(self, session, model, prompt, temperature=0.1):
        """
        Helper method to get JSON output from a model
        
        Args:
            session: aiohttp ClientSession
            model: Model ID to use
            prompt: The prompt text
            temperature: Sampling temperature
            
        Returns:
            Parsed JSON object or None
        """
        messages = [{"role": "user", "content": prompt}]
        response = await self.chat_completion(session, model, messages, temperature)
        
        if not response or 'choices' not in response:
            return None
            
        content = response['choices'][0]['message']['content']
        
        # Clean up response to extract only JSON
        content = content.strip()
        
        # Remove markdown code blocks if present
        content = re.sub(r'^```json\s*', '', content)
        content = re.sub(r'\s*```$', '', content)
        
        # Try to parse the JSON response
        try:
            return json.loads(content)
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Failed to parse JSON response: {e}\nContent: {content[:200]}")
            return None
