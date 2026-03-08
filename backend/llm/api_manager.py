"""
API Key pooling, failover management, and exponential backoff retry logic for LLM operations.
"""

import asyncio
import time
import functools
import logging
from typing import List, Callable, Any

try:
    import google.generativeai as genai
    from google.api_core.exceptions import ResourceExhausted, TooManyRequests
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

from config import AppConfig

logger = logging.getLogger(__name__)


class APIKeyManager:
    """Manages a pool of Gemini API keys with automatic failover tracking."""
    
    def __init__(self, keys: List[str]):
        self.keys = [k for k in keys if k]
        self.exhausted_keys = set()
        self.rate_limited_keys = {}  # key -> timestamp when it got an actual 429
        self.last_used_keys = {}     # key -> timestamp of its last generative request
        self.current_index = 0
        
        if not self.keys:
            logger.warning("No API keys provided to APIKeyManager!")

    async def get_current_key(self, tier_rpm: int = 15) -> str | None:
        """Get the currently active API key, rotating if necessary to find one that satisfies the RPM."""
        if not self.keys:
            return None
            
        required_cooldown = 60.0 / tier_rpm if tier_rpm > 0 else 0
        start_index = self.current_index
        now = time.time()
        
        # First pass: try to find a key that is neither exhausted, nor on 429 cooldown, nor skipping its RPM bound
        while True:
            key = self.keys[self.current_index]
            
            if key not in self.exhausted_keys:
                last_429 = self.rate_limited_keys.get(key, 0)
                last_used = self.last_used_keys.get(key, 0)
                
                if (now - last_429 > 25) and (now - last_used >= required_cooldown):
                    self.last_used_keys[key] = now
                    return key
                
            self._advance_index()
            
            if self.current_index == start_index:
                # All keys are currently busy or exhausted.
                break
                
        # Second pass: calculate which valid key will be available first and wait for it
        best_wait = float('inf')
        best_key = None
        
        for key in self.keys[self.current_index:] + self.keys[:self.current_index]:
            if key not in self.exhausted_keys:
                wait_429 = max(0.0, 25 - (now - self.rate_limited_keys.get(key, 0)))
                wait_rpm = max(0.0, required_cooldown - (now - self.last_used_keys.get(key, 0)))
                wait_time = max(wait_429, wait_rpm, 0.1) # Minimum 100ms yield
                
                if wait_time < best_wait:
                    best_wait = wait_time
                    best_key = key
                    
        if best_key:
            logger.info(f"Targeting {tier_rpm} RPM. Keys busy. Load-balancer waiting {best_wait:.1f}s...")
            await asyncio.sleep(best_wait)
            self.last_used_keys[best_key] = time.time()
            # Still advance index to distribute load evenly next time
            if self.keys[self.current_index] == best_key:
                self._advance_index()
            return best_key
                
        logger.error("ALL API KEYS EXHAUSTED!")
        return self.keys[start_index]  # Return the last one anyway

    def _advance_index(self):
        self.current_index = (self.current_index + 1) % len(self.keys)

    def mark_exhausted(self, key: str):
        """Mark a key as completely out of quota (429)."""
        logger.warning(f"Key ending in ...{key[-4:] if key else 'None'} marked as EXHAUSTED.")
        self.exhausted_keys.add(key)
        
        if self.keys and self.keys[self.current_index] == key:
            self._advance_index()

    def mark_rate_limited(self, key: str):
        """Mark a key as temporarily rate limited (e.g. per-minute limit)."""
        logger.warning(f"Key ending in ...{key[-4:] if key else 'None'} marked as RATE LIMITED.")
        self.rate_limited_keys[key] = time.time()
        
        if self.keys and self.keys[self.current_index] == key:
            self._advance_index()
            
    def get_status(self) -> dict:
        """Get the health status of the API key pool."""
        return {
            "total_keys": len(self.keys),
            "exhausted_keys": len(self.exhausted_keys),
            "rate_limited_keys": len(self.rate_limited_keys),
            "active_key_index": self.current_index,
            "has_available_keys": len(self.exhausted_keys) < len(self.keys) if self.keys else False
        }

# Global singleton instance
key_manager = APIKeyManager(AppConfig.llm.GEMINI_API_KEYS)


def with_llm_failover(max_retries: int = None, tier_rpm: int = 15):
    """
    Decorator to wrap any function calling the Gemini API.
    Catches 429 errors (Quota Exhausted / Too Many Requests) and automatically
    rotates the API key and retries the function call transparently.
    """
    if max_retries is None:
        # Retry up to the number of keys we have, plus a couple extra for temporary rate limits
        max_retries = max(3, len(key_manager.keys) + 2)
        
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            if not HAS_GENAI:
                return await func(*args, **kwargs)
                
            retries = 0
            while retries <= max_retries:
                current_key = await key_manager.get_current_key(tier_rpm=tier_rpm)
                
                # Configure the genai library with the active key right before the call
                if current_key:
                    genai.configure(api_key=current_key)
                    
                try:
                    return await func(*args, **kwargs)
                    
                except Exception as e:
                    # Check if it's a quota or rate limit error
                    is_quota = isinstance(e, ResourceExhausted) 
                    is_rate_limit = isinstance(e, TooManyRequests)
                    
                    error_str = str(e).lower()
                    
                    if "429" in error_str or "quota" in error_str or "exhausted" in error_str or getattr(e, "code", None) == 429:
                        # Always assume it's a temporary rate-limit / minute quota first
                        is_quota = False
                        is_rate_limit = True
                        
                        # Only mark as permanently exhausted if it explicitly mentions billing/day limits
                        if "billing" in error_str or "out of quota" in error_str or "daily" in error_str or "day" in error_str or "month" in error_str:
                            is_quota = True
                            is_rate_limit = False
                            
                    if is_quota or is_rate_limit:
                        retries += 1
                        logger.warning(f"LLM API limit hit on key {current_key[-4:] if current_key else 'None'}. Error: {error_str[:150]}")
                        
                        if is_quota:
                            key_manager.mark_exhausted(current_key)
                        else:
                            key_manager.mark_rate_limited(current_key)
                            
                        if retries > max_retries:
                            logger.error(f"Max retries ({max_retries}) reached for LLM failover. Aborting.")
                            raise e
                            
                        # Slight contextual backoff before switching to next key
                        logger.info(f"Retrying with next key (Attempt {retries}/{max_retries})...")
                        await asyncio.sleep(1.0)
                        continue
                        
                    # It's an unrelated error (e.g. 400 Bad Request, 500 Internal), re-raise immediately
                    raise e
                    
        return wrapper
    return decorator
