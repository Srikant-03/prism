"""
API Key pooling, failover management, and exponential backoff retry logic for LLM operations.
"""

import asyncio
import time
import functools
import logging
import threading
import random
from typing import List, Callable, Any

try:
    from google import genai
    from google.api_core.exceptions import ResourceExhausted, TooManyRequests
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

from config import AppConfig

logger = logging.getLogger(__name__)


# Module-level active client, set by the failover decorator before each API call.
_active_client = None


def get_active_client():
    """Return the currently active genai Client (set by the failover decorator)."""
    return _active_client


class APIKeyManager:
    """Manages a pool of Gemini API keys with automatic failover tracking."""
    
    def __init__(self, keys: List[str]):
        self.keys = [k for k in keys if k]
        self.exhausted_keys = set()
        self.rate_limited_keys = {}  # key -> timestamp when it got an actual 429
        self.last_used_keys = {}     # key -> timestamp of its last generative request
        self.current_index = 0
        self._lock = threading.Lock()
        
        if not self.keys:
            logger.warning("No API keys provided to APIKeyManager!")

    async def get_current_key(self, tier_rpm: int = 5) -> str | None:
        """Get the currently active API key, rotating if necessary to find one that satisfies the RPM."""
        if not self.keys:
            return None
            
        required_cooldown = 60.0 / tier_rpm if tier_rpm and tier_rpm > 0 else 0
        now = time.time()
        
        with self._lock:
            start_index = self.current_index
            
            # First pass: try to find a key that is neither exhausted, nor on 429 cooldown, nor skipping its RPM bound
            while True:
                key = self.keys[self.current_index]
                
                if key not in self.exhausted_keys:
                    last_429 = self.rate_limited_keys.get(key, 0)
                    last_used = self.last_used_keys.get(key, 0)
                    
                    if (now - last_429 > 65) and (now - last_used >= required_cooldown):
                        self.last_used_keys[key] = now
                        return key
                    
                self._advance_index()
                
                if self.current_index == start_index:
                    # All keys are currently busy or exhausted.
                    break
                    
            # Second pass: calculate which valid key will be available first and wait for it
            best_wait = float('inf')
            best_key = None
            
            for key in self.keys:
                if key not in self.exhausted_keys:
                    wait_429 = max(0.0, 65 - (now - self.rate_limited_keys.get(key, 0)))
                    wait_rpm = max(0.0, required_cooldown - (now - self.last_used_keys.get(key, 0)))
                    wait_time = max(wait_429, wait_rpm, 0.1) # Minimum 100ms yield
                    
                    if wait_time < best_wait:
                        best_wait = wait_time
                        best_key = key
                        
        if best_key:
            logger.info(f"Targeting {tier_rpm} RPM. Keys busy. Load-balancer waiting {best_wait:.1f}s...")
            await asyncio.sleep(best_wait)
            with self._lock:
                self.last_used_keys[best_key] = time.time()
                # Still advance index to distribute load evenly next time
                if self.keys and self.keys[self.current_index] == best_key:
                    self._advance_index()
            return best_key
                
        logger.error("ALL API KEYS EXHAUSTED!")
        return self.keys[0]  # Return the first one anyway

    def _advance_index(self):
        if self.keys:
            self.current_index = (self.current_index + 1) % len(self.keys)

    def mark_exhausted(self, key: str):
        """Mark a key as completely out of quota (429)."""
        logger.warning(f"Key ending in ...{key[-4:] if key else 'None'} marked as EXHAUSTED.")
        with self._lock:
            self.exhausted_keys.add(key)
            if self.keys and self.keys[self.current_index] == key:
                self._advance_index()

    def mark_rate_limited(self, key: str):
        """Mark a key as temporarily rate limited (e.g. per-minute limit)."""
        logger.warning(f"Key ending in ...{key[-4:] if key else 'None'} marked as RATE LIMITED.")
        with self._lock:
            self.rate_limited_keys[key] = time.time()
            if self.keys and self.keys[self.current_index] == key:
                self._advance_index()
            
    def all_keys_exhausted(self) -> bool:
        """Check if every key in the pool is permanently exhausted."""
        if not self.keys:
            return True
        with self._lock:
            return len(self.exhausted_keys) >= len(self.keys)

    def reset_exhausted(self):
        """Clear all exhausted/rate-limited state (e.g. after daily quota resets)."""
        with self._lock:
            self.exhausted_keys.clear()
            self.rate_limited_keys.clear()
            logger.info("API key exhaustion state reset.")

    def get_status(self) -> dict:
        """Get the health status of the API key pool."""
        return {
            "total_keys": len(self.keys),
            "exhausted_keys": len(self.exhausted_keys),
            "rate_limited_keys": len(self.rate_limited_keys),
            "active_key_index": self.current_index,
            "all_exhausted": self.all_keys_exhausted(),
            "has_available_keys": len(self.exhausted_keys) < len(self.keys) if self.keys else False
        }

# Global singleton instance
key_manager = APIKeyManager(AppConfig.llm.GEMINI_API_KEYS)


def with_llm_failover(max_retries: int = None, tier_rpm: int = 5):
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
            global _active_client
            if not HAS_GENAI:
                return await func(*args, **kwargs)

            # Fast path: if all keys are already permanently exhausted, don't waste time retrying
            if key_manager.all_keys_exhausted():
                logger.warning("All API keys exhausted — skipping LLM call, raising immediately.")
                raise ResourceExhausted("All API keys are exhausted. Falling back to offline mode.")

            retries = 0
            while retries <= max_retries:
                current_key = await key_manager.get_current_key(tier_rpm=tier_rpm)
                
                # Create a genai Client with the active key right before the call
                if current_key:
                    _active_client = genai.Client(api_key=current_key)
                    
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
                        
                        # Only mark as permanently exhausted for EXPLICIT daily/monthly quota signals.
                        # NOTE: Google's generic 429 message always contains "check your plan and billing details"
                        #       even for temporary per-minute rate limits — so "billing" alone is NOT a signal
                        #       of permanent exhaustion.
                        has_daily_signal = any(kw in error_str for kw in ["daily limit", "daily quota", "per-day", "per day"])
                        has_monthly_signal = any(kw in error_str for kw in ["monthly limit", "monthly quota", "per-month", "per month"])
                        has_permanent_signal = "out of quota" in error_str or "billing account" in error_str or "disabled" in error_str
                        
                        if has_daily_signal or has_monthly_signal or has_permanent_signal:
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
                            
                        if is_quota:
                            # Quota exhausted: failover to next key immediately without backoff
                            logger.info(f"Failing over to next key immediately (Attempt {retries}/{max_retries})...")
                            await asyncio.sleep(0.1)
                            continue
                            
                        # Exponential backoff with jitter for generic rate limits
                        backoff = min(2 ** (retries - 1), 60.0) + random.uniform(0, 1)
                        logger.info(f"Retrying with next key (Attempt {retries}/{max_retries}) after {backoff:.2f}s backoff...")
                        await asyncio.sleep(backoff)
                        continue
                        
                    # It's an unrelated error (e.g. 400 Bad Request, 500 Internal), re-raise immediately
                    raise e
                    
        return wrapper
    return decorator
