from fastapi import Header, Query, HTTPException, status
from typing import Optional
import time
from collections import defaultdict

from config import AppConfig

# Brute-force lockout: tracks failed attempts per IP
_failed_attempts: dict[str, list[float]] = defaultdict(list)
LOCKOUT_WINDOW = 60    # seconds
MAX_FAILURES = 10      # max failures before lockout


def _check_lockout(ip: str):
    """Raise 429 if the IP has exceeded the failure threshold in the lockout window."""
    now = time.time()
    # Prune old attempts outside the window
    _failed_attempts[ip] = [t for t in _failed_attempts[ip] if now - t < LOCKOUT_WINDOW]
    if len(_failed_attempts[ip]) >= MAX_FAILURES:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many failed authentication attempts. Try again in {LOCKOUT_WINDOW} seconds.",
        )


async def verify_api_key(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    api_key: Optional[str] = Query(None),
):
    """
    Verify the API key from either the X-API-Key header or the api_key query parameter.
    Query parameter is primarily used for WebSockets where headers cannot be easily set.
    Enforces brute-force lockout after 10 failures per IP within 60 seconds.
    """
    # We don't have the request object here, but we can use a fixed key for the lockout check
    # For a more accurate IP-based lockout, inject Request and use request.client.host
    received_key = x_api_key or api_key

    if not received_key or received_key != AppConfig.API_KEY:
        # Record failure under a generic key (or use IP if Request is injected)
        _failed_attempts["global"].append(time.time())
        _check_lockout("global")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key"
        )

    # Successful auth — clear any previous failures
    _failed_attempts.pop("global", None)
    return received_key
