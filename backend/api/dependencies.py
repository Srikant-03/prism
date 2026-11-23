from fastapi import Header, Query, Request, HTTPException, status
from typing import Optional
import time
from collections import defaultdict
import logging

from config import AppConfig

logger = logging.getLogger(__name__)

# Brute-force lockout: tracks failed attempts per client IP
_failed_attempts: dict[str, list[float]] = defaultdict(list)
LOCKOUT_WINDOW = 60    # seconds
MAX_FAILURES = 10      # max failures per IP before lockout


def _prune_expired_attempts(now: float):
    """Periodically clean up stale IP keys to prevent unbounded memory growth."""
    if len(_failed_attempts) > 1000:
        stale_ips = [
            ip for ip, timestamps in _failed_attempts.items()
            if not timestamps or (now - timestamps[-1] >= LOCKOUT_WINDOW)
        ]
        for ip in stale_ips:
            _failed_attempts.pop(ip, None)


def _get_client_ip(request: Optional[Request]) -> str:
    """Extract client IP taking proxy headers into account with safe fallback."""
    if request:
        # Check X-Forwarded-For if deployed behind a reverse proxy/load balancer
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        if request.client and request.client.host:
            return request.client.host
    return "127.0.0.1"


def _check_lockout(ip: str):
    """Raise 429 if the specific client IP has exceeded the failure threshold in the lockout window."""
    now = time.time()
    _prune_expired_attempts(now)
    # Prune old attempts for this IP outside the window
    _failed_attempts[ip] = [t for t in _failed_attempts[ip] if now - t < LOCKOUT_WINDOW]
    if len(_failed_attempts[ip]) >= MAX_FAILURES:
        logger.warning("Auth failure threshold exceeded for IP %s (%d failures). Locked out for %ds.", ip, MAX_FAILURES, LOCKOUT_WINDOW)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many failed authentication attempts. Try again in {LOCKOUT_WINDOW} seconds.",
        )


async def verify_api_key(
    request: Request,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    api_key: Optional[str] = Query(None),
):
    """
    Verify the API key from either the X-API-Key header or the api_key query parameter.
    Query parameter is primarily used for WebSockets where headers cannot be easily set.
    Enforces brute-force lockout per client IP (10 failures within 60 seconds).
    """
    client_ip = _get_client_ip(request)
    _check_lockout(client_ip)

    received_key = x_api_key or api_key

    if not received_key or received_key != AppConfig.API_KEY:
        _failed_attempts[client_ip].append(time.time())
        _check_lockout(client_ip)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key"
        )

    # Successful auth — clear any previous failures for this specific client IP
    _failed_attempts.pop(client_ip, None)
    return received_key
