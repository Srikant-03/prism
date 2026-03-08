from fastapi import Header, Query, HTTPException, status
from typing import Optional

from config import AppConfig

async def verify_api_key(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    api_key: Optional[str] = Query(None)
):
    """
    Verify the API key from either the X-API-Key header or the api_key query parameter.
    Query parameter is primarily used for WebSockets where headers cannot be easily set.
    """
    # Accept from either header or query parameter
    received_key = x_api_key or api_key

    if not received_key or received_key != AppConfig.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key"
        )

    return received_key
