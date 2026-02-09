from __future__ import annotations

import os
from typing import Optional

from fastapi import Header, HTTPException, status


def require_api_key(
    authorization: Optional[str] = Header(default=None),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> None:
    """Optional API-key guard.

    If environment variable API_KEY is set (non-empty), requests must provide either:
      - Header: X-API-Key: <API_KEY>
      - Header: Authorization: Bearer <API_KEY>

    If API_KEY is not set, this guard becomes a no-op.
    """

    expected = (os.getenv("API_KEY", "") or "").strip()
    if not expected:
        return

    # Check X-API-Key first
    if x_api_key and x_api_key.strip() == expected:
        return

    # Check Authorization: Bearer <token>
    if authorization:
        parts = authorization.split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer" and parts[1].strip() == expected:
            return

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized (missing/invalid API key)",
    )
