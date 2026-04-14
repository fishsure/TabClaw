"""FastAPI dependencies for authentication."""
from typing import Optional

from fastapi import Cookie, HTTPException

from auth import db, jwt_utils


async def get_current_user(session: Optional[str] = Cookie(default=None)) -> dict:
    if not session:
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = jwt_utils.verify_token(session)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    jti = payload.get("jti")
    if not jti or not db.is_session_valid(jti):
        raise HTTPException(status_code=401, detail="Session revoked")
    user_id = int(payload["sub"])
    user = db.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user


async def get_current_user_optional(session: Optional[str] = Cookie(default=None)) -> Optional[dict]:
    try:
        return await get_current_user(session)
    except HTTPException:
        return None
