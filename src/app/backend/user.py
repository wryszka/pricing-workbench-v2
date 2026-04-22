"""Extract the logged-in user from Databricks Apps SSO headers. Apps proxy the
caller's identity to the container via well-known X-Forwarded-* headers — we use
them so every approval / agent call is attributed to the actual logged-in user
instead of a hard-coded string."""
from __future__ import annotations
from fastapi import Request, Depends

def current_user(request: Request) -> dict[str, str]:
    """Returns {email, username, ip}. Falls back to anonymous on local dev."""
    h = request.headers
    email = (
        h.get("x-forwarded-email")
        or h.get("X-Forwarded-Email")
        or h.get("x-real-email")
        or ""
    )
    username = (
        h.get("x-forwarded-preferred-username")
        or h.get("X-Forwarded-Preferred-Username")
        or (email.split("@")[0] if email else "anonymous")
    )
    ip = h.get("x-forwarded-for", "").split(",")[0].strip() or "unknown"
    return {"email": email or f"{username}@local", "username": username, "ip": ip}

UserDep = Depends(current_user)
