"""Validation helpers for session and first-run administrator secrets."""

import os


def get_session_secret(environ=None):
    """Return a sufficiently strong configured session key or fail clearly."""
    environ = os.environ if environ is None else environ
    secret = (environ.get("NCS_SECRET_KEY") or "").strip()
    if len(secret) < 32:
        raise RuntimeError(
            "Set NCS_SECRET_KEY to a random value of at least 32 characters. "
            "Generate one with: python -c \"import secrets; print(secrets.token_urlsafe(48))\""
        )
    return secret


def default_admin_credentials(environ=None):
    """Require explicit first-run admin credentials; never use a public default."""
    environ = os.environ if environ is None else environ
    username = (environ.get("NCS_DEFAULT_ADMIN_USER") or "").strip()
    password = environ.get("NCS_DEFAULT_ADMIN_PASS") or ""
    if not username or len(password) < 16:
        raise RuntimeError(
            "For first-time setup, set NCS_DEFAULT_ADMIN_USER and "
            "NCS_DEFAULT_ADMIN_PASS (at least 16 characters) in the local .env file."
        )
    return username, password
