import pytest

from security_config import default_admin_credentials, get_session_secret


def test_session_secret_requires_a_long_configured_value():
    with pytest.raises(RuntimeError, match="NCS_SECRET_KEY"):
        get_session_secret({})
    assert get_session_secret({"NCS_SECRET_KEY": "x" * 32}) == "x" * 32
    with pytest.raises(RuntimeError, match="NCS_SECRET_KEY"):
        get_session_secret({"NCS_SECRET_KEY": "replace-with-a-unique-random-value-at-least-32-characters"})


def test_bootstrap_admin_credentials_are_never_defaulted():
    with pytest.raises(RuntimeError, match="NCS_DEFAULT_ADMIN_USER"):
        default_admin_credentials({})
    with pytest.raises(RuntimeError, match="16 characters"):
        default_admin_credentials({"NCS_DEFAULT_ADMIN_USER": "operator", "NCS_DEFAULT_ADMIN_PASS": "short"})
    with pytest.raises(RuntimeError, match="16 characters"):
        default_admin_credentials({"NCS_DEFAULT_ADMIN_USER": "operator", "NCS_DEFAULT_ADMIN_PASS": "replace-with-a-unique-password-at-least-16-chars"})
    assert default_admin_credentials({
        "NCS_DEFAULT_ADMIN_USER": "operator",
        "NCS_DEFAULT_ADMIN_PASS": "a-unique-strong-password",
    }) == ("operator", "a-unique-strong-password")
