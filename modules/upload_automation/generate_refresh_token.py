"""
Generate OAuth2 refresh token for Google Ads API.

Reads client_id / client_secret from .streamlit/secrets.toml [google_ads].
Run: python3 modules/upload_automation/generate_refresh_token.py
"""
import os
import sys

# Allow importing streamlit secrets when running standalone
try:
    import toml
except ImportError:
    sys.exit("toml package required: pip install toml")

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/adwords"]


def _load_secrets() -> dict:
    """Load [google_ads] section from .streamlit/secrets.toml."""
    # Walk up to find repo root with .streamlit/
    search = os.path.abspath(os.path.dirname(__file__))
    for _ in range(5):
        candidate = os.path.join(search, ".streamlit", "secrets.toml")
        if os.path.exists(candidate):
            data = toml.load(candidate)
            return data.get("google_ads", {})
        search = os.path.dirname(search)
    sys.exit("ERROR: .streamlit/secrets.toml not found. Run from project root.")


secrets = _load_secrets()
client_id = secrets.get("client_id")
client_secret = secrets.get("client_secret")

if not client_id or not client_secret:
    sys.exit("ERROR: client_id / client_secret not found in [google_ads] secrets.")

CLIENT_CONFIG = {
    "installed": {
        "client_id": client_id,
        "client_secret": client_secret,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost"],
    }
}

print("Starting OAuth flow...")
print("A browser window will open. Log in with the Google account that has Google Ads access.\n")

flow = InstalledAppFlow.from_client_config(CLIENT_CONFIG, scopes=SCOPES)
creds = flow.run_local_server(port=8080, open_browser=True)

print("\n--- Auth complete ---")
print(f"Token type: {type(creds)}")
print(f"Has refresh_token: {creds.refresh_token is not None}")
print(f"Has access_token: {creds.token is not None}")

if creds.refresh_token:
    print("\n" + "=" * 60)
    print("YOUR REFRESH TOKEN:")
    print("=" * 60)
    print(creds.refresh_token)
    print("=" * 60)
else:
    print("\nWARNING: No refresh_token returned.")
    print("This can happen if you previously authorized this app.")
    print("Try revoking access first:")
    print("  1. Go to https://myaccount.google.com/permissions")
    print("  2. Find and remove this app")
    print("  3. Run this script again")
    print()
    print("Access token (for reference):")
    print(creds.token)
