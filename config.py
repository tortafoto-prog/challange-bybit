import os
import json
from dotenv import load_dotenv

# Load .env file (looks for .env in current or parent dirs)
load_dotenv()

class Config:
    # ------------------------------------------------------------------
    # MULTI-USER CONFIGURATION
    # ------------------------------------------------------------------
    # Expects a JSON string in env var BYBIT_USERS_JSON
    # Format:
    # [
    #   {
    #     "name": "User1",
    #     "id": "u1",
    #     "is_eu": false, # Optional, use true for .eu endpoints
    #     "live": {"key": "...", "secret": "..."},    # Can coexist with testnet!
    #     "testnet": {"key": "...", "secret": "..."}  # Can coexist with live!
    #   },

    #   ...
    # ]
    # ------------------------------------------------------------------
    USERS_JSON = os.getenv("BYBIT_USERS_JSON")
    USERS = []
    
    if USERS_JSON:
        try:
            USERS = json.loads(USERS_JSON)
        except Exception as e:
            print(f"ERROR: Failed to parse BYBIT_USERS_JSON: {e}")
            print(f"RAW CONFIG: {USERS_JSON}")
            USERS = []

    # Single-user fallback
    _legacy_key = os.getenv("BYBIT_API_KEY")
    if not USERS and _legacy_key:
        print("WARNING: Using legacy single-user configuration.")
        _legacy_secret = os.getenv("BYBIT_API_SECRET")
        _is_eu = os.getenv("BYBIT_IS_EU", "False").lower() == "true"
        
        # Legacy Fallback: Defaults to LIVE unless specified otherwise?
        # Actually safer to just assume Live for legacy single-user vars.
        
        user_obj = {
            "name": "LegacyUser",
            "id": "legacy",
            "is_eu": _is_eu,
            "live": {"key": _legacy_key, "secret": _legacy_secret}
        }
        
        USERS.append(user_obj)


    # ------------------------------------------------------------------
    # Webhooks
    # ------------------------------------------------------------------
    GOOGLE_WEBHOOK_URL = os.getenv("GOOGLE_WEBHOOK_URL")
    APP_SECRET = os.getenv("APP_SECRET") 
