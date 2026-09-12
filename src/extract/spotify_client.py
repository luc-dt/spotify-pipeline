# spotify_client.py establishes and manages a reliable authenticated connection to the Spotify Web API, 
# then provides a reusable get() method for retrieving JSON data.
# Spotify tokens expire in 1 hour (3600s). Re-authenticating on every request triggers rate limits.
# We cache the token with a 60s safety buffer.
# When hit with HTTP 429, we parse the Retry-After header and back off with exponential jitter.

import random
import os 
import time
import requests
import base64
from typing import Optional, Dict, Any
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Fallback: Parse .env manually if dotenv is missing or keys are not yet in environment
for candidate in [
    os.path.join(os.getcwd(), ".env"),
    os.path.join(os.path.dirname(__file__), "..", "..", ".env"),
    "/opt/airflow/.env",
    "/opt/airflow/airflow/.env",
]:
    if os.path.exists(candidate):
        try:
            with open(candidate, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip().strip("'\""))
        except Exception:
            pass

class SpotifyRateLimitError(Exception):
    """Raised when Spotify API 429 rate limit is encountered and cannot be immediately resolved."""
    def __init__(self, retry_after: int, message: Optional[str] = None):
        self.retry_after = retry_after
        super().__init__(message or f"Rate limited by Spotify API. Retry-After: {retry_after}s")


class SpotifyClient:

    def __init__(self):
        # 1. Load credentials from .env
        self.client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        
        # Validate essential credentials
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Missing SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET."
                "Check your .env file"
            )

        # 2. Initialize requests.Session() for reusable TCP connections
        self.session = requests.Session()

        # 3. Initialize token cache (self.access_token = None)
        self.access_token = None
        self.token_expiry_epoch =  0.0
        self.request_count = 0  # Tracks total HTTP requests issued in this process  

    def _get_access_token(self):
        """Requests a new OAuth 2.0 access token via Client Credentitals flow."""
        auth_header = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode("utf-8")
        ).decode("utf-8")

        response = self.session.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "client_credentials"},
            headers={"Authorization": f"Basic {auth_header}"},
            timeout=10
        )
        # Raise an exception if Spotify returns 400/401/500
        response.raise_for_status()

        data = response.json()
        self.access_token = data["access_token"]
        expires_in = data.get("expires_in", 3600)

        # Cache the token: expire 60 seconds early as a safety buffer
        self.token_expiry_epoch = time.time() + expires_in - 60

    def _ensure_valid_token(self):
        """Ensure a valid, unexpired access token exists in memory cache."""
        if self.access_token is None or time.time() >= self.token_expiry_epoch:
            self._get_access_token()

    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 5,
        base_delay: float = 1.0,
    ) -> Dict[str, Any]:
        """Executes a GET request to the Spotify Web API with resilient 429 / 5 xx retry handling."""
        # Allow passing either full URL or relative endpoint like "v1/search" or "search"
        if endpoint.startswith("http"):
            url = endpoint
        else:
            clean_endpoint = endpoint.lstrip("/")
            if not clean_endpoint.startswith("v1/"):
                clean_endpoint = f"v1/{clean_endpoint}"
            url = f"https://api.spotify.com/{clean_endpoint}"
        
        last_retry_after = 60
        last_status_code = None

        for attempt in range(1, max_retries + 1):
            # 1. Always ensure our Bearer token is valid
            self._ensure_valid_token()
            headers = {"Authorization": f"Bearer {self.access_token}"}
            
            try:
                self.request_count += 1
                response = self.session.get(
                    url, headers=headers, params=params, timeout=15
                )
                last_status_code = response.status_code

                # Case 1: Success (200 OK)
                if response.status_code == 200:
                    return response.json()

                # Case 2: Rate Limited (429 Too Many Requests)
                elif response.status_code == 429:
                    reason = None
                    try:
                        err_payload = response.json()
                        reason = err_payload.get("error", {}).get("reason")
                    except Exception:
                        pass

                    last_retry_after = int(
                        response.headers.get(
                            "Retry-After", 
                            int(base_delay * (2 ** (attempt - 1)))
                        )
                    )
                    # Distinguish Dev Mode Quota exhaustion from short-term rolling 30s rate throttle
                    if reason == "QUOTA_EXCEEDED" or last_retry_after > 60:
                        raise SpotifyRateLimitError(
                            retry_after=last_retry_after,
                            message=(
                                f"Spotify Development Mode quota ceiling reached ({reason or 'High Retry-After'}). "
                                f"Retry-After is {last_retry_after}s (~{last_retry_after // 60} min). "
                                f"Stop the pipeline and retry after the quota window resets."
                            ),
                        )

                    # Short-term rolling 30s throttle: sleep with jitter and retry
                    sleep_time = last_retry_after + random.uniform(0.2, 0.8)
                    print(
                        f"[RATE_LIMIT] 429 rolling-window throttle received. Backing off for {sleep_time:.2f}s (attempt {attempt}/{max_retries})..."
                    )
                    time.sleep(sleep_time)

                # Case 3: Token unexpectedly expired / invalid (401 Unauthorized)
                elif response.status_code == 401:
                    print(
                        f"[WARN] 401 Unauthorized on attempt {attempt}/{max_retries}. Refreshing token..."
                    )
                    self.access_token = None
                    self._get_access_token()

                # Case 4: Spotify Server Errors (500, 502, 503, 504)
                elif response.status_code in [500, 502, 503, 504]:
                    sleep_time = base_delay * (2 ** (attempt - 1)) + random.uniform(0.1, 0.5)
                    print(
                        f"[WARN] Server error {response.status_code}. Retrying in {sleep_time:.2f}s..."
                    )
                    time.sleep(sleep_time)
                # Case 5: Permanent client errors (e.g., 400 Bad Request, 404 Not Found)
                else:
                    response.raise_for_status()
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as net_err:
                sleep_time = base_delay * (2 ** (attempt - 1)) + random.uniform(0.1, 0.5)
                print(
                     f"[WARN] Network error ({net_err}). Retrying in {sleep_time:.2f}s (Attempt {attempt}/{max_retries})..."
                )
                time.sleep(sleep_time)
        
        if last_status_code == 429:
            raise SpotifyRateLimitError(retry_after=last_retry_after)

        raise RuntimeError(
            f"Failed to fetch data from {url} after {max_retries} attempts (last status: {last_status_code})."
        )


def spotify_get(url: str, headers: Dict[str, str], max_retries: int = 3) -> Dict[str, Any]:
    """
    Standalone wrapper for direct Spotify API calls.
    Handles 429 explicitly with Retry-After before raising to Airflow.
    """
    last_retry_after = 60
    for attempt in range(1, max_retries + 1):
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code == 200:
            return response.json()

        if response.status_code == 429:
            last_retry_after = int(response.headers.get("Retry-After", 60))
            print(f"[QUOTA] 429 received. Waiting {last_retry_after}s (attempt {attempt}/{max_retries})")
            time.sleep(last_retry_after)
            continue

        response.raise_for_status()

    raise SpotifyRateLimitError(retry_after=last_retry_after)


# -------------------------------------------------------------
# Test Block
# -------------------------------------------------------------

if __name__ == "__main__":
    print("--- Testing SpotifyClient Token Fetch ---")

    try:
        client = SpotifyClient()
        print("✓ Initialized client and loaded credentials successfully.")

        # Test fetching the access token
        client._get_access_token()

        # Verify results 
        if client.access_token:
            masked_token = f"{client.access_token[:8]}...{client.access_token[-6:]}"
            ttl_minutes = (client.token_expiry_epoch - time.time()) / 60
            
            print(f"✓ Token retrieved: {masked_token}")
            print(f"✓ Token cache expiry buffer: {ttl_minutes:.1f} minutes from now")
            print("\n🎉 Everything is working correctly!")
            
        # Test a live GET call using our resilient client
        data = client.get(
            "search", params={"q": "Taylor Swift", "type": "artist", "limit": 1}
        )
        artist = data["artists"]["items"][0]
        print(f"\n✓ Successfully queried Web API: {artist['name']} (ID: {artist['id']})")

    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
    except requests.exceptions.HTTPError as e:
        print(f"❌ Spotify API Error: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")