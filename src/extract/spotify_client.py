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
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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

    def get(self, endpoint, params=None, max_retries=5, base_delay=1.0):
        """Executes a GET request to the Spotify Web API with resilient 429 / 5 xx retry handling."""
        # Allow passing either full URL or relative endpoint like "v1/search" or "search"
        if endpoint.startswith("http"):
            url = endpoint
        else:
            clean_endpoint = endpoint.lstrip("/")
            if not clean_endpoint.startswith("v1/"):
                clean_endpoint = f"v1/{clean_endpoint}"
            url = f"https://api.spotify.com/{clean_endpoint}"
        
        for attempt  in range(1, max_retries + 1):
            # 1. Always ensure our Bearer token is valid
            self._ensure_valid_token()
            headers = {"Authorization": f"Bearer {self.access_token}"}
            
            try:
                response = self.session.get(
                    url, headers=headers, params=params, timeout=15
                )
                # Case 1: Success (200 OK)
                if response.status_code == 200:
                    return response.json()

                # Case 2: Rate Limited (429 Too Many Requests)
                elif response.status_code == 429:
                    # Spotify gives use the exact seconds in the "Retry-After" header 
                    retry_after  = int(
                        response.headers.get(
                            "Retry-After", 
                            base_delay * (2 ** (attempt  - 1))
                        )
                    )
                     # 💡 If Spotify asks us to wait more than 60 seconds (e.g. 50 minutes quota exhausted):
                    if retry_after > 60:
                        raise RuntimeError(
                            f"Spotify Development Mode quota exceeded. "
                            f"Retry-After is {retry_after}s (~{retry_after // 60} min). "
                            f"Stop the pipeline and retry after the quota window resets."
                        )            
                    
                    # Add jitter (0.2s - 0.8s) to prevent the "thundering herd" problem
                    sleep_time = retry_after + random.uniform(0.2, 0.8)
                    print(
                        f"[WARN] 429 Rate limited on attempt {attempt }/{max_retries}. Backing off for {sleep_time:.2f}s..."
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
        
        raise RuntimeError(
            f"Failed to fetch data from {url} after {max_retries} attempts."
        )

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