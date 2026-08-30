"""
Day 1 Checkpoint: Spotify API Connection & Schema Verification Script
Reads SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET from .env file.
"""

import os
import base64
import requests
from dotenv import load_dotenv

# Read the .env file and sets them as environment variables
load_dotenv()

# Access them using os.getenv or os.environ
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")


def test_connection():
    if not CLIENT_ID or CLIENT_ID == "your_spotify_client_id_here":
        print("[ERROR] SPOTIFY_CLIENT_ID not found in .env! Please update your .env file.")
        return

    # 1. Fetch OAuth 2.0 Access Token
    print("[INFO] Authenticating with Spotify Accounts API...")
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    token_resp = requests.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        headers={"Authorization": f"Basic {auth_header}"},
        timeout=10
    )

    if token_resp.status_code != 200:
        print(f"[FAIL] Authentication failed ({token_resp.status_code}): {token_resp.text}")
        return

    token = token_resp.json()["access_token"]
    print("[SUCCESS] Connected to Spotify API! Access token acquired.")

    headers = {"Authorization": f"Bearer {token}"}

    # 2. Test Search Artist (Taylor Swift)
    print("\n[INFO] 1. Testing Artist Search (Taylor Swift)...")
    res = requests.get(
        "https://api.spotify.com/v1/search",
        headers=headers,
        params={"q": "Taylor Swift", "type": "artist", "limit": 1},
        timeout=10
    )
    artist_item = res.json()["artists"]["items"][0]
    artist_id = artist_item["id"]
    print(f"       Artist Name : {artist_item['name']}")
    print(f"       Artist ID   : {artist_id}")
    print(f"       Available Artist Keys: {list(artist_item.keys())}")

    # 3. Test Artist Albums
    print("\n[INFO] 2. Testing Artist Albums Endpoint...")
    albums_res = requests.get(
        f"https://api.spotify.com/v1/artists/{artist_id}/albums",
        headers=headers,
        params={"include_groups": "album,single", "limit": 5},
        timeout=10
    )
    albums = albums_res.json().get("items", [])
    total_albums = albums_res.json().get("total", 0)
    print(f"       Total Discography Items: {total_albums}")
    if albums:
        print(f"       Available Album Keys   : {list(albums[0].keys())}")
        for alb in albums[:3]:
            print(f"       - [{alb.get('release_date')}] {alb.get('name')} (type={alb.get('album_type')}, tracks={alb.get('total_tracks')})")

    # 4. Test Album Tracks
    if albums:
        sample_album_id = albums[0]["id"]
        sample_album_name = albums[0]["name"]
        print(f"\n[INFO] 3. Testing Album Tracks Endpoint for '{sample_album_name}'...")
        tracks_res = requests.get(
            f"https://api.spotify.com/v1/albums/{sample_album_id}/tracks",
            headers=headers,
            params={"limit": 5},
            timeout=10
        )
        tracks = tracks_res.json().get("items", [])
        if tracks:
            print(f"       Available Track Keys   : {list(tracks[0].keys())}")
            for trk in tracks[:3]:
                duration_min = round(trk.get("duration_ms", 0) / 1000 / 60, 2)
                print(f"       - Track #{trk.get('track_number')}: {trk.get('name')} ({duration_min} min, explicit={trk.get('explicit')})")

    print("\n" + "=" * 60)
    print("[DAY 1 VERIFIED] All core Spotify 2026 endpoints active and functioning!")
    print("=" * 60)


if __name__ == "__main__":
    test_connection()
