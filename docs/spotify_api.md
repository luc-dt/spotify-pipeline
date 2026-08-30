# 📡 Spotify Web API Specification (2026 Edition)

---

## 1. Authentication: OAuth 2.0 Client Credentials Flow
- **Token Endpoint**: `POST https://accounts.spotify.com/api/token`
- **Headers**: `Authorization: Basic base64(CLIENT_ID:CLIENT_SECRET)`
- **Body**: `grant_type=client_credentials`
- **Token Lifespan**: 3600 seconds (1 hour). Auto-refreshed by `src/extract/spotify_client.py`.

---

## 2. Core Endpoints for Catalog Extraction

### A. Artist Search / Retrieval
- **Endpoint**: `GET https://api.spotify.com/v1/search`
- **Parameters**: `q={artist_name}`, `type=artist`, `limit=1`
- **Key Fields**: `id`, `name`, `genres`, `external_urls.spotify`, `images`

### B. Artist Albums
- **Endpoint**: `GET https://api.spotify.com/v1/artists/{id}/albums`
- **Parameters**: `include_groups=album,single,compilation`, `limit=50`, `offset={offset}`
- **Key Fields**: `id`, `name`, `album_type`, `total_tracks`, `release_date`, `release_date_precision`, `artists`

### C. Album Tracks
- **Endpoint**: `GET https://api.spotify.com/v1/albums/{id}/tracks`
- **Parameters**: `limit=50`, `offset={offset}`
- **Key Fields**: `id`, `name`, `track_number`, `duration_ms`, `explicit`, `artists`

---

## 3. Rate Limiting & Resilience Strategy
- **Standard Quota**: Tens of requests per second.
- **Handling 429 (Too Many Requests)**: Read `Retry-After` response header, sleep for indicated seconds with jitter.
- **Batching Strategy**: Sequential entity traversal (`Artist → Albums → Tracks`) with clean pagination loops.
