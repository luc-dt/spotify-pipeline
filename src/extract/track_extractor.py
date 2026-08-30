# we have all the albums, we need to extract the tracks inside each album.
from datetime import datetime, timezone
import time
from typing import Any, Dict, List, Optional
from src.extract.spotify_client import SpotifyClient
from src.extract.album_extractor import AlbumExtractor

class TrackExtractor:

    def __init__(self, client: Optional[SpotifyClient] = None):
        self.client = client or SpotifyClient()

    def extract_track(self, album_id: str, artist_id: str ="") -> List[Dict[str, Any]]:
        """Pagininates through an album's tracks (limit=10) and extracts simplified track metadata."""
        all_tracks = []
        seen_ids = set()
        offset = 0

        while True:
            data = self.client.get(
                f"v1/albums/{album_id}/tracks",
                params={"limit": 10, "offset": offset},
            )

            items = data.get("items", [])
            total = data.get("total", 0)

            if not items:
                break

            for item in items:
                track_id = item.get("id")
                if not track_id or track_id in seen_ids:
                    continue

                seen_ids.add(track_id)
                all_tracks.append({
                    "track_id": track_id,
                    "track_name": item.get("name"),
                    "duration_ms": item.get("duration_ms"),
                    "explicit": item.get("explicit", False),
                    "track_number": item.get("track_number"),
                    "disc_number": item.get("disc_number", 1),
                    "album_id": album_id,
                    "artist_id": artist_id,
                    "spotify_uri": item.get("uri"),
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                })
            
            offset += len(items)
            time.sleep(0.05)            

            # Stop condition when all tracks in the album are collected
            if offset >= total:
                break 
        return all_tracks

# -------------------------------------------------------------
# Test Block
# -------------------------------------------------------------
if __name__ == "__main__":
     # 1. Fetch real albums for The Weeknd using our working extractor
    album_extractor = AlbumExtractor()
    albums = album_extractor.extract_albums("1Xyo4u8uXC1ZmMpatF05PJ", "The Weeknd")
        
    if albums:
        sample_album = albums[0]
        print(
            f"\nTesting TrackExtractor on real album: {sample_album['album_name']}"
            f" (ID: {sample_album['album_id']})..."
        )

        # 2. Extract tracks for this verified album
        extractor = TrackExtractor()
        tracks = extractor.extract_track(
            sample_album["album_id"], sample_album["artist_id"]
        )
        print(
            f"✓ Total tracks extracted for '{sample_album['album_name']}':"
            f" {len(tracks)}"
        )

        if tracks:
            print(
                f"✓ First track: #{tracks[0]['track_number']}"
                f" {tracks[0]['track_name']} ({tracks[0]['duration_ms']} ms,"
                f" explicit={tracks[0]['explicit']})"
            )
            print(
                f"✓ Last track:  #{tracks[-1]['track_number']}"
                f" {tracks[-1]['track_name']} ({tracks[-1]['duration_ms']} ms)"
            )