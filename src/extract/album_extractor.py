import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure repository root is in sys.path when running script directly
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.extract.spotify_client import SpotifyClient


class AlbumExtractor:

    def __init__(self, client: Optional[SpotifyClient] = None):
        self.client = client or SpotifyClient()

    def extract_albums(
        self,
        artist_id: str,
        artist_name: str = "",
        since_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Paginates through an artist's discography using Spotify API valid batch size (limit=10).
        Optionally filters releases on or after `since_date` (YYYY-MM-DD) to minimize downstream calls.
        """
        all_albums = []
        seen_ids = set()
        offset = 0

        # Fast-path incremental extraction: when since_date is provided, query groups
        # individually ('album', 'single') and break immediately on the first release older than since_date.
        if since_date:
            groups = ["album", "single"]
            for grp in groups:
                offset = 0
                while True:
                    data = self.client.get(
                        f"v1/artists/{artist_id}/albums",
                        params={
                            "include_groups": grp,
                            "limit": 10,
                            "offset": offset,
                        },
                    )
                    items = data.get("items", [])
                    total = data.get("total", 0)
                    if not items:
                        break

                    hit_older = False
                    for item in items:
                        album_id = item.get("id")
                        if not album_id or album_id in seen_ids:
                            continue

                        release_date = item.get("release_date", "")
                        if release_date and release_date < since_date:
                            hit_older = True
                            break

                        seen_ids.add(album_id)
                        images = item.get("images", [])
                        image_url = images[0]["url"] if images else None

                        all_albums.append({
                            "album_id": album_id,
                            "album_name": item.get("name"),
                            "album_type": item.get("album_type"),
                            "release_date": release_date,
                            "release_date_precision": item.get("release_date_precision"),
                            "total_tracks": item.get("total_tracks"),
                            "artist_id": artist_id,
                            "artist_name": artist_name,
                            "spotify_uri": item.get("uri"),
                            "image_url": image_url,
                            "extracted_at": datetime.now(timezone.utc).isoformat(),
                        })

                    if hit_older or offset + len(items) >= total:
                        break
                    offset += len(items)
                    time.sleep(0.1)
            return all_albums

        # Full discography extraction (initial load or backfill)
        while True:
            data = self.client.get(
                f"v1/artists/{artist_id}/albums",
                params={
                    "include_groups": "album,single,compilation",
                    "limit": 10,
                    "offset": offset,
                },
            )
            items = data.get("items", [])
            total = data.get("total", 0)

            if not items:
                break 
            
            for item in items:
                album_id = item.get("id")
                if not album_id or album_id in seen_ids:
                    continue 

                seen_ids.add(album_id)
                images = item.get("images", [])
                image_url = images[0]["url"] if images else None

                all_albums.append({
                    "album_id": album_id,
                    "album_name": item.get("name"),
                    "album_type": item.get("album_type"),
                    "release_date": item.get("release_date", ""),
                    "release_date_precision": item.get("release_date_precision"),
                    "total_tracks": item.get("total_tracks"),
                    "artist_id": artist_id,
                    "artist_name": artist_name,
                    "spotify_uri": item.get("uri"),
                    "image_url": image_url,
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                })

            offset += len(items)
            time.sleep(0.2)  # Polite throttle to stay well below Spotify burst rate limits
            
            # Stop once all items in the discography are collected
            if offset >= total:
                break
        return all_albums

# -------------------------------------------------------------
# Test Block
# -------------------------------------------------------------
if __name__ == "__main__":
  extractor = AlbumExtractor()
  # The Weeknd artist ID: 1Xyo4u8uXC1ZmMpatF05PJ
  albums = extractor.extract_albums("1Xyo4u8uXC1ZmMpatF05PJ", "The Weeknd")
  print(f"✓ Total albums extracted for The Weeknd: {len(albums)}")
  if albums:
    print(f"✓ First album: {albums[0]['album_name']} ({albums[0]['release_date']})")
    print(f"✓ Last album: {albums[-1]['album_name']} ({albums[-1]['release_date']})")



