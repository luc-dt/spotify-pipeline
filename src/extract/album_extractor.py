from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from src.extract.spotify_client import SpotifyClient
import time

class AlbumExtractor:

    def __init__(self, client: Optional[SpotifyClient] = None):
        self.client = client or SpotifyClient()

    def extract_albums(self, artist_id:str, artist_name: str = "") -> List[Dict[str, Any]]:
        """Paginates through an artist's full discography (limit=10) and extracts albums & singles."""
        all_albums = []
        seen_ids = set()
        offset = 0

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
                    "release_date": item.get("release_date"),
                    "release_date_precision": item.get("release_date_precision"),
                    "total_tracks": item.get("total_tracks"),
                    "artist_id": artist_id,
                    "artist_name": artist_name,
                    "spotify_uri": item.get("uri"),
                    "image_url": image_url,
                    "extracted_at": datetime.now(timezone.utc).isoformat(),
                })

            offset += len(items)
            time.sleep(0.1)
            
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



