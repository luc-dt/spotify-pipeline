from datetime import datetime, timezone
from typing import Any, Dict, Optional
from src.extract.spotify_client import SpotifyClient

class ArtistExtractor:

    def __init__(self, client: Optional[SpotifyClient] = None):
        # If client is passed, use it; otherwise create a new instance
        self.client = client or SpotifyClient()

    def extract_artist(self, artist_name: str) -> Dict[str, Any]:
        """Queries Spotify /v1/search for an artist name and returns clean metadata."""
        data = self.client.get(
            "v1/search", params={"q": artist_name, "type": "artist", "limit": 1}
        )
        items = data.get("artists", {}).get("items", [])
        if not items:
            raise ValueError(f"No artist fount for query: '{artist_name}'")

        artist_item = items[0] 

        # Get highest-resolution image safety if available
        images = artist_item.get("images", [])
        image_url = images[0]["url"] if images else None

        # Current UTC timestamp in ISO 8601 format
        extracted_at = datetime.now(timezone.utc).isoformat()
        
        return {
            "artist_id": artist_item["id"],
            "artist_name": artist_item["name"],
            "spotify_uri": artist_item["uri"],
            "image_url": image_url,
            "genres": artist_item.get("genres", []),
            "extracted_at": extracted_at,
        }

# -------------------------------------------------------------
# Test Block
# -------------------------------------------------------------
if __name__ == "__main__":
  extractor = ArtistExtractor()
  result = extractor.extract_artist("The Weeknd")
  print("✓ Extracted Artist Record:")
  print(result)
