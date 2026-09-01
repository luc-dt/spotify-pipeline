from datetime import datetime, timezone
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

# Ensure UTF-8 output encoding on Windows consoles
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from src.extract.spotify_client import SpotifyClient
from src.extract.artist_extractor import ArtistExtractor
from src.extract.album_extractor import AlbumExtractor
from src.extract.track_extractor import TrackExtractor

# Target Cohort for this extraction run
DEFAULT_TARGET_ARTISTS: List[str] = [
    "Taylor Swift",
    "The Weeknd",
    "Drake",
    "Ed Sheeran",
    "Billie Eilish",
    "Ariana Grande",
    "Coldplay",
    "BTS",
]

def run_extraction(
    target_artists: Optional[List[str]] = None,
    output_dir: str ="data/raw",
    snapshot_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Orchestrates end-to-end extration across our target artist cohort,
    injects snapshot_date audit field, and saves partitioned JSON in data/raw/.
    """
    start_time = time.time()
    artists_to_extract = target_artists or DEFAULT_TARGET_ARTISTS
    current_snapshot = snapshot_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print("=" * 70)
    print("🎵 SPOTIFY CATALOG EXTRACTION ENGINE ")
    print(f"📅 Snapshot Date : {current_snapshot}")
    print(
        f"🎯 Target Cohort : {len(artists_to_extract)} artists ->"
        f" {artists_to_extract}"
    )
    print(f"📁 Output Base   : {output_dir}")
    print("=" * 70)

    # 1. Initialize Client & Extractors (reusing one single connection session)
    client = SpotifyClient()
    artist_extractor = ArtistExtractor(client=client)
    album_extractor = AlbumExtractor(client=client)
    track_extractor = TrackExtractor(client=client)

    all_artists: List[Dict[str, Any]] = []
    all_albums: List[Dict[str, Any]] = []
    all_tracks: List[Dict[str, Any]] = []

    # 2. Iterate through each artist in out cohort.
    for idx, artist_name in enumerate(artists_to_extract, start=1):
        print(f"\n[{idx}/{len(artists_to_extract)}] Ingesting: '{artist_name}'...")
        try:
            # Step A: Resolve Artist metadata
            artist_record  = artist_extractor.extract_artist(artist_name)
            artist_record["snapshot_date"] = current_snapshot
            all_artists.append(artist_record)

            artist_id = artist_record["artist_id"]
            canonical_name = artist_record["artist_name"]
            print(f"    ✓ Artist: {canonical_name} (ID: {artist_id})")

            # Step B: Paginate Discography Album/Singles
            albums = album_extractor.extract_albums(
                artist_id=artist_id, 
                artist_name=canonical_name
            )
            for album in albums:
                album["snapshot_date"] = current_snapshot
                all_albums.append(album)

            print(f"    ✓ Extracted {len(albums)} albums/singles")

            # Step C: Paginate Tracks for each Album
            artist_tracks_count = 0
            for album in albums:
                album_id = album["album_id"]
                tracks = track_extractor.extract_track(
                    album_id=album_id,
                    artist_id=artist_id
                )
                for track in tracks:
                    track["snapshot_date"] = current_snapshot
                    all_tracks.append(track)
                artist_tracks_count  += len(tracks)
            
            print(
                f"    ✓ Extracted {artist_tracks_count} tracks across all"" discography releases")
        except Exception as e:
            print(f"    ❌ Error extracting '{artist_name}': {e}")

    # 3. Ensure Output Directories Exist
    artists_dir = os.path.join(output_dir, "artists")
    albums_dir = os.path.join(output_dir, "albums")
    tracks_dir = os.path.join(output_dir, "tracks")

    os.makedirs(artists_dir, exist_ok=True)
    os.makedirs(albums_dir, exist_ok=True)
    os.makedirs(tracks_dir, exist_ok=True)

    # 4. Write Partitioned Immutable JSON Payloads
    artists_file = os.path.join(artists_dir, f"artists_{current_snapshot}.json")
    albums_file = os.path.join(albums_dir, f"albums_{current_snapshot}.json")
    tracks_file = os.path.join(tracks_dir, f"tracks_{current_snapshot}.json")

    with open(artists_file, "w", encoding="utf-8") as f:
        json.dump(all_artists, f, indent=2, ensure_ascii=False)
    with open(albums_file, "w", encoding="utf-8") as f:
        json.dump(all_albums, f, indent=2, ensure_ascii=False)
    with open(tracks_file, "w", encoding="utf-8") as f:
        json.dump(all_tracks, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - start_time
    # 5. Output Execution Summary
    print("\n" + "=" * 70)
    print("🏁 EXTRACTION COMPLETE — SUMMARY REPORT")
    print("=" * 70)
    print(f"• Total Artists Extracted : {len(all_artists):,}")
    print(f"• Total Albums Extracted  : {len(all_albums):,}")
    print(f"• Total Tracks Extracted  : {len(all_tracks):,}")
    print(f"• Total Execution Time    : {elapsed:.2f} seconds")
    print("• Raw Data Files Saved:")
    print(
        f"    - Artists : {artists_file} ({os.path.getsize(artists_file)/1024:.1f}"
        " KB)"
    )
    print(
        f"    - Albums  : {albums_file} ({os.path.getsize(albums_file)/1024:.1f}"
        " KB)"
    )
    print(
        f"    - Tracks  : {tracks_file} ({os.path.getsize(tracks_file)/1024:.1f}"
        " KB)"
    )
    print("=" * 70)
    return {
        "status": "SUCCESS",
        "snapshot_date": current_snapshot,
        "artists_count": len(all_artists),
        "albums_count": len(all_albums),
        "tracks_count": len(all_tracks),
        "elapsed_seconds": elapsed,
    }

if __name__ == "__main__":
    import sys

    cli_artists = sys.argv[1:] if len(sys.argv) > 1 else None
    run_extraction(target_artists=cli_artists)