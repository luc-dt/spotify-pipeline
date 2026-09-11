"""
src/extract/main.py
-------------------
Master orchestrator for extracting raw Spotify catalog data.
Features:
- CLI argument parsing via argparse (--snapshot-date, --artists, --output-dir, --reset-checkpoint)
- Persistent per-artist checkpointing (data/raw/.checkpoints/) to prevent re-fetching completed artists
- Incremental flush to disk after each completed artist for crash recovery and quota resilience
"""
import argparse
from datetime import datetime, timezone
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Set

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


def load_checkpoint(checkpoint_file: str) -> Set[str]:
    """Loads set of completed artist names from the snapshot checkpoint file."""
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data.get("completed_artists", []))
        except Exception as e:
            print(f"⚠️ Warning: Could not read checkpoint file {checkpoint_file}: {e}")
    return set()


def save_checkpoint(checkpoint_file: str, completed_artists: Set[str]) -> None:
    """Persists completed artist identifiers to disk for crash recovery."""
    os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump({"completed_artists": sorted(list(completed_artists))}, f, indent=2)


def run_extraction(
    target_artists: Optional[List[str]] = None,
    output_dir: str = "data/raw",
    snapshot_date: Optional[str] = None,
    reset_checkpoint: bool = False,
) -> Dict[str, Any]:
    """
    Orchestrates end-to-end extraction across our target artist cohort,
    injects snapshot_date audit field, and saves partitioned JSON in data/raw/.
    Supports per-artist checkpointing and incremental state preservation.
    """
    start_time = time.time()
    artists_to_extract = target_artists or DEFAULT_TARGET_ARTISTS
    current_snapshot = snapshot_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print("=" * 70)
    print("🎵 SPOTIFY CATALOG EXTRACTION ENGINE")
    print(f"📅 Snapshot Date     : {current_snapshot}")
    print(f"🎯 Target Cohort     : {len(artists_to_extract)} artists -> {artists_to_extract}")
    print(f"📁 Output Base       : {output_dir}")
    print(f"🔄 Reset Checkpoint  : {reset_checkpoint}")
    print("=" * 70)

    # 1. Ensure Output & Checkpoint Directories Exist
    artists_dir = os.path.join(output_dir, "artists")
    albums_dir = os.path.join(output_dir, "albums")
    tracks_dir = os.path.join(output_dir, "tracks")
    checkpoint_dir = os.path.join(output_dir, ".checkpoints")

    os.makedirs(artists_dir, exist_ok=True)
    os.makedirs(albums_dir, exist_ok=True)
    os.makedirs(tracks_dir, exist_ok=True)
    os.makedirs(checkpoint_dir, exist_ok=True)

    artists_file = os.path.join(artists_dir, f"artists_{current_snapshot}.json")
    albums_file = os.path.join(albums_dir, f"albums_{current_snapshot}.json")
    tracks_file = os.path.join(tracks_dir, f"tracks_{current_snapshot}.json")
    checkpoint_file = os.path.join(checkpoint_dir, f"checkpoint_{current_snapshot}.json")

    # 2. Manage Checkpoint & Pre-existing Data
    if reset_checkpoint:
        completed_artists: Set[str] = set()
        all_artists: List[Dict[str, Any]] = []
        all_albums: List[Dict[str, Any]] = []
        all_tracks: List[Dict[str, Any]] = []
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
        print("🧹 Checkpoint reset: starting extraction from scratch.")
    else:
        completed_artists = load_checkpoint(checkpoint_file)
        # Load accumulated data from disk if it exists
        all_artists = []
        all_albums = []
        all_tracks = []
        if os.path.exists(artists_file):
            try:
                with open(artists_file, "r", encoding="utf-8") as f:
                    all_artists = json.load(f)
                with open(albums_file, "r", encoding="utf-8") as f:
                    all_albums = json.load(f)
                with open(tracks_file, "r", encoding="utf-8") as f:
                    all_tracks = json.load(f)
                print(f"📦 Resuming from existing raw state: {len(all_artists)} artists, {len(all_albums)} albums, {len(all_tracks)} tracks already saved.")
            except Exception as e:
                print(f"⚠️ Error loading existing raw data ({e}), starting in-memory lists fresh.")
                all_artists, all_albums, all_tracks = [], [], []

    # 3. Initialize Client & Extractors
    client = SpotifyClient()
    artist_extractor = ArtistExtractor(client=client)
    album_extractor = AlbumExtractor(client=client)
    track_extractor = TrackExtractor(client=client)

    # 4. Iterate through each artist in our cohort
    for idx, artist_name in enumerate(artists_to_extract, start=1):
        if artist_name in completed_artists:
            print(f"\n[{idx}/{len(artists_to_extract)}] ⏩ [SKIP] '{artist_name}' is already completed in checkpoint.")
            continue

        print(f"\n[{idx}/{len(artists_to_extract)}] Ingesting: '{artist_name}'...")
        try:
            # Step A: Resolve Artist metadata
            artist_record = artist_extractor.extract_artist(artist_name)
            artist_record["snapshot_date"] = current_snapshot
            artist_id = artist_record["artist_id"]
            canonical_name = artist_record["artist_name"]
            print(f"    ✓ Artist: {canonical_name} (ID: {artist_id})")

            # Step B: Paginate Discography Albums/Singles
            albums = album_extractor.extract_albums(
                artist_id=artist_id,
                artist_name=canonical_name,
            )
            for album in albums:
                album["snapshot_date"] = current_snapshot
            print(f"    ✓ Extracted {len(albums)} albums/singles")

            # Step C: Paginate Tracks for each Album
            artist_tracks: List[Dict[str, Any]] = []
            for album in albums:
                album_id = album["album_id"]
                tracks = track_extractor.extract_track(
                    album_id=album_id,
                    artist_id=artist_id,
                )
                for track in tracks:
                    track["snapshot_date"] = current_snapshot
                    artist_tracks.append(track)

            print(f"    ✓ Extracted {len(artist_tracks)} tracks across discography releases")

            # Step D: Atomic Flush for Completed Artist
            # Remove any prior partial entries for this artist to avoid duplication
            all_artists = [a for a in all_artists if a.get("artist_name") != artist_name]
            all_albums = [alb for alb in all_albums if alb.get("artist_name") != artist_name]
            all_tracks = [t for t in all_tracks if t.get("artist_id") != artist_id]

            all_artists.append(artist_record)
            all_albums.extend(albums)
            all_tracks.extend(artist_tracks)

            # Flush updated state to raw JSON files
            with open(artists_file, "w", encoding="utf-8") as f:
                json.dump(all_artists, f, indent=2, ensure_ascii=False)
            with open(albums_file, "w", encoding="utf-8") as f:
                json.dump(all_albums, f, indent=2, ensure_ascii=False)
            with open(tracks_file, "w", encoding="utf-8") as f:
                json.dump(all_tracks, f, indent=2, ensure_ascii=False)

            # Mark artist as completed in checkpoint
            completed_artists.add(artist_name)
            save_checkpoint(checkpoint_file, completed_artists)
            print(f"    💾 Flushed '{canonical_name}' to disk & updated checkpoint.")

        except Exception as e:
            print(f"    ❌ Error extracting '{artist_name}': {e}")
            print("    ⏸️ Extraction paused. Re-run after quota resets to resume from this artist.")
            break

    elapsed = time.time() - start_time

    # 5. Output Execution Summary
    print("\n" + "=" * 70)
    print("🏁 EXTRACTION STATUS REPORT")
    print("=" * 70)
    print(f"• Total Artists Extracted : {len(all_artists):,} / {len(artists_to_extract)}")
    print(f"• Total Albums Extracted  : {len(all_albums):,}")
    print(f"• Total Tracks Extracted  : {len(all_tracks):,}")
    print(f"• Completed Artists       : {sorted(list(completed_artists))}")
    print(f"• Total Execution Time    : {elapsed:.2f} seconds")
    print("• Raw Data Files:")
    if os.path.exists(artists_file):
        print(f"    - Artists : {artists_file} ({os.path.getsize(artists_file)/1024:.1f} KB)")
    if os.path.exists(albums_file):
        print(f"    - Albums  : {albums_file} ({os.path.getsize(albums_file)/1024:.1f} KB)")
    if os.path.exists(tracks_file):
        print(f"    - Tracks  : {tracks_file} ({os.path.getsize(tracks_file)/1024:.1f} KB)")
    print("=" * 70)

    is_complete = len(completed_artists) == len(artists_to_extract)
    return {
        "status": "SUCCESS" if is_complete else "PARTIAL",
        "snapshot_date": current_snapshot,
        "artists_count": len(all_artists),
        "albums_count": len(all_albums),
        "tracks_count": len(all_tracks),
        "completed_artists": sorted(list(completed_artists)),
        "is_complete": is_complete,
        "elapsed_seconds": elapsed,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Spotify Catalog Raw JSON Extraction Engine")
    parser.add_argument(
        "--snapshot-date",
        type=str,
        default=None,
        help="Audit watermark partition date (YYYY-MM-DD). Defaults to UTC today.",
    )
    parser.add_argument(
        "--artists",
        nargs="+",
        default=None,
        help="Optional subset of artist names to extract (defaults to full 8-artist cohort).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/raw",
        help="Base directory to persist raw partitioned JSON payloads (default: 'data/raw').",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Ignore existing checkpoint and re-extract from scratch.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_extraction(
        target_artists=args.artists,
        output_dir=args.output_dir,
        snapshot_date=args.snapshot_date,
        reset_checkpoint=args.reset_checkpoint,
    )