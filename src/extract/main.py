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
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# Ensure UTF-8 output encoding on Windows consoles
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Ensure repository root is in sys.path when running as a script directly
# (python src/extract/main.py) or via -m (python -m src.extract.main)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.extract.spotify_client import SpotifyClient
from src.extract.artist_extractor import ArtistExtractor
from src.extract.album_extractor import AlbumExtractor
from src.extract.track_extractor import TrackExtractor


# ---------------------------------------------------------------------------
#  Path-Traversal Protection (SonarCloud pythonsecurity:S8707)
# ---------------------------------------------------------------------------
def _safe_resolve_path(user_path: str, *, label: str = "path") -> Path:
    """Resolve *user_path* and ensure it stays within the project root.

    Raises ``ValueError`` when the resolved path escapes the project
    boundary — defending against path-traversal via CLI arguments.
    """
    resolved = (_PROJECT_ROOT / user_path).resolve()
    if not resolved.is_relative_to(_PROJECT_ROOT):
        raise ValueError(
            f"Security: {label} '{user_path}' resolves outside project root."
        )
    return resolved


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
    since_date: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Orchestrates end-to-end extraction across our target artist cohort,
    injects snapshot_date audit field, and saves partitioned JSON in data/raw/.
    Supports per-artist checkpointing and incremental state preservation.
    """
    start_time = time.time()
    artists_to_extract = target_artists or DEFAULT_TARGET_ARTISTS
    current_snapshot = snapshot_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Sanitize user-supplied output_dir against path traversal
    safe_output = _safe_resolve_path(output_dir, label="output-dir")

    print("=" * 70)
    print("🎵 SPOTIFY CATALOG EXTRACTION ENGINE")
    print(f"📅 Snapshot Date     : {current_snapshot}")
    print(f"🎯 Target Cohort     : {len(artists_to_extract)} artists -> {artists_to_extract}")
    print(f"📁 Output Base       : {safe_output}")
    print(f"🔄 Reset Checkpoint  : {reset_checkpoint}")
    if since_date:
        print(f"⏱️ Since Date Filter : releases >= {since_date}")
    print("=" * 70)

    # 1. Ensure Output & Checkpoint Directories Exist
    artists_dir = str(safe_output / "artists")
    albums_dir = str(safe_output / "albums")
    tracks_dir = str(safe_output / "tracks")
    checkpoint_dir = str(safe_output / ".checkpoints")

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
        prior_snapshot_date = None
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
        else:
            # Look for verified prior snapshot from watermark manager to inherit base catalog
            if os.path.exists(artists_dir):
                try:
                    from src.orchestration.watermark_manager import WatermarkManager
                    wm = WatermarkManager()
                    last_watermark = wm.get_last_processed_date("spotify_medallion_pipeline")
                    if (
                        last_watermark != "1970-01-01" 
                        and last_watermark < current_snapshot 
                        and os.path.exists(os.path.join(artists_dir, f"artists_{last_watermark}.json"))
                    ):
                        prior_snapshot_date = last_watermark
                except Exception:
                    pass

                if not prior_snapshot_date:
                    prior_files = sorted([
                        f for f in os.listdir(artists_dir) 
                        if f.startswith("artists_") and f.endswith(".json") and f < f"artists_{current_snapshot}.json"
                    ])
                    if prior_files:
                        latest_prior = prior_files[-1]
                        prior_snapshot_date = latest_prior.replace("artists_", "").replace(".json", "")

                if prior_snapshot_date:
                    prior_artists_file = os.path.join(artists_dir, f"artists_{prior_snapshot_date}.json")
                    prior_albums_file = os.path.join(albums_dir, f"albums_{prior_snapshot_date}.json")
                    prior_tracks_file = os.path.join(tracks_dir, f"tracks_{prior_snapshot_date}.json")
                    try:
                        with open(prior_artists_file, "r", encoding="utf-8") as f:
                            all_artists = json.load(f)
                        with open(prior_albums_file, "r", encoding="utf-8") as f:
                            all_albums = json.load(f)
                        with open(prior_tracks_file, "r", encoding="utf-8") as f:
                            all_tracks = json.load(f)
                        if not since_date:
                            since_date = prior_snapshot_date
                        print(f"📦 Inherited base catalog from prior snapshot '{prior_snapshot_date}': {len(all_artists)} artists, {len(all_albums)} albums, {len(all_tracks)} tracks.")
                        print(f"⚡ Incremental mode enabled: only scanning releases since {since_date}.")
                    except Exception as e:
                        print(f"⚠️ Could not load prior snapshot ({e}), starting fresh.")
                        all_artists, all_albums, all_tracks = [], [], []

    # 3. Initialize Client & Extractors
    client = SpotifyClient()
    artist_extractor = ArtistExtractor(client=client)
    album_extractor = AlbumExtractor(client=client)
    track_extractor = TrackExtractor(client=client)

    # Build historical album_id -> tracks cache to avoid re-fetching unchanged albums
    cached_tracks_by_album: Dict[str, List[Dict[str, Any]]] = {}
    for t in all_tracks:
        alb_id = t.get("album_id")
        if alb_id:
            cached_tracks_by_album.setdefault(alb_id, []).append(t)

    # If current run has no cached tracks, scan historical raw tracks files
    if not cached_tracks_by_album and os.path.exists(tracks_dir):
        for fname in sorted(os.listdir(tracks_dir), reverse=True):
            if fname.endswith(".json") and fname != os.path.basename(tracks_file):
                try:
                    with open(os.path.join(tracks_dir, fname), "r", encoding="utf-8") as f:
                        prior_tracks = json.load(f)
                        for t in prior_tracks:
                            alb_id = t.get("album_id")
                            if alb_id and alb_id not in cached_tracks_by_album:
                                cached_tracks_by_album.setdefault(alb_id, []).append(t)
                except Exception:
                    pass
                if cached_tracks_by_album:
                    print(f"📦 Populated track cache with {len(cached_tracks_by_album):,} albums from historical raw state.")
                    break

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

            # Step B: Paginate Discography Albums/Singles (limit=50, with optional since_date filter)
            albums = album_extractor.extract_albums(
                artist_id=artist_id,
                artist_name=canonical_name,
                since_date=since_date,
            )
            for album in albums:
                album["snapshot_date"] = current_snapshot
            print(f"    ✓ Extracted {len(albums)} albums/singles")

            # Step C: Paginate Tracks for each Album (Cache-Aware: 0 API calls for existing albums)
            artist_tracks: List[Dict[str, Any]] = []
            cached_hits = 0
            api_fetches = 0

            for album in albums:
                album_id = album["album_id"]
                if album_id in cached_tracks_by_album and not reset_checkpoint:
                    # Cache Hit: reuse existing tracks, stamping current snapshot_date
                    cached_hits += 1
                    for track in cached_tracks_by_album[album_id]:
                        track_copy = dict(track)
                        track_copy["snapshot_date"] = current_snapshot
                        artist_tracks.append(track_copy)
                else:
                    # Cache Miss (New Release): Fetch from Spotify Web API
                    api_fetches += 1
                    tracks = track_extractor.extract_track(
                        album_id=album_id,
                        artist_id=artist_id,
                    )
                    for track in tracks:
                        track["snapshot_date"] = current_snapshot
                        artist_tracks.append(track)

            print(f"    ✓ Extracted {len(artist_tracks)} tracks ({cached_hits} albums from cache, {api_fetches} newly fetched)")

            # Step D: Atomic Flush for Completed Artist
            # Find existing historical albums and tracks for this artist from base catalog
            existing_artist_albums = [alb for alb in all_albums if alb.get("artist_name") == artist_name]
            existing_artist_tracks = [t for t in all_tracks if t.get("artist_id") == artist_id]

            # Merge: keep existing, add new ones that were newly discovered
            seen_alb_ids = {alb["album_id"] for alb in existing_artist_albums if "album_id" in alb}
            for alb in albums:
                if alb.get("album_id") not in seen_alb_ids:
                    existing_artist_albums.append(alb)
                    seen_alb_ids.add(alb.get("album_id"))

            seen_trk_ids = {t["track_id"] for t in existing_artist_tracks if "track_id" in t}
            for trk in artist_tracks:
                if trk.get("track_id") not in seen_trk_ids:
                    existing_artist_tracks.append(trk)
                    seen_trk_ids.add(trk.get("track_id"))

            # Stamp current snapshot_date on all records for this snapshot partition
            for alb in existing_artist_albums:
                alb["snapshot_date"] = current_snapshot
            for trk in existing_artist_tracks:
                trk["snapshot_date"] = current_snapshot

            # Rebuild clean cumulative lists
            all_artists = [a for a in all_artists if a.get("artist_name") != artist_name]
            all_albums = [alb for alb in all_albums if alb.get("artist_name") != artist_name]
            all_tracks = [t for t in all_tracks if t.get("artist_id") != artist_id]

            all_artists.append(artist_record)
            all_albums.extend(existing_artist_albums)
            all_tracks.extend(existing_artist_tracks)

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
            if "RateLimit" in type(e).__name__ or "429" in str(e):
                print("    ⏳ Rate limit encountered. Escalating to orchestrator retry...")
                raise e
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
    print(f"• Total Spotify API Calls : {client.request_count} (Measured telemetry)")

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
        "api_calls_count": client.request_count,
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
    parser.add_argument(
        "--since-date",
        type=str,
        default=None,
        help="Optional release date filter (YYYY-MM-DD) to only extract albums released on or after this date.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_extraction(
        target_artists=args.artists,
        output_dir=args.output_dir,
        snapshot_date=args.snapshot_date,
        reset_checkpoint=args.reset_checkpoint,
        since_date=args.since_date,
    )

# Reusable module alias
main = run_extraction