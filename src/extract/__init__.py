"""Spotify Extraction Package.

Exposes core extractors, API client, and orchestrator.
"""

from src.extract.spotify_client import SpotifyClient
from src.extract.artist_extractor import ArtistExtractor
from src.extract.album_extractor import AlbumExtractor
from src.extract.track_extractor import TrackExtractor

__all__ = [
    "SpotifyClient",
    "ArtistExtractor",
    "AlbumExtractor",
    "TrackExtractor",
]
