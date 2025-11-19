from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

from pytubefix import YouTube
# from pytubefix.exceptions import PytubeError


class YoutubeVideoDownloader:
    """
    Download a single YouTube video in the highest available resolution
    to a local directory.

    Usage:
        downloader = YouTubeVideoDownloader(output_dir="downloads")
        file_path = downloader.download("https://www.youtube.com/watch?v=VIDEO_ID")
    """

    def __init__(
        self, output_dir: str | os.PathLike = "./downloads", overwrite: bool = False
    ):
        """
        :param output_dir: Directory where videos will be saved.
        :param overwrite: If True, existing files with the same name are overwritten.
        """
        self.output_dir = Path(output_dir)
        self.overwrite = overwrite
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def download(self, url: str, filename: Optional[str] = None) -> str:
        """
        Download the YouTube video at `url` in the highest resolution.

        :param url: Full YouTube video URL.
        :param filename: Optional base filename (without extension). If None,
                         the video title will be used (sanitized).
        :return: The full path of the downloaded file as a string.
        :raises ValueError: For invalid URL or if no suitable stream is found.
        :raises RuntimeError: For download errors.
        """
        try:
            yt = YouTube(url)
        except Exception as e:
            raise ValueError(f"Failed to initialize YouTube object: {e}") from e

        # Choose the highest-resolution *progressive* stream
        # (includes both audio and video).
        stream = yt.streams.get_highest_resolution()
        if stream is None:
            raise ValueError("No suitable video stream found for this URL.")

        # Determine filename
        base_filename = filename or self._sanitize_filename(yt.title or "youtube_video")
        final_path = self._target_path_with_extension(
            base_filename, stream.subtype or "mp4"
        )

        # Handle overwrite behavior
        if final_path.exists() and not self.overwrite:
            # Just return existing path if we don't want to overwrite
            return str(final_path)

        try:
            # pytube handles the real download
            stream.download(
                output_path=str(self.output_dir),
                filename=final_path.name,  # includes extension
            )
        except Exception as e:
            raise RuntimeError(f"Failed to download video: {e}") from e

        return str(final_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        """
        Make sure the filename is filesystem-safe.
        """
        # Replace path separators and illegal characters with underscores
        name = re.sub(r"[\\/]", "_", name)
        # Remove other problematic characters
        name = re.sub(r'[:*?"<>|]+', "", name)
        name = name.strip()
        return name or "youtube_video"

    def _target_path_with_extension(self, base_filename: str, extension: str) -> Path:
        """
        Build the final target file path (including extension).
        """
        if not extension.startswith("."):
            extension = "." + extension
        return self.output_dir / f"{base_filename}{extension}"
