from pytubefix import Channel
import json
import re
import os
import time
from datetime import datetime


class YouTubeBatchExporter:
    """
    Generic batch exporter for YouTube video metadata.

    Responsibilities:
    - Load all videos from a YouTube channel.
    - Process them in batches.
    - Export metadata to JSON files.
    - Support resuming, basic error handling, and batch validation.
    """

    def __init__(
        self,
        channel_url: str,
        batch_size: int = 50,
        output_dir: str = "./video_batches",
        file_prefix: str | None = None,
        max_batch_retries: int = 3,
        min_success_rate: float = 0.8,
    ):
        """
        :param channel_url: Full URL of the YouTube channel.
        :param batch_size: Number of videos per JSON file.
        :param output_dir: Directory where JSON batch files are stored.
        :param file_prefix: Filename prefix for batch files. If None, it will be
                            derived from the channel name (safe-ified) after loading.
        :param max_batch_retries: How many times to retry a failing batch.
        :param min_success_rate: Minimum success rate for a batch file to be
                                 considered valid when resuming.
        """
        self.channel_url = channel_url
        self.batch_size = batch_size
        self.output_dir = output_dir
        self.file_prefix = file_prefix
        self.max_batch_retries = max_batch_retries
        self.min_success_rate = min_success_rate

        # Filled when loading the channel
        self.channel: Channel | None = None
        self.youtube_objects: list = []
        self.channel_info: dict = {}

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

    # -------------------------------------------------------------------------
    # Helper methods
    # -------------------------------------------------------------------------

    @staticmethod
    def _make_safe_filename_part(name: str) -> str:
        """Create a filesystem-safe slug from the given name."""
        name = name.strip().replace(" ", "_")
        # keep only letters, digits, underscore and dash
        return re.sub(r"[^A-Za-z0-9_-]+", "", name) or "youtube_channel"

    def _batch_file_prefix(self) -> str:
        """
        Return the prefix to use for batch files.
        If file_prefix is not set, derive it from the channel name.
        """
        if self.file_prefix:
            return self.file_prefix

        if self.channel is not None and getattr(self.channel, "channel_name", None):
            return self._make_safe_filename_part(self.channel.channel_name)

        # fallback if channel is not yet loaded
        return "youtube_channel"

    def _batch_filename(self, batch_num: int) -> str:
        return f"{self._batch_file_prefix()}_{batch_num:03d}.json"

    def find_last_completed_batch(self) -> int:
        """
        Find the highest completed batch number in output_dir for this prefix.
        Returns the batch number to start from (last_completed + 1).
        """
        prefix = self._batch_file_prefix()

        if not os.path.exists(self.output_dir):
            return 1  # Start from batch 1 if directory doesn't exist

        completed_batches = []

        # Look for existing batch files
        for filename in os.listdir(self.output_dir):
            if filename.startswith(f"{prefix}_") and filename.endswith(".json"):
                try:
                    # Extract batch number from filename like "<prefix>_005.json"
                    batch_part = filename.replace(f"{prefix}_", "").replace(".json", "")
                    batch_num = int(batch_part)
                    completed_batches.append(batch_num)
                except ValueError:
                    continue

        if completed_batches:
            last_completed = max(completed_batches)
            print(f"Found existing batches: {sorted(completed_batches)}")
            print(f"Last completed batch: {last_completed}")
            return last_completed + 1
        else:
            return 1  # No existing batches found

    def validate_batch_file(self, filepath: str) -> bool:
        """
        Check if a batch file is complete and valid.
        Uses self.min_success_rate as minimum success threshold.
        """
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Check if it has the expected structure
            if "batch_info" in data and "videos" in data:
                videos = data["videos"]
                if not videos:
                    return False

                # Count successful vs error videos
                error_videos = len(
                    [v for v in videos if v.get("title", "").startswith("ERROR:")]
                )
                success_videos = len(videos) - error_videos
                success_rate = success_videos / len(videos) if videos else 0

                # File is valid if success rate is above threshold
                return success_rate >= self.min_success_rate

            return False

        except Exception:
            return False

    def load_channel_safely(self, max_retries: int = 3):
        """
        Safely load channel with retries and error handling.
        Populates self.channel, self.youtube_objects and self.channel_info.
        """

        for attempt in range(max_retries):
            try:
                print(f"Loading channel (attempt {attempt + 1}/{max_retries})...")
                channel = Channel(self.channel_url)
                youtube_objects = list(channel.video_urls)

                if len(youtube_objects) == 0:
                    raise Exception("No videos found in channel")

                print(
                    f"Successfully loaded {len(youtube_objects)} videos from '{channel.channel_name}'"
                )

                self.channel = channel
                self.youtube_objects = youtube_objects

                self.channel_info = {
                    "name": channel.channel_name,
                    "url": self.channel_url,
                    "total_videos_in_channel": len(youtube_objects),
                }

                # If no explicit prefix was provided, we can now derive it
                if self.file_prefix is None:
                    self.file_prefix = self._make_safe_filename_part(
                        channel.channel_name or "youtube_channel"
                    )

                return

            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5  # Progressive delay: 5, 10, 15 sec
                    print(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    print("All attempts to load channel failed.")
                    raise e

    def process_single_batch(
        self,
        batch_num: int,
        max_retries: int = 2,  # kept for compatibility; not used inside
    ):
        """
        Process a single batch with error recovery.
        Returns (success: bool, error_count: int, retry_needed: bool)
        """

        start_idx = (batch_num - 1) * self.batch_size
        end_idx = min(start_idx + self.batch_size, len(self.youtube_objects))
        current_batch = self.youtube_objects[start_idx:end_idx]

        batch_filename = self._batch_filename(batch_num)
        batch_filepath = os.path.join(self.output_dir, batch_filename)

        print(
            f"Batch {batch_num} | Videos {start_idx + 1:4d}-{end_idx:4d} | Processing..."
        )

        # Progress bar setup
        progress_interval = max(1, len(current_batch) // 50)
        print("Progress: [", end="", flush=True)

        batch_videos = []
        error_count = 0
        consecutive_errors = 0

        # Process each video
        for i, yt_obj in enumerate(current_batch):
            try:
                # Show progress
                if i % progress_interval == 0 or i == len(current_batch) - 1:
                    print("█", end="", flush=True)

                # Extract video info
                obj_repr = repr(yt_obj)
                match = re.search(r"videoId=([a-zA-Z0-9_-]{11})", obj_repr)
                video_id = match.group(1) if match else f"unknown_{start_idx + i}"

                # Get publish date
                publish_date = None
                try:
                    if hasattr(yt_obj, "publish_date") and yt_obj.publish_date:
                        publish_date = yt_obj.publish_date.isoformat()
                except Exception:
                    pass

                # Create video entry
                video_data = {
                    "title": getattr(yt_obj, "title", "Unknown Title"),
                    "duration": getattr(yt_obj, "length", 0),
                    "video_id": video_id,
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "publish_date": publish_date,
                }

                batch_videos.append(video_data)
                consecutive_errors = 0  # Reset

            except Exception as e:
                print("!", end="", flush=True)  # Error indicator
                error_count += 1
                consecutive_errors += 1

                batch_videos.append(
                    {
                        "title": f"ERROR: Failed to process video {start_idx + i + 1}",
                        "duration": 0,
                        "video_id": f"error_{start_idx + i}",
                        "url": f"ERROR: {str(e)[:100]}",
                        "publish_date": None,
                    }
                )

                # If too many consecutive errors, suggest a retry
                if consecutive_errors >= 10:
                    print(f"\n{consecutive_errors} consecutive errors detected.")
                    return False, error_count, True  # retry_needed = True

                continue

        print("] done")

        # Check error rate
        success_rate = (
            (len(batch_videos) - error_count) / len(batch_videos) if batch_videos else 0
        )

        # If error rate is too high, suggest retry
        if success_rate < 0.7:  # Less than 70% success
            print(
                f"High error rate: {error_count}/{len(batch_videos)} errors "
                f"({success_rate:.1%} success)."
            )
            return False, error_count, True

        # Create batch data
        batch_data = {
            "batch_info": {
                "batch_number": batch_num,
                "videos_in_batch": len(batch_videos),
                "video_range": f"{start_idx + 1}-{end_idx}",
                "export_date": datetime.now().isoformat(),
                "error_count": error_count,
                "success_rate": success_rate,
            },
            "channel_info": self.channel_info,
            "videos": batch_videos,
        }

        # Save batch file
        with open(batch_filepath, "w", encoding="utf-8") as f:
            json.dump(batch_data, f, indent=2, ensure_ascii=False)

        # Show file info
        file_size = os.path.getsize(batch_filepath)
        successful_videos = len(batch_videos) - error_count
        print(
            f"Saved: {batch_filename} "
            f"({successful_videos}/{len(batch_videos)} videos, {file_size // 1024:.0f} KB)"
        )

        if error_count > 0:
            print(f"{error_count} errors in this batch.")

        return True, error_count, False  # Success, no retry needed

    def show_final_summary(self, expected_batches: int):
        """Show final export summary"""
        completed_files = []
        total_videos_exported = 0
        total_errors = 0

        for batch_num in range(1, expected_batches + 1):
            batch_filename = self._batch_filename(batch_num)
            batch_filepath = os.path.join(self.output_dir, batch_filename)

            if os.path.exists(batch_filepath):
                try:
                    with open(batch_filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        video_count = len(data.get("videos", []))
                        error_count = data.get("batch_info", {}).get("error_count", 0)
                        total_videos_exported += video_count
                        total_errors += error_count
                        completed_files.append(
                            (batch_filename, video_count, error_count)
                        )
                except Exception:
                    pass

        print(f"Completed {len(completed_files)}/{expected_batches} batch files.")
        print(f"Total videos exported: {total_videos_exported}")
        print(f"Total errors encountered: {total_errors}")

        if total_errors > 0 and total_videos_exported > 0:
            success_rate = (
                total_videos_exported - total_errors
            ) / total_videos_exported
            print(f"Overall success rate: {success_rate:.1%}")

        print(f"Output directory: {os.path.abspath(self.output_dir)}")

    # -------------------------------------------------------------------------
    # Main public method
    # -------------------------------------------------------------------------

    def run(self):
        """
        Run the full batch export with error recovery:
        - Load channel
        - Determine starting batch (resume support)
        - Process all batches
        - Show summary
        """

        print("YouTube Video Metadata Batch Export")
        print("=" * 60)

        # Initial channel load
        try:
            self.load_channel_safely()
        except Exception as e:
            print(f"Failed to load channel: {e}")
            return

        total_videos = len(self.youtube_objects)
        total_batches = (total_videos + self.batch_size - 1) // self.batch_size

        print(f"Channel: {self.channel.channel_name}")
        print(f"Channel URL: {self.channel_url}")
        print(f"Total videos: {total_videos}")
        print(f"Batch size: {self.batch_size}")
        print(f"Total batches: {total_batches}")
        print(f"Output directory: {os.path.abspath(self.output_dir)}")
        print("=" * 60)

        # Check for existing batches and find where to resume
        start_batch = self.find_last_completed_batch()

        if start_batch > 1:
            print(f"Resuming from batch {start_batch}.")
        else:
            print("Starting fresh export.")

        print("=" * 60)

        # Process batches with error recovery
        current_batch = start_batch

        while current_batch <= total_batches:
            batch_retry_count = 0
            batch_success = False

            # Retry loop for current batch
            while batch_retry_count < self.max_batch_retries and not batch_success:
                try:
                    # Skip if file already exists and is valid
                    batch_filename = self._batch_filename(current_batch)
                    batch_filepath = os.path.join(self.output_dir, batch_filename)

                    if os.path.exists(batch_filepath) and self.validate_batch_file(
                        batch_filepath
                    ):
                        file_size = os.path.getsize(batch_filepath)
                        print(
                            f"Batch {current_batch:2d}/{total_batches} | "
                            f"SKIPPED (already exists, {file_size // 1024:.0f} KB)"
                        )
                        batch_success = True
                        break

                    # Process the batch
                    success, error_count, retry_needed = self.process_single_batch(
                        current_batch
                    )

                    if success and not retry_needed:
                        batch_success = True
                    else:
                        # Batch failed or needs retry
                        batch_retry_count += 1

                        if batch_retry_count < self.max_batch_retries:
                            print(
                                f"Batch {current_batch} needs retry "
                                f"({batch_retry_count}/{self.max_batch_retries})."
                            )
                            print("Reloading channel data...")

                            # Reload channel data to recover from connection issues
                            try:
                                self.load_channel_safely()
                                print("Channel data reloaded successfully.")
                                time.sleep(5)
                            except Exception as e:
                                print(f"Failed to reload channel: {e}")
                                break
                        else:
                            print(
                                f"Batch {current_batch} failed after "
                                f"{self.max_batch_retries} retries. Continuing."
                            )
                            batch_success = True  # Allow continuation

                except Exception as e:
                    print(f"Unexpected error in batch {current_batch}: {e}")
                    batch_retry_count += 1

                    if batch_retry_count < self.max_batch_retries:
                        print(f"Retrying batch {current_batch}...")
                        time.sleep(5)
                    else:
                        print(f"Giving up on batch {current_batch}. Continuing.")
                        batch_success = True  # Allow continuation

            # Move to next batch
            current_batch += 1

        print("=" * 60)
        print("Export completed.")
        self.show_final_summary(total_batches)


# Example usage (script mode)
if __name__ == "__main__":
    # Replace this URL with any YouTube channel URL you want to export.
    CHANNEL_URL = "https://www.youtube.com/@daveebbelaar"

    exporter = YouTubeBatchExporter(
        channel_url=CHANNEL_URL,
        batch_size=50,
        output_dir="./video_batches",
        # file_prefix="google_developers_videos",  # optional override
        max_batch_retries=3,
        min_success_rate=0.8,
    )
    exporter.run()
