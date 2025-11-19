from pytubefix import Channel
import json
import re
import os
import time
from datetime import datetime


def find_last_completed_batch(output_dir, safe_name):
    """
    Find the highest completed batch number
    Returns the batch number to start from (last_completed + 1)
    """
    if not os.path.exists(output_dir):
        return 1  # Start from batch 1 if directory doesn't exist

    completed_batches = []

    # Look for existing batch files
    for filename in os.listdir(output_dir):
        if filename.startswith(f"{safe_name}_") and filename.endswith(".json"):
            try:
                # Extract batch number from filename like "AZ_Alkmaar_videos_005.json"
                batch_part = filename.replace(f"{safe_name}_", "").replace(".json", "")
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


def validate_batch_file(filepath, min_success_rate=0.8):
    """
    Check if a batch file is complete and valid
    min_success_rate: Minimum percentage of successful videos required (0.8 = 80%)
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
            return success_rate >= min_success_rate

        return False

    except Exception:
        return False


def load_channel_safely(channel_url, max_retries=3):
    """
    Safely load channel with retries and error handling
    """
    for attempt in range(max_retries):
        try:
            print(f"Loading channel (attempt {attempt + 1}/{max_retries})...")
            channel = Channel(channel_url)
            youtube_objects = list(channel.video_urls)

            if len(youtube_objects) == 0:
                raise Exception("No videos found in channel")

            print(
                f"✅ Successfully loaded {len(youtube_objects)} videos from {channel.channel_name}"
            )
            return channel, youtube_objects

        except Exception as e:
            print(f"❌ Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Progressive delay: 5, 10, 15 seconds
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                print("All attempts failed!")
                raise e


def process_single_batch(
    youtube_objects,
    batch_num,
    batch_size,
    channel_info,
    output_dir,
    safe_name,
    max_retries=2,
):
    """
    Process a single batch with error recovery
    Returns (success: bool, error_count: int, retry_needed: bool)
    """

    start_idx = (batch_num - 1) * batch_size
    end_idx = min(start_idx + batch_size, len(youtube_objects))
    current_batch = youtube_objects[start_idx:end_idx]

    batch_filename = f"{safe_name}_{batch_num:03d}.json"
    batch_filepath = os.path.join(output_dir, batch_filename)

    print(f"Batch {batch_num} | Videos {start_idx + 1:4d}-{end_idx:4d} | Processing...")

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
            except:
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
            consecutive_errors = 0  # Reset consecutive error counter

        except Exception as e:
            print("!", end="", flush=True)  # Error indicator
            error_count += 1
            consecutive_errors += 1

            # Create error entry
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
                print(f"\n⚠️  {consecutive_errors} consecutive errors detected!")
                return False, error_count, True  # Signal retry needed

            continue

    print("] ✓")

    # Check error rate
    success_rate = (
        (len(batch_videos) - error_count) / len(batch_videos) if batch_videos else 0
    )

    # If error rate is too high, suggest retry
    if success_rate < 0.7:  # Less than 70% success
        print(
            f"⚠️  High error rate: {error_count}/{len(batch_videos)} errors ({success_rate:.1%} success)"
        )
        return False, error_count, True  # Signal retry needed

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
        "channel_info": channel_info,
        "videos": batch_videos,
    }

    # Save batch file
    with open(batch_filepath, "w", encoding="utf-8") as f:
        json.dump(batch_data, f, indent=2, ensure_ascii=False)

    # Show file info
    file_size = os.path.getsize(batch_filepath)
    successful_videos = len(batch_videos) - error_count
    print(
        f"   ✅ Saved: {batch_filename} ({successful_videos}/{len(batch_videos)} videos, {file_size // 1024:.0f} KB)"
    )

    if error_count > 0:
        print(f"   ⚠️  {error_count} errors in this batch")

    return True, error_count, False  # Success, no retry needed


def run_batch_export_with_error_recovery():
    """
    Batch export with comprehensive error recovery
    """

    # Settings
    channel_url = "https://www.youtube.com/@AZVideoArchief"
    batch_size = 50
    output_dir = "./video_batches"
    safe_name = "AZ_Alkmaar_videos"
    max_batch_retries = 3

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    print("🔴⚪ AZTV Video Batch Export with Error Recovery")
    print("=" * 60)

    # Initial channel load
    try:
        channel, youtube_objects = load_channel_safely(channel_url)
    except Exception as e:
        print(f"❌ Failed to load channel: {e}")
        return

    total_videos = len(youtube_objects)
    total_batches = (total_videos + batch_size - 1) // batch_size

    print(f"Channel: {channel.channel_name}")
    print(f"Total videos: {total_videos}")
    print(f"Total batches: {total_batches}")

    # Channel info for saving in batches
    channel_info = {
        "name": channel.channel_name,
        "url": channel_url,
        "total_videos_in_channel": total_videos,
    }

    # Check for existing batches and find where to resume
    start_batch = find_last_completed_batch(output_dir, safe_name)

    if start_batch > 1:
        print(f"🔄 RESUMING from batch {start_batch}")
    else:
        print("🚀 STARTING fresh export")

    print(f"📁 Output: {output_dir}/")
    print("=" * 60)

    # Process batches with error recovery
    current_batch = start_batch

    while current_batch <= total_batches:
        batch_retry_count = 0
        batch_success = False

        # Retry loop for current batch
        while batch_retry_count < max_batch_retries and not batch_success:
            try:
                # Skip if file already exists and is valid
                batch_filename = f"{safe_name}_{current_batch:03d}.json"
                batch_filepath = os.path.join(output_dir, batch_filename)

                if os.path.exists(batch_filepath) and validate_batch_file(
                    batch_filepath
                ):
                    file_size = os.path.getsize(batch_filepath)
                    print(
                        f"Batch {current_batch:2d}/{total_batches} | SKIPPED (already exists, {file_size // 1024:.0f} KB)"
                    )
                    batch_success = True
                    break

                # Process the batch
                success, error_count, retry_needed = process_single_batch(
                    youtube_objects,
                    current_batch,
                    batch_size,
                    channel_info,
                    output_dir,
                    safe_name,
                )

                if success and not retry_needed:
                    batch_success = True
                else:
                    # Batch failed or needs retry
                    batch_retry_count += 1

                    if batch_retry_count < max_batch_retries:
                        print(
                            f"🔄 Batch {current_batch} needs retry ({batch_retry_count}/{max_batch_retries})"
                        )
                        print("   Reloading channel data...")

                        # Reload channel data to recover from any connection issues
                        try:
                            channel, youtube_objects = load_channel_safely(channel_url)
                            channel_info = {
                                "name": channel.channel_name,
                                "url": channel_url,
                                "total_videos_in_channel": len(youtube_objects),
                            }
                            print("   ✅ Channel data reloaded successfully")

                            # Wait a bit before retrying
                            time.sleep(5)

                        except Exception as e:
                            print(f"   ❌ Failed to reload channel: {e}")
                            break
                    else:
                        print(
                            f"❌ Batch {current_batch} failed after {max_batch_retries} retries"
                        )
                        # Continue with next batch rather than stopping completely
                        batch_success = True  # Allow continuation

            except Exception as e:
                print(f"❌ Unexpected error in batch {current_batch}: {e}")
                batch_retry_count += 1

                if batch_retry_count < max_batch_retries:
                    print(f"Retrying batch {current_batch}...")
                    time.sleep(5)
                else:
                    print(f"Giving up on batch {current_batch}")
                    batch_success = True  # Allow continuation

        # Move to next batch
        current_batch += 1

    print("=" * 60)
    print("🎉 EXPORT COMPLETED!")

    # Show final summary
    show_final_summary(output_dir, safe_name, total_batches)


def show_final_summary(output_dir, safe_name, expected_batches):
    """Show final export summary"""
    completed_files = []
    total_videos_exported = 0
    total_errors = 0

    for batch_num in range(1, expected_batches + 1):
        batch_filename = f"{safe_name}_{batch_num:03d}.json"
        batch_filepath = os.path.join(output_dir, batch_filename)

        if os.path.exists(batch_filepath):
            try:
                with open(batch_filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    video_count = len(data.get("videos", []))
                    error_count = data.get("batch_info", {}).get("error_count", 0)
                    total_videos_exported += video_count
                    total_errors += error_count
                    completed_files.append((batch_filename, video_count, error_count))
            except:
                pass

    print(f"✅ Completed {len(completed_files)}/{expected_batches} batch files")
    print(f"📊 Total videos exported: {total_videos_exported}")
    print(f"⚠️  Total errors encountered: {total_errors}")

    if total_errors > 0:
        success_rate = (
            (total_videos_exported - total_errors) / total_videos_exported
            if total_videos_exported > 0
            else 0
        )
        print(f"📈 Overall success rate: {success_rate:.1%}")

    print(f"📁 Location: {output_dir}/")


print("🚀 AZTV Video Batch Export with Error Recovery")
print("=" * 50)
print("Features:")
print("✅ Resume from where you left off")
print("✅ Automatic error recovery")
print("✅ Channel data reloading on errors")
print("✅ Batch retries with progressive delays")
print("✅ High error rate detection")
print()

run_batch_export_with_error_recovery()
