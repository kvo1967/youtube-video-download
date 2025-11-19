from pytubefix import Channel
import json
import re
import os
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


def validate_batch_file(filepath):
    """
    Check if a batch file is complete and valid
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Check if it has the expected structure
        if "batch_info" in data and "videos" in data:
            video_count = len(data["videos"])
            expected_count = data.get("batch_info", {}).get("videos_in_batch", 0)

            # File is valid if it has videos and structure is correct
            return video_count > 0 and video_count == expected_count

        return False

    except Exception:
        return False


def run_batch_export_with_resume():
    """
    Batch export with resume functionality
    Skips existing valid batches and continues from where it left off
    """

    # Settings
    channel_url = "https://www.youtube.com/@AZTV"
    batch_size = 100
    output_dir = "./video_batches"
    safe_name = "AZ_Alkmaar_videos"

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    print("🔴⚪ Loading AZTV channel...")
    channel = Channel(channel_url)
    youtube_objects = list(channel.video_urls)

    total_videos = len(youtube_objects)
    total_batches = (total_videos + batch_size - 1) // batch_size

    print(f"Channel: {channel.channel_name}")
    print(f"Total videos: {total_videos}")
    print(f"Total batches needed: {total_batches}")

    # Check for existing batches and find where to resume
    start_batch = find_last_completed_batch(output_dir, safe_name)

    if start_batch > 1:
        print(f"🔄 RESUMING from batch {start_batch}")

        # Validate existing files
        print("Validating existing batch files...")
        for batch_num in range(1, start_batch):
            batch_filename = f"{safe_name}_{batch_num:03d}.json"
            batch_filepath = os.path.join(output_dir, batch_filename)

            if os.path.exists(batch_filepath):
                if validate_batch_file(batch_filepath):
                    file_size = os.path.getsize(batch_filepath)
                    print(
                        f"  ✅ Batch {batch_num:2d}: {batch_filename} ({file_size // 1024:.0f} KB)"
                    )
                else:
                    print(
                        f"  ❌ Batch {batch_num:2d}: {batch_filename} (INVALID - will be recreated)"
                    )
                    start_batch = batch_num  # Start from the first invalid batch
                    break
            else:
                print(f"  ❌ Batch {batch_num:2d}: Missing - will be recreated")
                start_batch = batch_num
                break
    else:
        print("🚀 STARTING fresh export")

    print(f"📁 Output: {output_dir}/")
    print("=" * 70)

    # Process batches starting from the resume point
    for batch_num in range(start_batch, total_batches + 1):
        start_idx = (batch_num - 1) * batch_size
        end_idx = min(start_idx + batch_size, total_videos)
        current_batch = youtube_objects[start_idx:end_idx]

        batch_filename = f"{safe_name}_{batch_num:03d}.json"
        batch_filepath = os.path.join(output_dir, batch_filename)

        # Skip if file already exists and is valid
        if os.path.exists(batch_filepath) and validate_batch_file(batch_filepath):
            file_size = os.path.getsize(batch_filepath)
            print(
                f"Batch {batch_num:2d}/{total_batches} | SKIPPED (already exists, {file_size // 1024:.0f} KB)"
            )
            continue

        print(
            f"Batch {batch_num:2d}/{total_batches} | Videos {start_idx + 1:4d}-{end_idx:4d} | Processing..."
        )

        # Progress bar setup
        progress_interval = max(1, len(current_batch) // 50)
        print("Progress: [", end="", flush=True)

        batch_videos = []

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

            except Exception as e:
                print("!", end="", flush=True)  # Error indicator
                # Create a placeholder entry for failed videos
                batch_videos.append(
                    {
                        "title": f"ERROR: Failed to process video {start_idx + i + 1}",
                        "duration": 0,
                        "video_id": f"error_{start_idx + i}",
                        "url": f"ERROR: {str(e)[:100]}",
                        "publish_date": None,
                    }
                )
                continue

        print("] ✓")

        # Create batch data
        batch_data = {
            "batch_info": {
                "batch_number": batch_num,
                "total_batches": total_batches,
                "videos_in_batch": len(batch_videos),
                "video_range": f"{start_idx + 1}-{end_idx}",
                "export_date": datetime.now().isoformat(),
            },
            "channel_info": {
                "name": channel.channel_name,
                "url": channel_url,
                "total_videos_in_channel": total_videos,
            },
            "videos": batch_videos,
        }

        # Save batch file
        with open(batch_filepath, "w", encoding="utf-8") as f:
            json.dump(batch_data, f, indent=2, ensure_ascii=False)

        # Show file info
        file_size = os.path.getsize(batch_filepath)
        successful_videos = len(
            [v for v in batch_videos if not v["title"].startswith("ERROR:")]
        )
        print(
            f"   ✅ Saved: {batch_filename} ({successful_videos}/{len(batch_videos)} videos, {file_size // 1024:.0f} KB)"
        )

        # If we had errors, show them
        failed_videos = len(batch_videos) - successful_videos
        if failed_videos > 0:
            print(f"   ⚠️  {failed_videos} videos had errors in this batch")

    print("=" * 70)
    print("🎉 BATCH EXPORT COMPLETED!")

    # Show final summary
    completed_files = []
    total_videos_exported = 0

    for batch_num in range(1, total_batches + 1):
        batch_filename = f"{safe_name}_{batch_num:03d}.json"
        batch_filepath = os.path.join(output_dir, batch_filename)

        if os.path.exists(batch_filepath):
            try:
                with open(batch_filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    video_count = len(data.get("videos", []))
                    total_videos_exported += video_count
                    completed_files.append((batch_filename, video_count))
            except:
                pass

    print(f"✅ Created {len(completed_files)}/{total_batches} batch files")
    print(f"📊 Total videos exported: {total_videos_exported}/{total_videos}")
    print(f"📁 Location: {output_dir}/")

    # Show missing batches if any
    missing_batches = []
    for batch_num in range(1, total_batches + 1):
        batch_filename = f"{safe_name}_{batch_num:03d}.json"
        batch_filepath = os.path.join(output_dir, batch_filename)
        if not os.path.exists(batch_filepath):
            missing_batches.append(batch_num)

    if missing_batches:
        print(f"⚠️  Missing batches: {missing_batches}")
        print("   Run the script again to complete these batches")
    else:
        print("🏆 ALL BATCHES COMPLETED SUCCESSFULLY!")


def show_export_status():
    """
    Show current export status without running export
    """
    output_dir = "./video_batches"
    safe_name = "AZ_Alkmaar_videos"

    if not os.path.exists(output_dir):
        print("No export directory found. No batches have been created yet.")
        return

    print("Current Export Status:")
    print("=" * 50)

    # Estimate total batches (approximate)
    expected_batches = 55  # We know AZ Alkmaar has ~5494 videos

    completed_batches = []
    invalid_batches = []

    for batch_num in range(1, expected_batches + 1):
        batch_filename = f"{safe_name}_{batch_num:03d}.json"
        batch_filepath = os.path.join(output_dir, batch_filename)

        if os.path.exists(batch_filepath):
            if validate_batch_file(batch_filepath):
                completed_batches.append(batch_num)

                # Show file info
                try:
                    with open(batch_filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        video_count = len(data.get("videos", []))
                    file_size = os.path.getsize(batch_filepath)
                    print(
                        f"✅ Batch {batch_num:2d}: {video_count} videos ({file_size // 1024:.0f} KB)"
                    )
                except:
                    print(f"✅ Batch {batch_num:2d}: File exists")
            else:
                invalid_batches.append(batch_num)
                print(f"❌ Batch {batch_num:2d}: Invalid/corrupted file")
        else:
            # Stop at first missing batch for cleaner output
            if len(completed_batches) > 0:
                print(f"❓ Batch {batch_num:2d}: Missing (and all subsequent batches)")
                break

    print("=" * 50)
    print(f"Completed: {len(completed_batches)} batches")
    if invalid_batches:
        print(f"Invalid: {len(invalid_batches)} batches - {invalid_batches}")

    if len(completed_batches) < expected_batches:
        next_batch = max(completed_batches) + 1 if completed_batches else 1
        print(f"Next batch to process: {next_batch}")
    else:
        print("🏆 All batches appear to be completed!")


if __name__ == "__main__":
    print("🚀 AZTV Video Batch Export (with Resume)")
    print("=" * 50)
    print("1. Run/Resume export")
    print("2. Show current status")

    choice = input("\nChoice (1-2): ").strip()

    if choice == "2":
        show_export_status()
    else:
        print("\nThis script will:")
        print("✅ Skip existing valid batch files")
        print("✅ Resume from the last incomplete batch")
        print("✅ Continue until all batches are complete")
        print("✅ Show progress for each video")
        print()
        run_batch_export_with_resume()
