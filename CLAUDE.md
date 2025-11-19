# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based YouTube video downloader and metadata exporter using the `pytubefix` library. The codebase provides tools for downloading individual videos and batch exporting metadata from entire YouTube channels.

## Dependencies

The project depends on the `pytubefix` library. Install it with:
```bash
pip install pytubefix
```

## Architecture

### Core Components

1. **YoutubeVideoDownloader** (`youtube_video_downloader.py`)
   - Single video downloader class
   - Downloads highest resolution progressive streams (video + audio)
   - Handles filename sanitization and overwrite protection
   - Main method: `download(url, filename=None)` returns local file path

2. **YouTubeBatchExporter** (`youtube_channel_explorer.py`)
   - Object-oriented batch exporter for channel metadata
   - Exports video metadata to JSON files in configurable batch sizes
   - Features:
     - Resume support: detects existing batches and continues from last completed
     - Error recovery: retries failed batches with progressive delays
     - Batch validation: validates success rate before marking batch complete
     - Channel reloading on connection issues
   - Key configuration: `batch_size`, `max_batch_retries`, `min_success_rate`

3. **Functional Batch Exporters**
   - `batch_export_with_recovery.py`: Standalone script with comprehensive error recovery
   - `channel_content.py`: Earlier version with basic resume functionality
   - Both export JSON files with metadata: title, duration, video_id, url, publish_date

### Data Flow

1. **Single video download**: User provides URL → `YoutubeVideoDownloader` → Downloads to `output_dir`
2. **Batch metadata export**: Channel URL → Load all video URLs → Process in batches → Export JSON files with structure:
   ```
   {
     "batch_info": { batch_number, videos_in_batch, video_range, export_date, error_count, success_rate },
     "channel_info": { name, url, total_videos_in_channel },
     "videos": [ { title, duration, video_id, url, publish_date }, ... ]
   }
   ```

### Naming Conventions

- Batch files: `{safe_channel_name}_{batch_num:03d}.json` (e.g., `AZ_Alkmaar_videos_001.json`)
- Safe channel names: spaces replaced with underscores, special chars removed
- Video filenames: sanitized titles with illegal characters removed

## Running the Code

### Download a single video
```bash
python main.py
```
Edit `main.py` to change the target URL and output directory.

### Export channel metadata (OOP version)
```bash
python youtube_channel_explorer.py
```
Edit the `CHANNEL_URL` at the bottom of the file to target different channels.

### Export channel metadata (functional version with recovery)
```bash
python batch_export_with_recovery.py
```

### Export channel metadata (basic resume support)
```bash
python channel_content.py
```
This script provides an interactive menu for running export or checking status.

## Key Implementation Details

### Error Handling Strategy
- **Consecutive errors**: If 10+ consecutive video errors occur, batch is retried
- **Error rate threshold**: Batches with <70% success rate trigger retry
- **Max retries**: Configurable per batch (default: 3)
- **Channel reloading**: On batch failure, channel data is reloaded to recover from connection issues
- **Progressive delays**: Retry delays increase with attempt number (5s, 10s, 15s)

### Resume Functionality
- Scans output directory for existing batch files matching prefix pattern
- Validates batch files for completeness using success rate threshold
- Continues from `max(existing_batch_numbers) + 1`
- Skips existing valid batches to avoid duplicate work

### Filename Sanitization
- Path separators (`/`, `\`) replaced with underscores
- Illegal characters (`:*?"<>|`) removed entirely
- Leading/trailing whitespace stripped
- Falls back to default names if empty

### Video ID Extraction
Uses regex pattern `videoId=([a-zA-Z0-9_-]{11})` to extract from `repr()` of YouTube objects, as direct access may not be reliable.

## Common Patterns

- Always use `Path` objects for file operations (`youtube_video_downloader.py`)
- Use `os.makedirs(..., exist_ok=True)` to ensure output directories exist
- Progress indicators: `█` for successful video processing, `!` for errors
- Error entries in JSON: videos that fail to process get `"ERROR:"` prefix in title field
