from youtube_video_downloader import YoutubeVideoDownloader

if __name__ == "__main__":
    downloader = YoutubeVideoDownloader(
        output_dir="./downloads",
        overwrite=False,  # change to True if you want to overwrite existing files
    )

    url = "https://www.youtube.com/watch?v=8pG883qt_xE"
    print(f"Start downloading from youtube: {url}")
    local_path = downloader.download(url)
    print(f"Video saved to: {local_path}")
