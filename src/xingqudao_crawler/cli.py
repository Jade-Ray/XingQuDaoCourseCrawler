from __future__ import annotations

import argparse
import asyncio
import sys

from .config import XingQuDaoCourseCrawlerConfig
from .crawler import XingQuDaoCourseCrawler
from .exceptions import CrawlerError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download and merge VOD ts streams from XingQuDao page URL"
    )
    parser.add_argument("vod_url", help="XingQuDao VOD page URL")
    parser.add_argument(
        "--cookie-file",
        default="cookies.txt",
        help="Path to cookie file. The file content should be one full Cookie header line.",
    )
    parser.add_argument("--save-dir", default="download", help="Directory to save ts files and mp4")
    parser.add_argument("--quality", default="1080p", help="Preferred video quality, e.g. 720p/1080p")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    parser.add_argument("--max-concurrency", type=int, default=20, help="Max concurrent ts downloads")
    parser.add_argument("--retry-rounds", type=int, default=3, help="Redownload rounds for missing ts files")
    parser.add_argument(
        "--transcode-mode",
        choices=["quality", "balanced", "size"],
        default="quality",
        help="Video merge preset: quality(no re-encode), balanced, size",
    )
    parser.add_argument(
        "--keep-ts",
        action="store_true",
        help="Keep ts files after mp4 merge (default: clean up)",
    )
    return parser


async def run_from_args(args: argparse.Namespace) -> int:
    try:
        config = XingQuDaoCourseCrawlerConfig.from_cookie_file(
            args.cookie_file,
            save_dir=args.save_dir,
            default_quality=args.quality,
            timeout=args.timeout,
            max_concurrency=args.max_concurrency,
            retry_rounds=args.retry_rounds,
            transcode_mode=args.transcode_mode,
        )
        crawler = XingQuDaoCourseCrawler(config, args.vod_url)
        output = await crawler.run()
        print(f"Video saved to: {output}")
        if not args.keep_ts:
            crawler.clean_temp_files()
            print("Temporary ts files removed")
        return 0
    except (CrawlerError, OSError, ValueError) as exc:
        print(f"Crawler failed: {exc}")
        return 1


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return asyncio.run(run_from_args(args))


if __name__ == "__main__":
    sys.exit(main())
