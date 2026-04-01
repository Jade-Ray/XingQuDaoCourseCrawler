from __future__ import annotations

import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import re
import socket
import subprocess
import time
import unicodedata
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import aiohttp
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from .config import XingQuDaoCourseCrawlerConfig
from .exceptions import (
    DownloadIncompleteError,
    InitDataParseError,
    LoginStateError,
    M3U8ParseError,
    MergeVideoError,
)


class XingQuDaoCourseCrawler:
    _MAX_NAME_LEN = 80

    def __init__(self, config: XingQuDaoCourseCrawlerConfig, vod_page_url: str):
        self.config = config
        self.vod_page_url = vod_page_url
        self.topic_id = "unknown"
        self.topic_name = "course"
        self.selected_quality = self.config.default_quality
        self.course_dir = self.config.save_dir
        self.ts_dir = self.course_dir / "ts"
        self.manifest_path = self.course_dir / "manifest.json"
        self._manifest: dict[str, Any] = {}
        self._segment_errors: dict[int, str] = {}
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/146.0.0.0 Safari/537.36"
                ),
                "Cookie": self.config.cookie,
            }
        )
        self.ts_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def _sanitize_name(cls, value: str) -> str:
        normalized = unicodedata.normalize("NFKC", value).strip()
        cleaned = re.sub(r"[\\/:*?\"<>|]", "_", normalized)
        cleaned = re.sub(r"\s+", "_", cleaned)
        cleaned = re.sub(r"_+", "_", cleaned).strip("._")
        if not cleaned:
            return "course"
        return cleaned[: cls._MAX_NAME_LEN]

    def _apply_course_paths(self, init_data: dict[str, Any]) -> None:
        raw_topic_id = str(init_data.get("topicId") or "unknown")
        raw_topic_name = str(init_data.get("topicName") or "course")

        self.topic_id = self._sanitize_name(raw_topic_id)
        self.topic_name = self._sanitize_name(raw_topic_name)

        folder_name = f"{self.topic_id}_{self.topic_name}"
        self.course_dir = self.config.save_dir / folder_name
        self.ts_dir = self.course_dir / "ts"
        self.manifest_path = self.course_dir / "manifest.json"
        self.ts_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _write_manifest(self, updates: dict[str, Any]) -> None:
        if not self._manifest:
            self._manifest = {
                "status": "running",
                "createdAt": self._utc_now(),
                "updatedAt": self._utc_now(),
                "vodPageUrl": self.vod_page_url,
                "topicId": self.topic_id,
                "topicName": self.topic_name,
                "quality": self.selected_quality,
                "transcodeMode": self.config.transcode_mode,
                "paths": {
                    "courseDir": str(self.course_dir),
                    "tsDir": str(self.ts_dir),
                },
                "m3u8": {
                    "replayUrl": "",
                    "mediaM3u8Url": "",
                },
                "download": {
                    "totalSegments": 0,
                    "downloadedSegments": 0,
                    "missingSegments": [],
                    "failedSegments": {},
                    "retryRounds": self.config.retry_rounds,
                },
                "output": {
                    "videoPath": "",
                },
                "error": "",
            }

        for key, value in updates.items():
            if isinstance(value, dict) and isinstance(self._manifest.get(key), dict):
                self._manifest[key].update(value)
            else:
                self._manifest[key] = value

        self._manifest["updatedAt"] = self._utc_now()
        self.course_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path.write_text(
            json.dumps(self._manifest, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def check_login_status(self) -> dict[str, Any]:
        headers = {
            "Content-Type": "application/json",
            "Origin": self.config.base_host,
            "Referer": self.vod_page_url,
        }
        params = {"url": "/gate/user/getUserInfoById"}
        body = {"transferUrl": "/gate/user/getUserInfoById", "transferData": {}}

        try:
            response = self.session.post(
                self.config.transfer_api,
                params=params,
                json=body,
                headers=headers,
                timeout=self.config.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise LoginStateError(f"Failed to validate login status: {exc}") from exc
        except ValueError as exc:
            raise LoginStateError("Login status response is not valid JSON") from exc

        state = payload.get("state", {})
        if state.get("code") != 0:
            raise LoginStateError(
                f"Cookie is invalid or expired: {state.get('msg', 'unknown error')}"
            )
        return payload.get("data", {})

    def fetch_page_html(self, page_url: str | None = None) -> str:
        target_url = page_url or self.vod_page_url
        headers = {
            "Referer": self.config.base_host,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
                "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
            ),
        }
        try:
            response = self.session.get(
                target_url,
                headers=headers,
                timeout=self.config.timeout,
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to fetch page HTML: {exc}") from exc

    def extract_init_data(self, html: str) -> dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        for script in soup.find_all("script"):
            if not script.string or "INIT_DATA" not in script.string:
                continue

            match = re.search(
                r"window\.INIT_DATA\s*=\s*(\{.+?\})",
                script.string,
                re.DOTALL,
            )
            if not match:
                continue

            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError as exc:
                raise InitDataParseError("INIT_DATA JSON decode failed") from exc

        raise InitDataParseError("INIT_DATA not found in page")

    def _extract_sub_m3u8_dict(self, m3u8_content: str) -> dict[str, dict[str, str]]:
        sub_m3u8_dict: dict[str, dict[str, str]] = {}
        lines = m3u8_content.splitlines()
        for idx, line in enumerate(lines):
            current = line.strip()
            if not current.startswith("#EXT-X-STREAM-INF:"):
                continue
            if idx + 1 >= len(lines):
                continue

            sub_url = lines[idx + 1].strip()
            res_match = re.search(r"RESOLUTION=(\d+)x(\d+)", current)
            if not res_match:
                continue

            width, height = res_match.groups()
            quality = f"{height}p"
            sub_m3u8_dict[quality] = {
                "width": width,
                "height": height,
                "sub_url": sub_url,
            }

        if not sub_m3u8_dict:
            raise M3U8ParseError("No sub m3u8 stream found in master m3u8")
        return sub_m3u8_dict

    def parse_m3u8_url(self, m3u8_url: str, quality: str | None = None) -> str:
        selected_quality = quality or self.config.default_quality
        try:
            master_m3u8 = self._fetch_text_with_retry(m3u8_url)
        except requests.RequestException as exc:
            raise M3U8ParseError(f"Failed to fetch master m3u8: {exc}") from exc

        master_base_url = m3u8_url.rsplit("/", 1)[0] + "/"
        try:
            sub_m3u8_dict = self._extract_sub_m3u8_dict(master_m3u8)
        except M3U8ParseError:
            # Some replayUrl values already point to media playlists instead of master playlists.
            if "#EXTINF" in master_m3u8 or ".ts" in master_m3u8:
                self.selected_quality = selected_quality
                return m3u8_url
            raise

        if selected_quality in sub_m3u8_dict:
            self.selected_quality = selected_quality
            return urljoin(master_base_url, sub_m3u8_dict[selected_quality]["sub_url"])

        # fallback to highest quality
        max_quality = max(sub_m3u8_dict.keys(), key=lambda value: int(value.rstrip("p")))
        self.selected_quality = max_quality
        return urljoin(master_base_url, sub_m3u8_dict[max_quality]["sub_url"])

    def parse_media_m3u8(self, m3u8_url: str) -> list[str]:
        try:
            media_m3u8 = self._fetch_text_with_retry(m3u8_url)
        except requests.RequestException as exc:
            raise M3U8ParseError(f"Failed to fetch media m3u8: {exc}") from exc

        media_base_url = m3u8_url.rsplit("/", 1)[0] + "/"
        ts_urls = [
            urljoin(media_base_url, line.strip())
            for line in media_m3u8.splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        if not ts_urls:
            raise M3U8ParseError("No ts segment URL found in media m3u8")
        return ts_urls

    def _fetch_text_with_retry(self, url: str) -> str:
        last_error: requests.RequestException | None = None
        for attempt in range(3):
            try:
                response = self.session.get(url, timeout=self.config.timeout)
                response.raise_for_status()
                return response.text
            except requests.exceptions.SSLError as exc:
                last_error = exc
                # Some CDN nodes occasionally break TLS handshake; fallback retry.
                try:
                    response = self.session.get(
                        url,
                        timeout=self.config.timeout,
                        verify=False,
                    )
                    response.raise_for_status()
                    return response.text
                except requests.RequestException as insecure_exc:
                    last_error = insecure_exc
            except requests.RequestException as exc:
                last_error = exc

            time.sleep(0.6 * (attempt + 1))

        assert last_error is not None
        raise last_error

    def _download_one_with_requests(self, ts_url: str, index: int) -> bool:
        ts_path = self.ts_dir / f"{index:05d}.ts"
        temp_path = self.ts_dir / f"{index:05d}.part"

        response = self.session.get(ts_url, timeout=self.config.timeout)
        response.raise_for_status()
        data = response.content
        if not data:
            return False

        temp_path.write_bytes(data)
        temp_path.replace(ts_path)
        return True

    async def _download_one(
        self,
        client: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        ts_url: str,
        index: int,
    ) -> tuple[bool, str | None]:
        ts_path = self.ts_dir / f"{index:05d}.ts"
        temp_path = self.ts_dir / f"{index:05d}.part"
        request_timeout = aiohttp.ClientTimeout(
            total=float(self.config.timeout),
            connect=10,
            sock_connect=10,
            sock_read=float(self.config.timeout),
        )
        last_error: str | None = None
        async with semaphore:
            for attempt in range(3):
                try:
                    async with asyncio.timeout(self.config.timeout + 15):
                        async with client.get(ts_url, timeout=request_timeout) as resp:
                            if resp.status != 200:
                                last_error = f"http_{resp.status}"
                                continue
                            data = await resp.read()
                            if not data:
                                last_error = "empty_body"
                                continue
                            temp_path.write_bytes(data)
                            temp_path.replace(ts_path)
                            return True, None
                except aiohttp.ClientConnectorDNSError:
                    last_error = "ClientConnectorDNSError"
                    try:
                        ok = await asyncio.to_thread(
                            self._download_one_with_requests,
                            ts_url,
                            index,
                        )
                        if ok:
                            return True, None
                        last_error = "requests_empty_body"
                    except requests.RequestException as exc:
                        last_error = f"requests_{exc.__class__.__name__}"
                    except OSError as exc:
                        last_error = f"requests_{exc.__class__.__name__}"
                except asyncio.TimeoutError:
                    last_error = "timeout"
                except aiohttp.ClientError as exc:
                    last_error = exc.__class__.__name__
                except OSError as exc:
                    last_error = exc.__class__.__name__
                await asyncio.sleep(0.5 * (attempt + 1))

            temp_path.unlink(missing_ok=True)
            return False, last_error

    def _find_missing_indexes(self, total_count: int) -> list[int]:
        missing: list[int] = []
        for idx in range(total_count):
            ts_path = self.ts_dir / f"{idx:05d}.ts"
            if not ts_path.exists() or ts_path.stat().st_size == 0:
                missing.append(idx)
        return missing

    async def download_all_ts(self, ts_urls: list[str]) -> None:
        headers = {
            "User-Agent": str(self.session.headers.get("User-Agent", "Mozilla/5.0")),
            "Cookie": str(self.session.headers.get("Cookie", "")),
        }
        connector = aiohttp.TCPConnector(
            ssl=False,
            limit=self.config.max_concurrency,
            limit_per_host=self.config.max_concurrency,
            ttl_dns_cache=300,
            family=socket.AF_INET,
            enable_cleanup_closed=True,
        )
        semaphore = asyncio.Semaphore(self.config.max_concurrency)

        total_count = len(ts_urls)
        if total_count == 0:
            raise DownloadIncompleteError("No ts URL found for download")

        self._write_manifest({"download": {"totalSegments": total_count}})

        missing_indexes = self._find_missing_indexes(total_count)
        completed_indexes = {idx for idx in range(total_count) if idx not in missing_indexes}
        self._segment_errors = {}
        with tqdm(
            total=total_count,
            initial=len(completed_indexes),
            desc="Downloading TS",
            unit="seg",
        ) as progress:
            async with aiohttp.ClientSession(connector=connector, headers=headers) as client:
                for round_idx in range(self.config.retry_rounds):
                    if not missing_indexes:
                        break

                    if round_idx != 0:
                        print(
                            f"Retry round {round_idx + 1}/{self.config.retry_rounds}, "
                            f"remaining: {len(missing_indexes)}"
                        )

                    round_errors: Counter[str] = Counter()

                    async def _download_with_index(idx: int) -> tuple[int, bool, str | None]:
                        ok, error = await self._download_one(client, semaphore, ts_urls[idx], idx)
                        return idx, ok, error

                    tasks = [_download_with_index(idx) for idx in missing_indexes]
                    for task in asyncio.as_completed(tasks):
                        idx, ok, error = await task
                        ts_path = self.ts_dir / f"{idx:05d}.ts"
                        if ok and ts_path.exists() and ts_path.stat().st_size > 0:
                            if idx not in completed_indexes:
                                completed_indexes.add(idx)
                                progress.update(1)
                            self._segment_errors.pop(idx, None)
                        elif error:
                            self._segment_errors[idx] = error
                            round_errors[error] += 1

                    missing_indexes = self._find_missing_indexes(total_count)
                    if round_errors:
                        summary = ", ".join(
                            f"{name}={count}" for name, count in round_errors.most_common()
                        )
                        print(f"Round {round_idx + 1} failures: {summary}")

                    self._write_manifest(
                        {
                            "download": {
                                "downloadedSegments": len(completed_indexes),
                                "missingSegments": missing_indexes,
                                "failedSegments": {
                                    str(idx): self._segment_errors[idx]
                                    for idx in sorted(self._segment_errors)
                                },
                            }
                        }
                    )
                    if not missing_indexes:
                        break

        self._write_manifest(
            {
                "download": {
                    "downloadedSegments": len(completed_indexes),
                    "missingSegments": missing_indexes,
                    "failedSegments": {
                        str(idx): self._segment_errors[idx]
                        for idx in sorted(self._segment_errors)
                    },
                }
            }
        )

        if missing_indexes:
            raise DownloadIncompleteError(
                f"Still missing {len(missing_indexes)} ts files after retries: "
                f"{missing_indexes[:20]}"
            )

    @staticmethod
    def _detect_nvenc() -> bool:
        """检测当前系统 ffmpeg 是否支持 NVENC 硬件编码。"""
        try:
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-hide_banner",
                    "-f", "lavfi",
                    "-i", "nullsrc=s=128x128:d=0.1",
                    "-c:v", "h264_nvenc",
                    "-f", "null",
                    "-",
                ],
                capture_output=True,
                timeout=10,
            )
            return result.returncode == 0
        except Exception:
            return False

    def merge_ts_to_mp4(self, ts_urls: list[str], output_name: str | None = None) -> Path:
        file_list = (self.course_dir / "file_list.txt").resolve()
        with file_list.open("w", encoding="utf-8", newline="\n") as handle:
            for idx in range(len(ts_urls)):
                ts_file = (self.ts_dir / f"{idx:05d}.ts").resolve().as_posix()
                handle.write(f"file '{ts_file}'\n")

        mp4_name = output_name or f"{self.topic_name}_{self.selected_quality}.mp4"
        output_path = (self.course_dir / mp4_name).resolve()

        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(file_list),
        ]

        if self.config.transcode_mode == "quality":
            cmd.extend(["-c", "copy"])
        elif self.config.transcode_mode in ("balanced", "size"):
            use_gpu = self._detect_nvenc()
            if use_gpu:
                print(f"[merge] NVENC GPU detected, using hardware encoding.")
            else:
                print(f"[merge] NVENC not available, falling back to CPU (libx264).")

            if self.config.transcode_mode == "balanced":
                if use_gpu:
                    cmd.extend(["-c:v", "h264_nvenc", "-preset", "p3", "-rc", "vbr", "-cq", "25", "-b:v", "0"])
                else:
                    cmd.extend(["-c:v", "libx264", "-preset", "fast", "-crf", "25"])
                cmd.extend(["-c:a", "aac", "-b:a", "96k"])
            else:  # size
                if use_gpu:
                    cmd.extend(["-c:v", "h264_nvenc", "-preset", "p4", "-rc", "vbr", "-cq", "30", "-b:v", "0"])
                else:
                    cmd.extend(["-c:v", "libx264", "-preset", "veryfast", "-crf", "30"])
                cmd.extend(["-c:a", "aac", "-b:a", "64k"])
        else:
            raise MergeVideoError(
                f"Unsupported transcode mode: {self.config.transcode_mode}"
            )

        cmd.extend([str(output_path), "-stats", "-loglevel", "info"])

        try:
            subprocess.run(cmd, check=True)
            return output_path
        except (subprocess.CalledProcessError, FileNotFoundError) as exc:
            raise MergeVideoError(
                "Failed to merge ts files. Ensure ffmpeg is installed and callable."
            ) from exc
        finally:
            if file_list.exists():
                file_list.unlink()

    def clean_temp_files(self) -> None:
        for ts_file in self.ts_dir.glob("*.ts"):
            ts_file.unlink(missing_ok=True)
        for part_file in self.ts_dir.glob("*.part"):
            part_file.unlink(missing_ok=True)

    async def run(self) -> Path:
        try:
            print("[1/5] Checking login state...")
            self.check_login_status()
            print("[2/5] Fetching page HTML...")
            page_html = self.fetch_page_html()
            print("[3/5] Parsing INIT_DATA...")
            init_data = self.extract_init_data(page_html)
            self._apply_course_paths(init_data)
            replay_url = str(init_data.get("replayUrl", "")).strip()
            if not replay_url:
                raise InitDataParseError("replayUrl is missing in INIT_DATA")

            self._write_manifest(
                {
                    "status": "running",
                    "topicId": self.topic_id,
                    "topicName": self.topic_name,
                    "quality": self.selected_quality,
                    "transcodeMode": self.config.transcode_mode,
                    "m3u8": {"replayUrl": replay_url},
                    "error": "",
                }
            )

            print("[4/5] Resolving m3u8 and ts list...")
            media_m3u8_url = self.parse_m3u8_url(replay_url)
            ts_urls = self.parse_media_m3u8(media_m3u8_url)
            print(f"Found {len(ts_urls)} ts segments")
            self._write_manifest(
                {
                    "quality": self.selected_quality,
                    "m3u8": {"mediaM3u8Url": media_m3u8_url},
                    "download": {"totalSegments": len(ts_urls)},
                }
            )

            print("[5/5] Downloading and merging...")
            await self.download_all_ts(ts_urls)
            output = self.merge_ts_to_mp4(ts_urls)

            self._write_manifest(
                {
                    "status": "completed",
                    "output": {"videoPath": str(output)},
                }
            )
            return output
        except Exception as exc:
            self._write_manifest(
                {
                    "status": "failed",
                    "error": str(exc),
                    "quality": self.selected_quality,
                }
            )
            raise
