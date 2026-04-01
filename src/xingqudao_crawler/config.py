from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class XingQuDaoCourseCrawlerConfig:
    base_host: str = "https://m1.nicegoods.cn"
    cookie: str = ""
    timeout: int = 30
    default_quality: str = "1080p"
    save_dir: Path = Path("download")
    max_concurrency: int = 20
    retry_rounds: int = 3
    transcode_mode: str = "quality"

    @property
    def transfer_api(self) -> str:
        return f"{self.base_host}/financial/api/transfer"

    @classmethod
    def from_cookie_file(
        cls,
        cookie_file: str | Path,
        *,
        save_dir: str | Path = "download",
        default_quality: str = "1080p",
        timeout: int = 30,
        max_concurrency: int = 20,
        retry_rounds: int = 3,
        transcode_mode: str = "quality",
    ) -> "XingQuDaoCourseCrawlerConfig":
        cookie_path = Path(cookie_file)
        if not cookie_path.exists():
            raise FileNotFoundError(
                f"Cookie file not found: {cookie_path}. Please create it first."
            )

        cookie = cookie_path.read_text(encoding="utf-8").strip()
        if not cookie:
            raise ValueError(f"Cookie file is empty: {cookie_path}")

        save_path = Path(save_dir)
        save_path.mkdir(parents=True, exist_ok=True)

        return cls(
            cookie=cookie,
            timeout=timeout,
            default_quality=default_quality,
            save_dir=save_path,
            max_concurrency=max_concurrency,
            retry_rounds=retry_rounds,
            transcode_mode=transcode_mode,
        )
