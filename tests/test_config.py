from pathlib import Path

import pytest

from xingqudao_crawler.config import XingQuDaoCourseCrawlerConfig


def test_config_from_cookie_file_reads_cookie_and_creates_save_dir(tmp_path: Path) -> None:
    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text("foo=bar; hello=world", encoding="utf-8")

    save_dir = tmp_path / "video"
    config = XingQuDaoCourseCrawlerConfig.from_cookie_file(
        cookie_file,
        save_dir=save_dir,
    )

    assert config.cookie == "foo=bar; hello=world"
    assert config.save_dir == save_dir
    assert save_dir.exists()


def test_config_from_cookie_file_accepts_transcode_mode(tmp_path: Path) -> None:
    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text("foo=bar", encoding="utf-8")

    config = XingQuDaoCourseCrawlerConfig.from_cookie_file(
        cookie_file,
        transcode_mode="size",
    )

    assert config.transcode_mode == "size"


def test_config_from_cookie_file_raises_for_empty_file(tmp_path: Path) -> None:
    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text("   ", encoding="utf-8")

    with pytest.raises(ValueError):
        XingQuDaoCourseCrawlerConfig.from_cookie_file(cookie_file)
