import json
from pathlib import Path

import pytest

from xingqudao_crawler.config import XingQuDaoCourseCrawlerConfig
from xingqudao_crawler.crawler import XingQuDaoCourseCrawler
from xingqudao_crawler.exceptions import DownloadIncompleteError, InitDataParseError


@pytest.fixture
def crawler(tmp_path: Path) -> XingQuDaoCourseCrawler:
    config = XingQuDaoCourseCrawlerConfig(
        cookie="foo=bar",
        save_dir=tmp_path,
        retry_rounds=2,
        max_concurrency=3,
    )
    return XingQuDaoCourseCrawler(config, "https://m1.nicegoods.cn/financial/open-live?foo=1")


def test_extract_init_data_success(crawler: XingQuDaoCourseCrawler) -> None:
    html = """
    <html><body>
      <script>window.INIT_DATA = {"replayUrl":"https://example.com/master.m3u8"}</script>
    </body></html>
    """
    data = crawler.extract_init_data(html)
    assert data["replayUrl"] == "https://example.com/master.m3u8"


def test_extract_init_data_raises_when_missing(crawler: XingQuDaoCourseCrawler) -> None:
    with pytest.raises(InitDataParseError):
        crawler.extract_init_data("<html></html>")


def test_parse_m3u8_url_accepts_direct_media_playlist(
    crawler: XingQuDaoCourseCrawler, monkeypatch: pytest.MonkeyPatch
) -> None:
    class DummyResponse:
        text = "#EXTM3U\n#EXTINF:6.0,\n00001.ts\n#EXTINF:6.0,\n00002.ts\n"

        @staticmethod
        def raise_for_status() -> None:
            return None

    def fake_get(url: str, timeout: int):
        return DummyResponse()

    monkeypatch.setattr(crawler.session, "get", fake_get)

    url = "https://cdn.example.com/path/media.m3u8"
    assert crawler.parse_m3u8_url(url) == url


@pytest.mark.asyncio
async def test_download_all_ts_retries_and_succeeds(crawler: XingQuDaoCourseCrawler, monkeypatch: pytest.MonkeyPatch) -> None:
    ts_urls = ["u1", "u2", "u3"]
    calls: dict[int, int] = {}

    async def fake_download_one(client, semaphore, ts_url, index):
        calls[index] = calls.get(index, 0) + 1
        file_path = crawler.ts_dir / f"{index:05d}.ts"
        # index=1 fails in first round and succeeds in second round
        if index == 1 and calls[index] == 1:
            return False, "timeout"
        file_path.write_bytes(b"ok")
        return True, None

    monkeypatch.setattr(crawler, "_download_one", fake_download_one)

    await crawler.download_all_ts(ts_urls)

    for i in range(3):
        assert (crawler.ts_dir / f"{i:05d}.ts").exists()


@pytest.mark.asyncio
async def test_download_all_ts_raises_when_missing(crawler: XingQuDaoCourseCrawler, monkeypatch: pytest.MonkeyPatch) -> None:
    ts_urls = ["u1", "u2"]

    async def always_fail(client, semaphore, ts_url, index):
        return False, "timeout"

    monkeypatch.setattr(crawler, "_download_one", always_fail)

    with pytest.raises(DownloadIncompleteError):
        await crawler.download_all_ts(ts_urls)

    payload = json.loads(crawler.manifest_path.read_text(encoding="utf-8"))
    assert payload["download"]["failedSegments"] == {"0": "timeout", "1": "timeout"}


def test_apply_course_paths_builds_topic_subdirs(crawler: XingQuDaoCourseCrawler) -> None:
    crawler._apply_course_paths({"topicId": 12345, "topicName": "Python 直播课 / 第1讲"})

    assert crawler.course_dir.name.startswith("12345_")
    assert crawler.ts_dir == crawler.course_dir / "ts"
    assert crawler.ts_dir.exists()


def test_merge_output_name_uses_topic_and_quality(
    crawler: XingQuDaoCourseCrawler, monkeypatch: pytest.MonkeyPatch
) -> None:
    crawler._apply_course_paths({"topicId": 1, "topicName": "测试课程"})
    crawler.selected_quality = "720p"
    monkeypatch.setattr(
        "xingqudao_crawler.crawler.XingQuDaoCourseCrawler._detect_nvenc",
        staticmethod(lambda: False),
    )

    def fake_run(cmd, **kwargs):
        file_list_path = Path(cmd[7])
        content = file_list_path.read_text(encoding="utf-8")
        assert "\nfile " in content
        assert "tsnfile" not in content
        return None

    monkeypatch.setattr("xingqudao_crawler.crawler.subprocess.run", fake_run)

    out = crawler.merge_ts_to_mp4(["u1", "u2"], output_name=None)
    assert out.name == "测试课程_720p.mp4"


@pytest.mark.parametrize(
    ("mode", "use_gpu", "expected_args"),
    [
        ("quality", False, ["-c", "copy"]),
        ("balanced", False, ["-c:v", "libx264", "-crf", "25", "-c:a", "aac", "-b:a", "96k"]),
        ("size", False, ["-c:v", "libx264", "-crf", "30", "-c:a", "aac", "-b:a", "64k"]),
        ("balanced", True, ["-c:v", "h264_nvenc", "-cq", "25", "-c:a", "aac", "-b:a", "96k"]),
        ("size", True, ["-c:v", "h264_nvenc", "-cq", "30", "-c:a", "aac", "-b:a", "64k"]),
    ],
)
def test_merge_transcode_mode_builds_ffmpeg_args(
    crawler: XingQuDaoCourseCrawler,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    use_gpu: bool,
    expected_args: list[str],
) -> None:
    crawler._apply_course_paths({"topicId": 1, "topicName": "课程"})
    crawler.config.transcode_mode = mode
    monkeypatch.setattr(
        "xingqudao_crawler.crawler.XingQuDaoCourseCrawler._detect_nvenc",
        staticmethod(lambda: use_gpu),
    )

    captured: dict[str, list[str]] = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return None

    monkeypatch.setattr("xingqudao_crawler.crawler.subprocess.run", fake_run)
    crawler.merge_ts_to_mp4(["u1"], output_name=None)

    cmd = captured["cmd"]
    for arg in expected_args:
        assert arg in cmd


def test_write_manifest_creates_manifest_file(crawler: XingQuDaoCourseCrawler) -> None:
    crawler._apply_course_paths({"topicId": 99, "topicName": "Manifest课"})
    crawler._write_manifest({"status": "running", "quality": "1080p"})

    assert crawler.manifest_path.exists()
    payload = json.loads(crawler.manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "running"
    assert payload["topicId"] == "99"
    assert payload["topicName"] == "Manifest课"


def test_download_one_with_requests_writes_ts_file(
    crawler: XingQuDaoCourseCrawler, monkeypatch: pytest.MonkeyPatch
) -> None:
    class DummyResponse:
        content = b"segment-bytes"

        @staticmethod
        def raise_for_status() -> None:
            return None

    def fake_get(url: str, timeout: int):
        return DummyResponse()

    monkeypatch.setattr(crawler.session, "get", fake_get)

    ok = crawler._download_one_with_requests("https://cdn.example.com/00000.ts", 0)

    assert ok is True
    assert (crawler.ts_dir / "00000.ts").read_bytes() == b"segment-bytes"
