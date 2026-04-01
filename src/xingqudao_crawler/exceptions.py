class CrawlerError(Exception):
    """Base exception for crawler failures."""


class LoginStateError(CrawlerError):
    """Raised when cookie is invalid or login state is expired."""


class InitDataParseError(CrawlerError):
    """Raised when INIT_DATA cannot be extracted from page HTML."""


class M3U8ParseError(CrawlerError):
    """Raised when m3u8 parsing fails."""


class DownloadIncompleteError(CrawlerError):
    """Raised when ts files are still missing after retries."""


class MergeVideoError(CrawlerError):
    """Raised when ffmpeg merge step fails."""
