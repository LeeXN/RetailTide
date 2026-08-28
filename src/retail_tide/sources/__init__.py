from .base import (
    ObservationSource,
    ProbeResult,
    SourceError,
    parse_paged_response,
    raw_from_mapping,
)
from .commoncrawl import CommonCrawlSource
from .guba import GubaSource
from .taoguba import TaogubaSource
from .wikimedia import WikimediaPageviewsSource
from .xiaohongshu import XiaohongshuSource
from .zhihu import ZhihuSource


def source_for_name(name: str, **kwargs):
    normalized = name.lower().replace("_", "-")
    sources = {
        "guba": GubaSource,
        "taoguba": TaogubaSource,
        "xiaohongshu": XiaohongshuSource,
        "zhihu": ZhihuSource,
        "common-crawl": CommonCrawlSource,
        "wikimedia-pageviews": WikimediaPageviewsSource,
    }
    try:
        return sources[normalized](**kwargs)
    except KeyError as exc:
        raise ValueError(f"unknown source: {name}") from exc


__all__ = [
    "CommonCrawlSource",
    "GubaSource",
    "ObservationSource",
    "ProbeResult",
    "SourceError",
    "TaogubaSource",
    "WikimediaPageviewsSource",
    "XiaohongshuSource",
    "ZhihuSource",
    "parse_paged_response",
    "raw_from_mapping",
    "source_for_name",
]
