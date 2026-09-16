"""One bounded smoke read, with no content/database writes or session inspection."""

import asyncio
import json

from retail_tide.cli import _exclusive_refresh_lock
from retail_tide.config import get_settings
from retail_tide.sources.base import SourceError
from retail_tide.sources.xhs_control import XhsControl
from retail_tide.sources.xiaohongshu import XiaohongshuSource
from retail_tide.time import scheduled_post_window


async def main():
    settings = get_settings()
    control = XhsControl()
    with _exclusive_refresh_lock(settings):
        source = XiaohongshuSource(
            credential=settings.source_credential("xiaohongshu"),
            spider_endpoint=settings.xiaohongshu_spider_endpoint,
            spider_credential=settings.xiaohongshu_spider_credential(),
            use_fixture=False,
            max_detail_requests=1,
            spider_max_detail_requests=1,
            min_request_interval=settings.request_interval("xiaohongshu"),
            search_cooldown=settings.xiaohongshu_search_cooldown,
            page_cooldown=settings.xiaohongshu_page_cooldown,
            detail_cooldown=settings.xiaohongshu_detail_cooldown,
        )
        since, until = scheduled_post_window()
        try:
            result = await source.collect("股票", since, until=until)
            print(
                json.dumps(
                    {
                        "status": "partial" if result.partial else "read_completed",
                        "items": len(result.items),
                        "search_transport": result.diagnostics.get("search_transport"),
                        "detail_successes": result.diagnostics.get("detail_successes"),
                        "database_writes": False,
                    },
                    ensure_ascii=False,
                )
            )
        except SourceError as exc:
            print(
                json.dumps(
                    {
                        "status": "blocked",
                        "error_type": type(exc).__name__,
                        "error_code": getattr(exc, "error_code", None),
                    },
                    ensure_ascii=False,
                )
            )
        state = control.read()
        print(
            json.dumps(
                {key: state.get(key) for key in ("paused", "reason", "retry_at")},
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
