from __future__ import annotations

import json
import stat
import time

import pytest

import retail_tide.cli as cli_module
from retail_tide.config import Settings, source_config_status
from retail_tide.source_sessions import (
    SourceSessionError,
    delete_source_session,
    import_source_session,
    source_session_cookie_header,
    source_session_request_headers,
    source_session_status,
)


def test_imports_curl_cookie_without_echoing_secrets(tmp_path):
    source_file = tmp_path / "authenticated-request.curl"
    source_file.write_text(
        "curl 'https://www.tgb.cn/search/getSearchTopicResult' "
        "-H 'Accept: application/json' "
        "-H 'Cookie: JSESSIONID=example-session-secret; userToken=example-token-secret'",
        encoding="utf-8",
    )
    target = tmp_path / "auth" / "taoguba.session.json"

    result = import_source_session("taoguba", source_file, target)

    assert result["state"] == "ready"
    assert result["cookie_count"] == 2
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert stat.S_IMODE(target.parent.stat().st_mode) == 0o700
    assert source_session_cookie_header("taoguba", target) == (
        "JSESSIONID=example-session-secret; userToken=example-token-secret"
    )
    assert "example-session-secret" not in str(result)
    assert "example-token-secret" not in str(result)


def test_imports_only_browser_fingerprint_headers_from_curl(tmp_path):
    source_file = tmp_path / "authenticated-request.curl"
    source_file.write_text(
        "curl 'https://guba.eastmoney.com/list,518880,f.html' "
        "-H 'Cookie: session=example-secret' "
        "-H 'User-Agent: Example Browser/1.0' "
        "-H 'Accept: text/html,application/xhtml+xml' "
        "-H 'Accept-Language: zh-CN,zh;q=0.9' "
        "-H 'Sec-Fetch-Dest: document' "
        "-H 'Sec-Fetch-Mode: navigate' "
        "-H 'Sec-Fetch-Site: same-origin' "
        "-H 'Upgrade-Insecure-Requests: 1' "
        "-H 'Sec-CH-UA-Platform: \"Linux\"' "
        "-H 'Referer: https://untrusted.example/' "
        "-H 'X-Api-Key: example-api-secret'",
        encoding="utf-8",
    )
    target = tmp_path / "auth" / "guba.session.json"

    result = import_source_session("guba", source_file, target)

    assert result["request_header_names"] == [
        "accept",
        "accept-language",
        "sec-ch-ua-platform",
        "sec-fetch-dest",
        "sec-fetch-mode",
        "sec-fetch-site",
        "upgrade-insecure-requests",
        "user-agent",
    ]
    assert source_session_request_headers("guba", target) == {
        "accept": "text/html,application/xhtml+xml",
        "accept-language": "zh-CN,zh;q=0.9",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "upgrade-insecure-requests": "1",
        "user-agent": "Example Browser/1.0",
    }
    saved = target.read_text(encoding="utf-8")
    assert "Referer" not in saved
    assert "X-Api-Key" not in saved
    assert "example-api-secret" not in saved


def test_imports_browser_cookie_header_with_an_empty_cookie_value(tmp_path):
    source_file = tmp_path / "authenticated-request.curl"
    source_file.write_text(
        "curl 'https://www.tgb.cn/search/getSearchTopicResult' "
        "-H 'Cookie: JSESSIONID=example-session-secret; optionalFlag=; userToken=token'",
        encoding="utf-8",
    )
    target = tmp_path / "auth" / "taoguba.session.json"

    result = import_source_session("taoguba", source_file, target)

    assert result["state"] == "ready"
    assert result["cookie_count"] == 3
    assert source_session_cookie_header("taoguba", target) == (
        "JSESSIONID=example-session-secret; optionalFlag=; userToken=token"
    )


def test_storage_state_keeps_only_active_cookies_for_taoguba(tmp_path):
    source_file = tmp_path / "storage-state.json"
    source_file.write_text(
        json.dumps(
            {
                "cookies": [
                    {
                        "name": "active",
                        "value": "example-active-secret",
                        "domain": ".tgb.cn",
                        "path": "/",
                        "expires": time.time() + 3600,
                    },
                    {
                        "name": "expired",
                        "value": "example-expired-secret",
                        "domain": ".tgb.cn",
                        "path": "/",
                        "expires": time.time() - 1,
                    },
                    {
                        "name": "unrelated",
                        "value": "example-other-secret",
                        "domain": ".example.test",
                        "path": "/",
                        "expires": time.time() + 3600,
                    },
                ],
                "origins": [{"origin": "https://unrelated.example.test"}],
            }
        ),
        encoding="utf-8",
    )
    target = tmp_path / "auth" / "taoguba.session.json"

    result = import_source_session("taoguba", source_file, target)

    assert result["cookie_count"] == 1
    assert source_session_cookie_header("taoguba", target) == "active=example-active-secret"
    saved = json.loads(target.read_text(encoding="utf-8"))
    assert [cookie["name"] for cookie in saved["cookies"]] == ["active"]
    assert "origins" not in saved


def test_rejects_unsafe_saved_session_permissions(tmp_path):
    source_file = tmp_path / "cookie.txt"
    source_file.write_text("Cookie: session=example-secret", encoding="utf-8")
    target = tmp_path / "auth" / "taoguba.session.json"
    import_source_session("taoguba", source_file, target)
    target.chmod(0o644)

    with pytest.raises(SourceSessionError, match="permissions are too broad"):
        source_session_cookie_header("taoguba", target)
    status = source_session_status("taoguba", target)
    assert status["state"] == "invalid"
    assert status["configured"] is False
    assert "example-secret" not in str(status)


def test_refuses_symlink_session_output_and_unsupported_source(tmp_path):
    source_file = tmp_path / "cookie.txt"
    source_file.write_text("session=example-secret", encoding="utf-8")
    real_target = tmp_path / "real.json"
    real_target.write_text("keep", encoding="utf-8")
    linked_target = tmp_path / "linked.json"
    linked_target.symlink_to(real_target)

    with pytest.raises(SourceSessionError, match="must not be a symlink"):
        import_source_session("taoguba", source_file, linked_target)
    with pytest.raises(SourceSessionError, match="not supported"):
        import_source_session("zhihu", source_file, tmp_path / "zhihu.json")
    assert real_target.read_text(encoding="utf-8") == "keep"


def test_guba_session_keeps_only_eastmoney_cookies(tmp_path):
    source_file = tmp_path / "storage-state.json"
    source_file.write_text(
        json.dumps(
            {
                "cookies": [
                    {
                        "name": "eastmoney_session",
                        "value": "example-eastmoney-secret",
                        "domain": ".eastmoney.com",
                        "path": "/",
                        "expires": time.time() + 3600,
                    },
                    {
                        "name": "unrelated",
                        "value": "example-other-secret",
                        "domain": ".example.test",
                        "path": "/",
                        "expires": time.time() + 3600,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    target = tmp_path / "auth" / "guba.session.json"

    result = import_source_session("guba", source_file, target)

    assert result["cookie_count"] == 1
    assert source_session_cookie_header("guba", target) == (
        "eastmoney_session=example-eastmoney-secret"
    )
    assert "example-eastmoney-secret" not in str(result)


def test_logout_is_idempotent_and_does_not_touch_remote_account(tmp_path):
    source_file = tmp_path / "cookie.txt"
    source_file.write_text("session=example-secret", encoding="utf-8")
    target = tmp_path / "auth" / "taoguba.session.json"
    import_source_session("taoguba", source_file, target)

    assert delete_source_session("taoguba", target)["removed"] is True
    assert delete_source_session("taoguba", target)["removed"] is False
    assert not target.exists()


def test_settings_and_status_report_session_readiness_without_values(monkeypatch, tmp_path):
    source_file = tmp_path / "cookie.txt"
    source_file.write_text("session=example-secret", encoding="utf-8")
    target = tmp_path / "auth" / "taoguba.session.json"
    import_source_session("taoguba", source_file, target)
    settings = Settings(taoguba_session_file=target)

    status = source_config_status("taoguba", settings=settings)

    assert status["configured"] is True
    assert status["browser_session"]["state"] == "ready"
    assert "example-secret" not in str(status)

    captured = {}
    monkeypatch.setattr(cli_module, "get_settings", lambda: settings)
    monkeypatch.setattr(cli_module, "_json", lambda value: captured.update(value))
    cli_module.source_auth_status("taoguba", session_file=None)
    assert captured["state"] == "ready"
    assert "example-secret" not in str(captured)


def test_settings_resolve_separate_guba_and_taoguba_sessions(tmp_path):
    settings = Settings(
        guba_session_file=tmp_path / "guba.json",
        taoguba_session_file=tmp_path / "taoguba.json",
    )

    assert settings.source_session_file("guba") == tmp_path / "guba.json"
    assert settings.source_session_file("taoguba") == tmp_path / "taoguba.json"
