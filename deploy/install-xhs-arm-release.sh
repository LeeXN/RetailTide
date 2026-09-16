#!/usr/bin/env bash
set -euo pipefail

# Run only after both candidate images have been built and checked.
release=/opt/retail-tide-ops/releases/xhs-20260915
test "$(uname -m)" = aarch64
test -d "$release/src/retail_tide"
test ! -e "$release/installed"
for unit in retail-tide-posts.service retail-tide-wikimedia.service retail-tide-xhs-collect.service retail-tide-xiaohongshu.service; do
    if systemctl is-active --quiet "$unit"; then
        echo "Active collection unit: $unit" >&2
        exit 1
    fi
done
docker image inspect retail-tide:xhs-20260915 >/dev/null
docker image inspect retailtide/xiaohongshu-mcp-arm64:read-20260915 >/dev/null

# Backups deliberately exclude credentials and the MCP-owned runtime directory.
mkdir -p "$release/rollback"
tar -czf "$release/rollback/application.tar.gz" -C /opt/retail-tide src README.md README.en.md
tar -czf "$release/rollback/checkpoints.tar.gz" -C /opt/retail-tide var/state
cp -p /opt/retail-tide-ops/systemd/retail-tide-posts-yesterday.timer "$release/rollback/posts-ops.timer"
cp -p /etc/systemd/system/retail-tide-posts-yesterday.timer "$release/rollback/posts-installed.timer"
docker tag retail-tide:local retail-tide:before-xhs-20260915

systemctl stop retail-tide-posts-yesterday.timer
install -m 0644 "$release/deploy/retail-tide-posts-yesterday.timer" /opt/retail-tide-ops/systemd/retail-tide-posts-yesterday.timer
install -m 0644 "$release/deploy/retail-tide-posts-yesterday.timer" /etc/systemd/system/retail-tide-posts-yesterday.timer
install -m 0644 "$release/deploy/retail-tide-xiaohongshu-arm.service" /opt/retail-tide-ops/systemd/retail-tide-xiaohongshu.service
install -m 0644 "$release/deploy/retail-tide-xiaohongshu-arm.service" /etc/systemd/system/retail-tide-xiaohongshu.service
install -m 0644 "$release/deploy/retail-tide-xiaohongshu-yesterday.timer" /opt/retail-tide-ops/systemd/retail-tide-xiaohongshu-yesterday.timer
install -m 0644 "$release/deploy/retail-tide-xiaohongshu-yesterday.timer" /etc/systemd/system/retail-tide-xiaohongshu-yesterday.timer
install -m 0644 "$release/deploy/xhs-read-lifecycle.compose.yaml" /opt/retail-tide-ops/compose/xhs-read-lifecycle.compose.yaml
mkdir -p /etc/systemd/system/retail-tide-xhs.service.d /opt/retail-tide-ops/systemd/retail-tide-xhs.service.d
install -m 0644 "$release/deploy/xhs-read-lifecycle.conf" /etc/systemd/system/retail-tide-xhs.service.d/read-lifecycle.conf
install -m 0644 "$release/deploy/xhs-read-lifecycle.conf" /opt/retail-tide-ops/systemd/retail-tide-xhs.service.d/read-lifecycle.conf

cp -a "$release/src/." /opt/retail-tide/src/
install -m 0644 "$release/README.md" "$release/README.en.md" /opt/retail-tide/
docker tag retail-tide:xhs-20260915 retail-tide:local
docker compose --env-file /opt/retail-tide/.env -f /opt/retail-tide-ops/compose/retail-tide.compose.yaml run --rm --no-deps api split-xiaohongshu-schedule
systemd-analyze verify /etc/systemd/system/retail-tide-xiaohongshu.service /etc/systemd/system/retail-tide-xiaohongshu-yesterday.timer
systemctl daemon-reload
systemctl reload retail-tide.service
systemctl reload retail-tide-xhs.service
systemctl enable --now retail-tide-posts-yesterday.timer retail-tide-xiaohongshu-yesterday.timer
touch "$release/installed"
systemctl list-timers --all --no-pager 'retail-tide-*'
