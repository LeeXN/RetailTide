#!/usr/bin/env bash
set -euo pipefail
release=/opt/retail-tide-ops/releases/xhs-20260915
test -f "$release/rollback/application.tar.gz"
for unit in retail-tide-posts.service retail-tide-wikimedia.service retail-tide-xhs-collect.service retail-tide-xiaohongshu.service; do
    if systemctl is-active --quiet "$unit"; then
        echo "Stop the active collection deliberately before rollback: $unit" >&2
        exit 1
    fi
done
systemctl stop retail-tide-posts-yesterday.timer
systemctl disable --now retail-tide-xiaohongshu-yesterday.timer
cp -p "$release/rollback/posts-ops.timer" /opt/retail-tide-ops/systemd/retail-tide-posts-yesterday.timer
cp -p "$release/rollback/posts-installed.timer" /etc/systemd/system/retail-tide-posts-yesterday.timer
mv /etc/systemd/system/retail-tide-xhs.service.d/read-lifecycle.conf "$release/rollback/disabled-read-lifecycle.conf"
mv /opt/retail-tide-ops/systemd/retail-tide-xhs.service.d/read-lifecycle.conf "$release/rollback/disabled-ops-read-lifecycle.conf"
tar -xzf "$release/rollback/application.tar.gz" -C /opt/retail-tide
docker tag retail-tide:before-xhs-20260915 retail-tide:local
# Keep new checkpoints and control state: restoring an old snapshot could lose progress.
systemctl daemon-reload
systemctl reload retail-tide.service
systemctl reload retail-tide-xhs.service
systemctl start retail-tide-posts-yesterday.timer
