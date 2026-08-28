# xiaohongshu-mcp Linux ARM64 适配记录

本文记录 RetailTide 在 Linux ARM64 云主机上部署 `xiaohongshu-mcp` 与 Spider bridge 时实际做过的兼容改动、验证结果和维护边界。它是技术说明，不绑定 Docker Compose、systemd、Caddy、Nginx 或 Traefik 等具体部署工具。

## 上游基线

- 项目：`xpzouying/xiaohongshu-mcp`
- 版本：v2.5.0
- 固定提交：`6583124dfda92312b6bc19a042a6acfae63fe498`
- 许可证：Apache-2.0
- 目标平台：Linux ARM64 / AArch64

适配时，上游 Dockerfile 将 Go 构建目标固定为 `GOARCH=amd64`，并只下载 `linux-x64.tar.xz` 浏览器。上游浏览器分发目录没有 Linux ARM64 构建，因此仅修改 `GOARCH` 不能得到可工作的 ARM64 登录环境。

## 对 xiaohongshu-mcp 的源码改动

### 1. 显式外部浏览器入口

在 `browser/browser_download.go::EnsureBrowser()` 增加 `XHS_BROWSER_BIN`：

- 未设置时完全保留上游内置浏览器选择与校验逻辑。
- 设置时检查路径存在、不是目录且具有可执行位。
- 检查失败直接返回错误，不允许 Rod 静默下载或选择其他浏览器。
- Linux ARM64 镜像将其设置为 `/usr/bin/chromium`。

这个改动解决“上游没有 Linux ARM64 浏览器包”的启动阻塞，但不意味着 Debian Chromium 等价于上游定制浏览器。

### 2. 普通 Chromium 的兼容隐身模式

上游定制浏览器支持 `fingerprint`、`fingerprint-platform` 和 `fingerprint-brand` 等源码级参数。Debian Chromium 不支持这些参数，继续传递会造成“日志声称指纹已启用、浏览器实际忽略”的无声降级，并曾导致 Rod 页面目标关闭。

在 `browser/browser.go::NewBrowser()` 中增加分支：

- 使用上游内置浏览器时保留原有源码级指纹路径。
- 设置 `XHS_BROWSER_BIN` 时不再传递定制浏览器参数，改用 `headless_browser.WithStealthJS(true)`。
- 日志明确标记 `operator-supplied browser uses JS stealth compatibility mode`。

该分支修复了 ARM Chromium 下 `/api/v1/login/status` 返回 HTTP 500、`Inspected target navigated or closed` 的问题。

## ARM64 镜像构建内容

构建流程固定上游提交，并使用 BuildKit 的 `TARGETOS/TARGETARCH`：

1. 拉取固定提交并应用上述补丁。
2. 使用 Go 1.24、`CGO_ENABLED=0` 构建 `linux/arm64` 二进制。
3. 运行层使用 Debian Bookworm ARM64。
4. 安装 Debian ARM64 Chromium、中文字体和 `tini`。
5. 为人工验证码流程额外安装 Xvfb、Openbox、x11vnc、noVNC、Websockify 与 Supervisor；正常模式不启动这些进程。

验证过的本地镜像标签为：

```text
retailtide/xiaohongshu-mcp-arm64:v2.5.0-6583124
```

镜像必须满足：

```text
Architecture=arm64
uname -m=aarch64
XHS_BROWSER_BIN=/usr/bin/chromium
```

## Spider_XHS 与 bridge

这次 ARM 部署没有修改 `cv-cat/Spider_XHS` 上游源码。RetailTide 继续使用仓库已有的 `integrations/xhs_spider_bridge`：

- 固定 Spider_XHS 提交 `e1888d712519040f5fcc294baeac4b9505b25c98`。
- bridge 本身原生构建 ARM64。
- 只开放搜索、详情和健康检查。
- 搜索默认硬超时 45 秒，详情默认硬超时 30 秒。
- 每个操作放入可替换子进程，超时后终止并重建 worker。
- 将登录失效、限流、风控、上游超时和响应结构错误映射为稳定错误码。
- 禁用可能在异常日志中输出临时详情 URL 的上游日志 sink。

Spider 与 MCP 是同一个逻辑来源的两个 transport，不能统计为两个独立信息源。MCP 登录状态也不能自动复制给 Spider；两者的会话边界保持独立。

## 运行验证

ARM64 实机验证覆盖：

- MCP `/health` 返回 HTTP 200。
- MCP Streamable HTTP `tools/list` 返回登录工具。
- ARM Chromium 能执行 `/api/v1/login/status`。
- 账号所有者通过可见浏览器完成验证码后，状态返回 `is_logged_in: true`。
- 切回无头模式并重启容器后，登录状态仍为 `true`。
- 正常模式不运行 Xvfb/noVNC，且不监听 noVNC 端口。

## 限制与升级要求

- Debian Chromium 不是上游定制浏览器，不能保证具有相同的指纹一致性或风控表现。
- 扫码可能触发短信验证码；项目不增加接收或提交验证码的 MCP Tool。
- 不自动登录、不处理 CAPTCHA、不读取或迁移 Cookie。
- 每次升级上游提交或 Chromium 版本，都必须重新验证健康检查、登录状态、人工验证码流程、只读搜索和详情。
- 容器健康或登录成功不是采集完成证明；恢复任务前仍需检查日期覆盖、去重、详情发布时间和失败状态。
