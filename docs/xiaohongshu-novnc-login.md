# 小红书 MCP 的 noVNC 人工登录方案

当扫码触发短信验证码时，纯 API 登录无法展示验证码输入框。noVNC 方案的目标是临时显示 MCP 所使用的真实 Chromium，让账号所有者在小红书官方页面亲自完成验证；它不是验证码自动化或风控绕过方案。

## 适用场景

- `get_login_qrcode` 能返回二维码。
- 手机扫码后触发短信验证码。
- 手机端看似成功，但服务端 `/api/v1/login/status` 仍返回 `false`。
- 后台日志没有确认会话已经保存。

若 MCP 已经返回 `is_logged_in: true`，不要启用 noVNC。

## 组成

临时登录模式通常包含：

```text
MCP -headless=false
        │
        ▼
   Chromium ── DISPLAY=:99 ── Xvfb
                                  │
                               x11vnc
                                  │
                              Websockify
                                  │
                                noVNC
                                  │
                     TLS + 外层认证反向代理
```

Openbox 等轻量窗口管理器可避免 Chromium 窗口尺寸和焦点异常。Supervisor 或等价进程管理器负责启动顺序、重启和退出回收。

## 网络与认证要求

- MCP API、VNC 和 noVNC 默认都只绑定回环或私有容器网络。
- 不直接公开 VNC 5900 或 noVNC 6080。
- 临时入口必须使用 TLS。
- noVNC 外层必须再加高强度认证，例如反向代理 Basic Auth、SSO 或短期访问令牌。
- 传统 VNC 密码通常只有前 8 个字符有效，不能作为唯一公网安全边界。
- 反向代理必须支持 WebSocket Upgrade，并正确转发 noVNC 的 `/websockify` 路径。
- 临时入口应使用独立路径或监听器，登录完成后立即移除。

反向代理可以是 Caddy、Nginx、Traefik 或其他实现；仓库不规定具体产品。配置只需满足：

1. TLS 终止。
2. 外层强认证。
3. 静态 noVNC 页面代理。
4. WebSocket 代理。
5. `Cache-Control: no-store`。
6. 登录结束后可完整撤销。

## 操作流程

### 1. 进入临时可见模式

临时覆盖应完成以下动作：

- 设置 `XHS_LOGIN_UI_ENABLED=true`。
- 为 MCP 进程设置 `DISPLAY=:99` 和 `-headless=false`。
- 启动 Xvfb、窗口管理器、x11vnc 与 Websockify。
- 只在临时覆盖中挂载 noVNC 密码和短期 TLS 证书。
- 让反向代理临时连接 noVNC 所在私有网络。

正常服务定义应始终保持 `XHS_LOGIN_UI_ENABLED=false`，不挂载登录界面密钥，也不暴露 noVNC 端口。

### 2. 打开 noVNC

浏览器先通过反向代理的外层认证，再输入内层 VNC 密码。成功连接后看到黑色桌面并不代表故障：MCP 只有在调用二维码接口时才创建 Chromium 窗口。

### 3. 只调用一次二维码接口

在 MCP 所在机器上调用：

```bash
curl -fsS \
  -H "Authorization: Bearer ${XHS_MCP_TOKEN}" \
  http://127.0.0.1:18060/api/v1/login/qrcode \
  -o /tmp/xhs-qrcode-response.json
```

Chromium 随后出现在 noVNC 桌面。不要连续调用二维码接口；新的请求会取消上一轮后台等待会话。

### 4. 由账号所有者完成人工验证

账号所有者在小红书官方页面扫码并输入短信验证码。验证码不提交给 MCP、不写入脚本、不进入日志，也不交给自动化代理。

### 5. 以服务端状态验收

手机端提示不能代替服务端验收：

```bash
curl -fsS \
  -H "Authorization: Bearer ${XHS_MCP_TOKEN}" \
  http://127.0.0.1:18060/api/v1/login/status
```

只有明确返回 `is_logged_in: true` 才算完成。

### 6. 立即撤销临时入口

1. 切回基础服务定义和 `XHS_LOGIN_UI_ENABLED=false`。
2. 重建或重启 MCP。
3. 再次确认 `is_logged_in: true`，证明登录状态已经持久化。
4. 移除反向代理临时路由、外层密码哈希和明文密码。
5. 删除 noVNC 密码、短期证书和二维码临时文件。
6. 确认 Xvfb、x11vnc、Websockify 与 Supervisor 不再运行。
7. 确认 5900/6080 没有公网监听。

## 常见故障

### noVNC 是黑屏

先确认是否已经调用二维码接口。没有 Chromium 进程、没有“等待扫码登录”日志时，黑屏只是空的 Xvfb 桌面。

### 没有密码输入框或连接立即失败

检查 noVNC 是否带了自动连接参数、WebSocket 路径是否正确、反向代理是否把认证信息应用到 WebSocket 请求，以及 x11vnc 是否正在等待密码认证。

### 手机端成功但服务端仍是 false

查看不含会话内容的生命周期日志，区分“仍在等待”“验证码未完成”“保存失败”和“会话超时”。不要检查、打印或复制 Cookie。

### 重启后登录丢失

确认 MCP 自己的持久化数据目录未变化。不要从其他机器或 Spider 复制会话文件来修复。

## 安全边界

- 不新增接收短信验证码的 Tool 或 API。
- 不自动定位或填写验证码输入框。
- 不截图、读取或记录验证码。
- 不读取、复制、打印或传输 Cookie/会话文件。
- 登录失败时停止小红书采集，不让其拖住其他来源。
