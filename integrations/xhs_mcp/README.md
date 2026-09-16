# ARM MCP 读取操作补丁

基线：`xpzouying/xiaohongshu-mcp` 提交 `6583124dfda92312b6bc19a042a6acfae63fe498`。

在该提交应用 `browser-arm64.patch` 和 `read-lifecycle.patch`，将 `read_operation.go`、`read_operation_test.go` 放到源码根目录。运行 `go test ./...`，再执行：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags='-s -w -X main.version=v2.5.0-retailtide-read-20260915' -o xiaohongshu-mcp .
```

将二进制与 `Dockerfile.overlay` 放入独立构建目录，在 ARM 主机基于已验证的旧镜像构建新标签。不要移动或复制会话目录。记录旧镜像 ID、新二进制和补丁 SHA-256，保留旧镜像用于回滚。

补丁覆盖 HTTP/MCP 共用的登录状态、列表、搜索和详情读取服务：同进程串行、45 秒操作期限、取消时回收页面后释放操作槽、无临时 URL 的生命周期日志。搜索/详情按数据就绪条件等待，详情只导航一次。采集请求冷却、跨进程互斥及账户暂停由 RetailTide 管理。

现有 ARM 浏览器兼容补丁保持原样，本补丁不新增指纹伪装或模拟用户交互。登录、发布等人工操作不由本补丁自动执行。
