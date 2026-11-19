# T14/B1 MVP 单机部署示例（非生产密钥，仅结构示意）

cell-controller.toml（同节点，每 pod 一个投递目录）：

```toml
cell_id = "cell-mvp-1"
control_plane_url = "https://cp.internal"
token_file = "/run/secrets/vrt-cell-token"
health_listen = "0.0.0.0:9090"
poll_interval_secs = 5
fence_file = "/var/lib/vrt/cell-fence.json"
# MVP: 无 K8s NetworkPolicy applier —— 显式逃生门。
# /healthz 将长期挂 egressPolicyDisabled: true，启动期有结构化 warn。
egress_required = false
[[trusted_keys]]
keyId = "publish-key-1"
publicKey = "<base64-ed25519-pub>"
[[pods]]
id = "pod-0"
dir = "/var/lib/vrt/pods/pod-0/delivery"   # 0700, pod uid 属主
readyz = "http://127.0.0.1:9091"
```

vrt-host.toml（pod-0）：

```toml
cell_id = "cell-mvp-1"
workerd_binary = "/usr/local/bin/workerd"
work_dir = "/var/lib/vrt/pods/pod-0/work"
cache_dir = "/var/lib/vrt/pods/pod-0/cache"
listen_addr = "127.0.0.1:8080"
control_listen = "127.0.0.1:9091"          # readyz/healthz（loopback 强制）

# T15/B2: 控制面 bundle 拉取；缺省则用 cacheDir/source 本地目录（dev）
[bundles]
base_url = "https://cp.internal/api/internal/clusters/cl_mvp_1"
token_file = "/run/secrets/vrt-cell-token"  # 0600，scoped ingest token
request_timeout_ms = 10000
deadline_ms = 30000
max_attempts = 3
max_bytes = 33554432                        # 32 MiB（与上传上限一致）
[desired]
path = "/var/lib/vrt/pods/pod-0/delivery/envelope.json"
assignment_file = "/var/lib/vrt/pods/pod-0/delivery/assignment.json"
pod_id = "pod-0"
reload_ms = 500
grace_versions = 1
[[desired.trusted_keys]]
keyId = "publish-key-1"
publicKey = "<base64-ed25519-pub>"
[render]
mode = "capnp"
params_file = "/etc/vrt/render-params.json"
```

交付契约：`envelope.json` = 原样签名 `compute.desired`；
`assignment.json` = `{podId, revision, payloadDigest,
envelopeDigest, generatedAt, workers:[{tenantId,workerId}]}`。
host 只采用 digest 匹配的对；控制器仅在 pod readyz 报告
`ready && revision==目标 && payloadDigest==签名` 后 ACK APPLIED。
