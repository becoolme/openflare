# T14/B1 MVP single-node deployment example (non-production keys, structure illustration only)

cell-controller.toml (same node, one delivery directory per pod):

```toml
cell_id = "cell-mvp-1"
control_plane_url = "https://cp.internal"
token_file = "/run/secrets/vrt-cell-token"
health_listen = "0.0.0.0:9090"
poll_interval_secs = 5
fence_file = "/var/lib/vrt/cell-fence.json"
# MVP: no K8s NetworkPolicy applier -- an explicit escape hatch.
# /healthz will report egressPolicyDisabled: true for the whole lifetime; startup emits a structured warn.
egress_required = false
[[trusted_keys]]
keyId = "publish-key-1"
publicKey = "<base64-ed25519-pub>"
[[pods]]
id = "pod-0"
dir = "/var/lib/vrt/pods/pod-0/delivery"   # 0700, owned by the pod uid
readyz = "http://127.0.0.1:9091"
```

vrt-host.toml (pod-0):

```toml
cell_id = "cell-mvp-1"
workerd_binary = "/usr/local/bin/workerd"
work_dir = "/var/lib/vrt/pods/pod-0/work"
cache_dir = "/var/lib/vrt/pods/pod-0/cache"
listen_addr = "127.0.0.1:8080"
control_listen = "127.0.0.1:9091"          # readyz/healthz (loopback enforced)

# T15/B2: control-plane bundle fetch; if omitted, use the local cacheDir/source directory (dev)
[bundles]
base_url = "https://cp.internal/api/internal/clusters/cl_mvp_1"
token_file = "/run/secrets/vrt-cell-token"  # 0600, scoped ingest token
request_timeout_ms = 10000
deadline_ms = 30000
max_attempts = 3
max_bytes = 33554432                        # 32 MiB (same as the upload limit)
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

Delivery contract: `envelope.json` = the signed `compute.desired`, verbatim;
`assignment.json` = `{podId, revision, payloadDigest,
envelopeDigest, generatedAt, workers:[{tenantId,workerId}]}`.
The host only adopts pairs whose digest matches; the controller sends ACK APPLIED only after the pod's readyz reports
`ready && revision==target && payloadDigest==signed`.

Controller readiness has two endpoints on the health listener:

- `GET /readyz` is the serving check the kubelet probes. It fails closed on a
  NetworkPolicy apply failure, a missing or expired desired snapshot, a probe
  failure or a not-ready pod. A ready pod that is still on the previous revision
  keeps it passing for up to 120 s after the pod last proved an applied revision,
  so a tenant deploy does not pull a single-replica cell out of the Service
  endpoints while the host switches generations.
- `GET /readyz/converged` passes only when every pod loaded the applied revision.
  Use it in rollout checks that need to know a revision has landed everywhere.
