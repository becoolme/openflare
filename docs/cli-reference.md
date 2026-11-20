# CLI reference (placeholder)

A fuller `vrt` CLI reference will be added by the CLI work. This page
only lists commands that exist on the current tree — the usage string in
`vrt-local/src/bin/vrt.rs` is the source of truth.

Ops talk to `vrt serve` over its management unix socket while it is
live; with serve down they open the root directly (the writer lock
serializes against a running serve). Secret values come from stdin only
— never argv.

```text
vrt init --root <dir> --cell <id> --tenant <id> [--mode capnp|stub]
         [--facade-dist <dir>] [--workerd <path>] [--ingress-listen <addr>]
vrt serve --root <dir> [--supervise] [--vrt-host-bin <p>] [--vrt-provider-bin <p>]
vrt deploy --root <dir> <worker_dir>
vrt rollback --root <dir> <worker> <version_id>
vrt hostnames add --root <dir> <worker> <hostname>
vrt hostnames rm  --root <dir> <worker> <hostname>
vrt hostnames ls  --root <dir> <worker>
vrt secret put --root <dir> <secret_ref>        (value on stdin)
vrt secret rm  --root <dir> <secret_ref>
vrt secret ls  --root <dir>
vrt status  --root <dir>
vrt versions --root <dir> <worker>
vrt renew   --root <dir>
vrt key     --root <dir>                        (print trusted public key)
vrt health  --root <dir>                        (mgmt /healthz → exit code)
vrt probe   --url <http://host:port/path> | --file <path>
vrt do-snapshot --root <dir> --worker <name> --class <uniqueKey> --tenant <id>
vrt do-migrate  --root <dir> --worker <name> --class <uniqueKey> --tenant <id> --target-epoch <n>
vrt worker-id --root <dir> <name|workerId>      (print workerId, one line)
vrt do-key    --root <dir> <name|workerId> <className>  (print class uniqueKey)
vrt workflows --root <dir> <name|workerId>      (print declared workflows JSON)
vrt wfop --root <dir> --worker <name|id> --workflow <name> --op <create|get|action|delete|sendEvent>
         [--instance <id>] [--action <pause|resume|restart|terminate>]
         [--params <json>] [--event-type <t>] [--payload <json>] [--tenant <id>]
```

Two root-level ops are implemented but not in the usage banner:

```text
vrt metrics       --root <dir>                    (Prometheus text off the mgmt socket)
vrt queue-metrics --root <dir> (--queue-id <26 chars> | --queue-name <name> [--tenant <id>])
```
