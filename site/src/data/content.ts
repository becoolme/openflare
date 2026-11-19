// Page copy and data. Every claim here is checked against the repo.
const B = (k: string, s: 'ok' | 'lim', note: string) => ({ k, note, ok: s === 'ok', lim: s === 'lim' });

export const content = {
  steps: [
    { n: '1', t: 'Request arrives', d: 'HTTP, or an HTTP/1.1 WebSocket upgrade, hits the vrt-host ingress.' },
    { n: '2', t: 'Trust headers reset', d: 'Headers that cross the trust boundary are stripped and rewritten.' },
    { n: '3', t: 'Routed by Host', d: 'The Host header picks the tenant, from hostnames or wrangler routes.' },
    { n: '4', t: 'Woken if idle', d: 'A tenant scaled to zero is started before the request is forwarded.' },
    { n: '5', t: 'Handed to workerd', d: 'Forwarded over a unix socket to that tenant’s own workerd process.' },
    { n: '6', t: 'Bindings called', d: 'Facade workers turn env.KV, env.DB and friends into calls to vrt-provider and vrt-sql.' },
    { n: '7', t: 'Egress checked', d: 'Outbound fetch() and connect() are denied for private, link-local and metadata addresses.' }
  ],
  binds: [
    B('KV', 'ok', 'Filesystem or Kvrocks, through vrt-provider.'),
    B('R2', 'lim', 'Filesystem or any S3-compatible store; conditional writes depend on the backend.'),
    B('D1', 'lim', 'vrt-sql data plane under workerd’s d1-api; the server mints database IDs.'),
    B('Durable Objects', 'lim', 'SQL storage and migrations; transferred_classes is rejected.'),
    B('Queues', 'lim', 'Producers, consumers, retries, concurrency and dead-letter queues on vrt-sql.'),
    B('Workflows', 'lim', 'Vendored engine as a Durable Object; class must extend WorkflowEntrypoint.'),
    B('Cron Triggers', 'ok', 'Delivered to your scheduled() handler.'),
    B('Hyperdrive', 'lim', 'Postgres wire proxy; credentials stay outside the isolate.'),
    B('Rate limiting', 'lim', 'wrangler ratelimits with limit and period; per-process fixed window.'),
    B('Static assets', 'lim', 'Assets directory and binding, with not_found_handling and deploy caps.'),
    B('Cache API', 'lim', 'caches.default and caches.open; per-host and in-memory.'),
    B('Service bindings', 'lim', 'wrangler services; default entrypoint only.'),
    B('WebSockets', 'lim', 'Passthrough to Workers and Durable Objects, with session caps.'),
    B('Vars and secrets', 'lim', 'vars arrive as strings; secrets via vrt secret put.')
  ],
  limits: [
    { k: 'WebSocket upgrades', v: 'HTTP/1.1 only' },
    { k: 'WebSocket idle', v: '300 s' },
    { k: 'WebSocket lifetime', v: '4 h max' },
    { k: 'Sessions per ingress', v: '4,096' },
    { k: 'Sessions per tenant', v: '256 by default' },
    { k: 'Outbound network', v: 'public addresses only' },
    { k: 'DO migrations', v: 'transferred_classes rejected' }
  ],
  qs: [
    { n: '01', t: 'Supply workerd', d: 'Copy your workerd binary in and read the pinned digest from the lock file.', code: "git clone https://github.com/becoolme/openflare\ncd openflare\ncp /path/to/workerd deploy/standalone/workerd\nWORKERD_SHA256=$(node -p \"JSON.parse(require('fs')\n  .readFileSync('deploy/docker/workerd.lock','utf8'))\n  .artifacts['linux-x64'].binarySha256\")" },
    { n: '02', t: 'Start the stack', d: 'Compose builds and starts every service and waits until they report healthy.', code: "WORKERD_SOURCE=file \\\nWORKERD_SHA256=$WORKERD_SHA256 \\\ndocker compose \\\n  -f deploy/standalone/docker-compose.yml \\\n  up --build --wait" },
    { n: '03', t: 'Deploy and call', d: 'Copy in the hello example, set its secret, deploy it and send a request.', code: "dc=\"docker compose -f deploy/standalone/docker-compose.yml\"\n$dc exec vrt mkdir -p /tmp/examples\n$dc cp examples/standalone-hello vrt:/tmp/examples/\nprintf 'my-secret' | $dc exec -T vrt \\\n  vrt secret put --root /data greeting-secret\n$dc exec vrt vrt deploy --root /data \\\n  /tmp/examples/standalone-hello\ncurl -H 'Host: hello.vrt.local' http://127.0.0.1:8080/" }
  ],
  faq: [
    { q: 'Is OpenFlare a fork of workerd?', a: 'No. It runs the open-source workerd binary unmodified, pinned by digest in deploy/docker/workerd.lock. OpenFlare is the platform around it: ingress, management plane, binding services and signed deployments.' },
    { q: 'Is it affiliated with Cloudflare?', a: 'No. It is an independent open-source project. “Workers” is used only to describe API compatibility, which comes from running workerd.' },
    { q: 'Does it download workerd for me?', a: 'No. You supply the binary, and the build checks it against the pinned SHA-256. For smoke tests without workerd, use WORKERD_SOURCE=stub.' },
    { q: 'What happens to wrangler keys it doesn’t support?', a: 'account_id, placement and hosted resource IDs are accepted and ignored. Keys it can’t honour, such as ai or vectorize, fail the deploy with a typed error that names them.' },
    { q: 'Where does my data live?', a: 'In services you run: vrt-provider serves KV (filesystem or Kvrocks) and R2 (filesystem or S3-compatible); vrt-sql holds D1 and queue state; Durable Object storage is workerd localDisk under the host doStorageDir.' },
    { q: 'How are tenants isolated?', a: 'Each tenant gets its own workerd process with its own config, sockets and limits, supervised by vrt-host. Outbound traffic passes an egress filter.' },
    { q: 'Is it production-ready?', a: 'It is early and actively developed. Wire formats and the CLI may change between releases, so pin a commit and read the history before upgrading.' }
  ]
};
