using Workerd = import "/workerd/workerd.capnp";

# vrt tenant config; tenant=team-alpha revision=7
const config :Workerd.Config = (
  structuredLogging = true,
  services = [
    (name = "worker-w-alpha-1", worker = (
      compatibilityDate = "2026-11-01",
      limits = (subRequests = 100),
      modules = [
        (name = "index.js", esModule = embed "bundles/daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4/index.js"),
        (name = "blob.bin", data = embed "bundles/daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4/blob.bin"),
        (name = "cfg.json", json = embed "bundles/daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4/cfg.json"),
        (name = "lib/util.js", esModule = embed "bundles/daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4/lib/util.js"),
        (name = "mod.wasm", wasm = embed "bundles/daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4/mod.wasm"),
        (name = "vendor/legacy.cjs", commonJsModule = embed "bundles/daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4/vendor/legacy.cjs"),
      ],
      bindings = [
        (name = "LOG_LEVEL", text = "info"),
        (name = "API_TOKEN", fromEnvironment = "VRT_S_w_x2dalpha_x2d1__API_x5fTOKEN"),
        (name = "KV", service = (name = "facade-kv", entrypoint = "KVNamespace", props = (json = "{\"bindingId\":\"w-alpha-1~KV\",\"descriptorSha256\":\"daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4\",\"instanceId\":\"team-alpha\",\"namespaceResourceId\":\"kvns-01\",\"permissions\":{\"read\":true,\"write\":false},\"resourceSpecGeneration\":7,\"versionId\":\"v-2026-11-08-1\",\"workerId\":\"w-alpha-1\"}"))),
        (name = "MEDIA", r2Bucket = (name = "facade-r2", entrypoint = "R2Transport", props = (json = "{\"bindingId\":\"w-alpha-1~MEDIA\",\"descriptorSha256\":\"daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4\",\"instanceId\":\"team-alpha\",\"namespaceResourceId\":\"media-bucket-01\",\"permissions\":{\"read\":true,\"write\":true},\"resourceSpecGeneration\":7,\"versionId\":\"v-2026-11-08-1\",\"workerId\":\"w-alpha-1\"}"))),
        (name = "DB", service = (name = "facade-pg", entrypoint = "PgTransport", props = (json = "{\"bindingId\":\"w-alpha-1~DB\",\"descriptorSha256\":\"daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4\",\"instanceId\":\"team-alpha\",\"namespaceResourceId\":\"db-01\",\"permissions\":{\"read\":false,\"write\":false},\"resourceSpecGeneration\":7,\"versionId\":\"v-2026-11-08-1\",\"workerId\":\"w-alpha-1\"}"))),
        (name = "AUTH", service = (name = "worker-w-alpha-2")),
        (name = "DB1", wrapped = (moduleName = "cloudflare-internal:d1-api", innerBindings = [
            (name = "fetcher", service = (name = "facade-d1", entrypoint = "D1Facade", props = (json = "{\"bindingId\":\"w-alpha-1~DB1\",\"descriptorSha256\":\"daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4\",\"instanceId\":\"team-alpha\",\"namespaceResourceId\":\"abcdefghijklmnopqrstuvwxyz\",\"permissions\":{\"read\":true,\"write\":true},\"resourceSpecGeneration\":7,\"versionId\":\"v-2026-11-08-1\",\"workerId\":\"w-alpha-1\"}")))
        ])),
        (name = "JOBS", queue = (name = "facade-queue", entrypoint = "QueueFacade", props = (json = "{\"bindingId\":\"w-alpha-1~JOBS\",\"descriptorSha256\":\"daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4\",\"instanceId\":\"team-alpha\",\"namespaceResourceId\":\"qqqqqqqqqqqqqqqqqqqqqqqqqq\",\"permissions\":{\"read\":true,\"write\":true},\"queueName\":\"jobs\",\"resourceSpecGeneration\":7,\"versionId\":\"v-2026-11-08-1\",\"workerId\":\"w-alpha-1\"}"))),
        (name = "RATE_LIMITER", service = (name = "facade-ratelimit", entrypoint = "RateLimiter", props = (json = "{\"instanceId\":\"team-alpha\",\"limit\":40,\"namespaceId\":\"1001\",\"period\":60,\"workerId\":\"w-alpha-1\"}"))),
        (name = "ASSETS", service = (name = "facade-assets", entrypoint = "AssetTransport", props = (json = "{\"bindingId\":\"w-alpha-1~ASSETS\",\"descriptorSha256\":\"daa8eaa80a45398cae9a6f07666c0bda8c4e2a8c4288e732e0a36e083aa9f6a4\",\"instanceId\":\"team-alpha\",\"namespaceResourceId\":\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"permissions\":{\"read\":true,\"write\":false},\"resourceSpecGeneration\":7,\"versionId\":\"v-2026-11-08-1\",\"workerId\":\"w-alpha-1\"}"))),
      ],
      tails = [(name = "vrt-tail-w-alpha-1"), (name = "worker-w-alpha-3"), (name = "worker-w-alpha-2")],
      globalOutbound = (name = "vrt-egress", entrypoint = "EgressProxy"),
    )),
    (name = "worker-w-alpha-2", worker = (
      compatibilityDate = "2026-10-15",
      limits = (cpuMs = 10, memoryMb = 64),
      modules = [
        (name = "auth.js", esModule = embed "bundles/a240937c4aee30091d616ab4d6ee9014417f8f9fc6a1d53c92926f3bfa02fe41/auth.js"),
      ],
      bindings = [
      ],
      tails = [(name = "vrt-tail-w-alpha-2")],
      globalOutbound = (name = "vrt-egress", entrypoint = "EgressProxy"),
    )),
    (name = "worker-w-alpha-3", worker = (
      compatibilityDate = "2026-10-20",
      limits = (cpuMs = 10, memoryMb = 64),
      modules = [
        (name = "third.js", esModule = embed "bundles/3af9775cb5ac1424b5a68e85256bb77e71d3b58113bc3830dfbc5ae770eecdcf/third.js"),
      ],
      bindings = [
      ],
      tails = [(name = "vrt-tail-w-alpha-3")],
      globalOutbound = (name = "vrt-egress", entrypoint = "EgressProxy"),
    )),
    (name = "vrt-tail-w-alpha-1", worker = (
      compatibilityDate = "2026-09-08",
      modules = [
        (name = "vrt-tail.mjs", esModule = "const MAX = 8192;\x0aconst MAX_RECORDS = 100;\x0afunction cut(s) {\x0a  s = String(s);\x0a  return s.length > MAX ? s.slice(0, MAX) : s;\x0a}\x0afunction fmt(v) {\x0a  if (typeof v === \"string\") return v;\x0a  try {\x0a    const j = JSON.stringify(v);\x0a    return j === undefined ? String(v) : j;\x0a  } catch (_) {\x0a    return \"[unserializable]\";\x0a  }\x0a}\x0afunction fnv1a(s) {\x0a  let h = 0x811c9dc5;\x0a  for (let i = 0; i < s.length; i++) {\x0a    h ^= s.charCodeAt(i);\x0a    h = (h * 0x01000193) >>> 0;\x0a  }\x0a  return h;\x0a}\x0a// Head sampling, per invocation (one `ev` = one invocation): CF keeps\x0a// or drops the WHOLE invocation \xe2\x80\x94 deterministic over fields the\x0a// TraceItem actually carries (scriptName + eventTimestamp +\x0a// durableObjectId \xe2\x80\x94 the pinned workerd exposes no traceId/spanId on\x0a// tail() events; those exist only on TailStream), so a re-delivered\x0a// event gets the same verdict across collectors. Distinct invocations\x0a// sharing a timestamp+script share a fate \xe2\x80\x94 an accepted bias versus\x0a// losing reproducibility.\x0afunction keep(ev, rate) {\x0a  if (rate >= 1) return true;\x0a  if (rate <= 0) return false;\x0a  const id =\x0a    String(ev.scriptName || \"\") +\x0a    \"|\" +\x0a    String(ev.eventTimestamp || \"\") +\x0a    \"|\" +\x0a    String(ev.durableObjectId || \"\");\x0a  const u = fnv1a(id) / 0x100000000;\x0a  return u < rate;\x0a}\x0aexport default {\x0a  async tail(events, env) {\x0a    try {\x0a      let obs = {};\x0a      try {\x0a        obs = JSON.parse(env.OBS || \"{}\");\x0a      } catch (_) {\x0a        obs = {};\x0a      }\x0a      const rate = typeof obs.headSamplingRate === \"number\" ? obs.headSamplingRate : 1;\x0a      const il = obs.invocationLogs !== false;\x0a      const records = [];\x0a      for (const ev of events || []) {\x0a        if (!keep(ev, rate)) continue;\x0a        const base = ev.eventTimestamp || Date.now();\x0a        for (const l of ev.logs || []) {\x0a          if (records.length >= MAX_RECORDS) break;\x0a          const msg = Array.isArray(l.message) ? l.message.map(fmt).join(\" \") : fmt(l.message);\x0a          records.push({ kind: \"log\", level: String(l.level || \"log\"), ts: l.timestamp || base, message: cut(msg) });\x0a        }\x0a        for (const x of ev.exceptions || []) {\x0a          if (records.length >= MAX_RECORDS) break;\x0a          const stack = typeof x.stack === \"string\" ? \"\\n\" + x.stack : \"\";\x0a          records.push({ kind: \"exception\", level: \"error\", ts: x.timestamp || base, message: cut(String(x.name || \"Error\") + \": \" + String(x.message || \"\") + stack) });\x0a        }\x0a        if (il && records.length < MAX_RECORDS) {\x0a          const req = ev.event && ev.event.request;\x0a          const res = ev.event && ev.event.response;\x0a          const m = req ? String(req.method || \"\") + \" \" + String(req.url || \"\") : \"\";\x0a          records.push({\x0a            kind: \"outcome\",\x0a            level: ev.outcome === \"ok\" ? \"info\" : \"error\",\x0a            ts: base,\x0a            message: cut(m),\x0a            outcome: String(ev.outcome || \"\"),\x0a            status: res && typeof res.status === \"number\" ? res.status : null,\x0a          });\x0a        }\x0a      }\x0a      if (records.length === 0) return;\x0a      await env.SINK.fetch(\"http://vrt-logsink/v1/logs/ingest\", {\x0a        method: \"POST\",\x0a        headers: { \"content-type\": \"application/json\", \"x-vrt-log-worker\": env.WORKER_ID, \"x-vrt-log-token\": env.TOKEN },\x0a        body: JSON.stringify({ records }),\x0a      });\x0a    } catch (_) {\x0a      /* collection failure must never surface */\x0a    }\x0a  },\x0a};\x0a"),
      ],
      bindings = [
        (name = "SINK", service = "vrt-logsink"),
        (name = "WORKER_ID", text = "w-alpha-1"),
        (name = "TOKEN", text = "13e93fbcedd72d2d11edc46259a4948897e78c7b1fb56f2f292f2c79fb891231"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "vrt-tail-w-alpha-2", worker = (
      compatibilityDate = "2026-09-08",
      modules = [
        (name = "vrt-tail.mjs", esModule = "const MAX = 8192;\x0aconst MAX_RECORDS = 100;\x0afunction cut(s) {\x0a  s = String(s);\x0a  return s.length > MAX ? s.slice(0, MAX) : s;\x0a}\x0afunction fmt(v) {\x0a  if (typeof v === \"string\") return v;\x0a  try {\x0a    const j = JSON.stringify(v);\x0a    return j === undefined ? String(v) : j;\x0a  } catch (_) {\x0a    return \"[unserializable]\";\x0a  }\x0a}\x0afunction fnv1a(s) {\x0a  let h = 0x811c9dc5;\x0a  for (let i = 0; i < s.length; i++) {\x0a    h ^= s.charCodeAt(i);\x0a    h = (h * 0x01000193) >>> 0;\x0a  }\x0a  return h;\x0a}\x0a// Head sampling, per invocation (one `ev` = one invocation): CF keeps\x0a// or drops the WHOLE invocation \xe2\x80\x94 deterministic over fields the\x0a// TraceItem actually carries (scriptName + eventTimestamp +\x0a// durableObjectId \xe2\x80\x94 the pinned workerd exposes no traceId/spanId on\x0a// tail() events; those exist only on TailStream), so a re-delivered\x0a// event gets the same verdict across collectors. Distinct invocations\x0a// sharing a timestamp+script share a fate \xe2\x80\x94 an accepted bias versus\x0a// losing reproducibility.\x0afunction keep(ev, rate) {\x0a  if (rate >= 1) return true;\x0a  if (rate <= 0) return false;\x0a  const id =\x0a    String(ev.scriptName || \"\") +\x0a    \"|\" +\x0a    String(ev.eventTimestamp || \"\") +\x0a    \"|\" +\x0a    String(ev.durableObjectId || \"\");\x0a  const u = fnv1a(id) / 0x100000000;\x0a  return u < rate;\x0a}\x0aexport default {\x0a  async tail(events, env) {\x0a    try {\x0a      let obs = {};\x0a      try {\x0a        obs = JSON.parse(env.OBS || \"{}\");\x0a      } catch (_) {\x0a        obs = {};\x0a      }\x0a      const rate = typeof obs.headSamplingRate === \"number\" ? obs.headSamplingRate : 1;\x0a      const il = obs.invocationLogs !== false;\x0a      const records = [];\x0a      for (const ev of events || []) {\x0a        if (!keep(ev, rate)) continue;\x0a        const base = ev.eventTimestamp || Date.now();\x0a        for (const l of ev.logs || []) {\x0a          if (records.length >= MAX_RECORDS) break;\x0a          const msg = Array.isArray(l.message) ? l.message.map(fmt).join(\" \") : fmt(l.message);\x0a          records.push({ kind: \"log\", level: String(l.level || \"log\"), ts: l.timestamp || base, message: cut(msg) });\x0a        }\x0a        for (const x of ev.exceptions || []) {\x0a          if (records.length >= MAX_RECORDS) break;\x0a          const stack = typeof x.stack === \"string\" ? \"\\n\" + x.stack : \"\";\x0a          records.push({ kind: \"exception\", level: \"error\", ts: x.timestamp || base, message: cut(String(x.name || \"Error\") + \": \" + String(x.message || \"\") + stack) });\x0a        }\x0a        if (il && records.length < MAX_RECORDS) {\x0a          const req = ev.event && ev.event.request;\x0a          const res = ev.event && ev.event.response;\x0a          const m = req ? String(req.method || \"\") + \" \" + String(req.url || \"\") : \"\";\x0a          records.push({\x0a            kind: \"outcome\",\x0a            level: ev.outcome === \"ok\" ? \"info\" : \"error\",\x0a            ts: base,\x0a            message: cut(m),\x0a            outcome: String(ev.outcome || \"\"),\x0a            status: res && typeof res.status === \"number\" ? res.status : null,\x0a          });\x0a        }\x0a      }\x0a      if (records.length === 0) return;\x0a      await env.SINK.fetch(\"http://vrt-logsink/v1/logs/ingest\", {\x0a        method: \"POST\",\x0a        headers: { \"content-type\": \"application/json\", \"x-vrt-log-worker\": env.WORKER_ID, \"x-vrt-log-token\": env.TOKEN },\x0a        body: JSON.stringify({ records }),\x0a      });\x0a    } catch (_) {\x0a      /* collection failure must never surface */\x0a    }\x0a  },\x0a};\x0a"),
      ],
      bindings = [
        (name = "SINK", service = "vrt-logsink"),
        (name = "WORKER_ID", text = "w-alpha-2"),
        (name = "TOKEN", text = "89663ee46e4104b2a60cf06817fbc2ec2a6b3d36edf423ee6654aff01053190f"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "vrt-tail-w-alpha-3", worker = (
      compatibilityDate = "2026-09-08",
      modules = [
        (name = "vrt-tail.mjs", esModule = "const MAX = 8192;\x0aconst MAX_RECORDS = 100;\x0afunction cut(s) {\x0a  s = String(s);\x0a  return s.length > MAX ? s.slice(0, MAX) : s;\x0a}\x0afunction fmt(v) {\x0a  if (typeof v === \"string\") return v;\x0a  try {\x0a    const j = JSON.stringify(v);\x0a    return j === undefined ? String(v) : j;\x0a  } catch (_) {\x0a    return \"[unserializable]\";\x0a  }\x0a}\x0afunction fnv1a(s) {\x0a  let h = 0x811c9dc5;\x0a  for (let i = 0; i < s.length; i++) {\x0a    h ^= s.charCodeAt(i);\x0a    h = (h * 0x01000193) >>> 0;\x0a  }\x0a  return h;\x0a}\x0a// Head sampling, per invocation (one `ev` = one invocation): CF keeps\x0a// or drops the WHOLE invocation \xe2\x80\x94 deterministic over fields the\x0a// TraceItem actually carries (scriptName + eventTimestamp +\x0a// durableObjectId \xe2\x80\x94 the pinned workerd exposes no traceId/spanId on\x0a// tail() events; those exist only on TailStream), so a re-delivered\x0a// event gets the same verdict across collectors. Distinct invocations\x0a// sharing a timestamp+script share a fate \xe2\x80\x94 an accepted bias versus\x0a// losing reproducibility.\x0afunction keep(ev, rate) {\x0a  if (rate >= 1) return true;\x0a  if (rate <= 0) return false;\x0a  const id =\x0a    String(ev.scriptName || \"\") +\x0a    \"|\" +\x0a    String(ev.eventTimestamp || \"\") +\x0a    \"|\" +\x0a    String(ev.durableObjectId || \"\");\x0a  const u = fnv1a(id) / 0x100000000;\x0a  return u < rate;\x0a}\x0aexport default {\x0a  async tail(events, env) {\x0a    try {\x0a      let obs = {};\x0a      try {\x0a        obs = JSON.parse(env.OBS || \"{}\");\x0a      } catch (_) {\x0a        obs = {};\x0a      }\x0a      const rate = typeof obs.headSamplingRate === \"number\" ? obs.headSamplingRate : 1;\x0a      const il = obs.invocationLogs !== false;\x0a      const records = [];\x0a      for (const ev of events || []) {\x0a        if (!keep(ev, rate)) continue;\x0a        const base = ev.eventTimestamp || Date.now();\x0a        for (const l of ev.logs || []) {\x0a          if (records.length >= MAX_RECORDS) break;\x0a          const msg = Array.isArray(l.message) ? l.message.map(fmt).join(\" \") : fmt(l.message);\x0a          records.push({ kind: \"log\", level: String(l.level || \"log\"), ts: l.timestamp || base, message: cut(msg) });\x0a        }\x0a        for (const x of ev.exceptions || []) {\x0a          if (records.length >= MAX_RECORDS) break;\x0a          const stack = typeof x.stack === \"string\" ? \"\\n\" + x.stack : \"\";\x0a          records.push({ kind: \"exception\", level: \"error\", ts: x.timestamp || base, message: cut(String(x.name || \"Error\") + \": \" + String(x.message || \"\") + stack) });\x0a        }\x0a        if (il && records.length < MAX_RECORDS) {\x0a          const req = ev.event && ev.event.request;\x0a          const res = ev.event && ev.event.response;\x0a          const m = req ? String(req.method || \"\") + \" \" + String(req.url || \"\") : \"\";\x0a          records.push({\x0a            kind: \"outcome\",\x0a            level: ev.outcome === \"ok\" ? \"info\" : \"error\",\x0a            ts: base,\x0a            message: cut(m),\x0a            outcome: String(ev.outcome || \"\"),\x0a            status: res && typeof res.status === \"number\" ? res.status : null,\x0a          });\x0a        }\x0a      }\x0a      if (records.length === 0) return;\x0a      await env.SINK.fetch(\"http://vrt-logsink/v1/logs/ingest\", {\x0a        method: \"POST\",\x0a        headers: { \"content-type\": \"application/json\", \"x-vrt-log-worker\": env.WORKER_ID, \"x-vrt-log-token\": env.TOKEN },\x0a        body: JSON.stringify({ records }),\x0a      });\x0a    } catch (_) {\x0a      /* collection failure must never surface */\x0a    }\x0a  },\x0a};\x0a"),
      ],
      bindings = [
        (name = "SINK", service = "vrt-logsink"),
        (name = "WORKER_ID", text = "w-alpha-3"),
        (name = "TOKEN", text = "9253bf79c685f8483569012318ea4be7e053592bb2183e4d73f8a077b924bb6e"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "vrt-logsink", external = (http = ())),
    (name = "facade-cron-w-alpha-1", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "cron-dispatcher.js", esModule = embed "sys/b30f59801d7bd76dc6245f956cd7b966397ec2dfb65a25a986d57c63a8cd910c"),
      ],
      bindings = [
        (name = "TARGET", service = "worker-w-alpha-1"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "facade-assets", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/facade-assets.js", esModule = embed "sys/022859b9f78333db380453fa953d92d9293f2524c2b5c8da8679ae5d21fb8f08"),
        (name = "facade/props.js", esModule = embed "sys/f89460a2990c7f42849f017de9e45e996b166a436ddc2916c6bb5f2e9f73e92c"),
        (name = "facade/router-wire.js", esModule = embed "sys/e6dda1dd32f372b5ef8d1d092691a7dc9b6fb60e30cd71a9ab713c979e957ee9"),
        (name = "third_party/open-compute/facade/bindings/json-body.js", esModule = embed "sys/d1386702d201992e704b8218a5b37327d2ae27abd056076c1c942376be0e226c"),
        (name = "third_party/open-compute/facade/loader/python-snapshot.js", esModule = embed "sys/97154655cb68c44d37f225a18d0bf995387da9b48f0c27356eb3239553951d55"),
        (name = "third_party/open-compute/facade/loader/shared.js", esModule = embed "sys/ad6ec6871acd01776f6fe414510e0126266f15f95480ce1172c3fa34babfef6e"),
        (name = "third_party/open-compute/facade/loader/snapshot.js", esModule = embed "sys/197e8b61660cae5b8829f707ea091adbc206f9ceec3a2bdb105ecbd65cde0a58"),
      ],
      bindings = [
        (name = "BINDING_BACKEND", service = "binding-backend"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "facade-d1", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/facade-d1.js", esModule = embed "sys/28bd5fe09b3ee764bd52342aa8d5ac6c16f616b88d63de113b66e01a32a37866"),
        (name = "facade/d1-wire.js", esModule = embed "sys/6c946b92333e55e1acc70e860de50191471cf62e5cc926e6ba6ba45127164320"),
        (name = "facade/deadline.js", esModule = embed "sys/e2179b8bad1a28e87f2fa5016361062adbb7a29c53183fcd9574bf6b2adc30ee"),
        (name = "facade/props.js", esModule = embed "sys/f89460a2990c7f42849f017de9e45e996b166a436ddc2916c6bb5f2e9f73e92c"),
        (name = "third_party/open-compute/facade/bindings/json-body.js", esModule = embed "sys/d1386702d201992e704b8218a5b37327d2ae27abd056076c1c942376be0e226c"),
        (name = "third_party/open-compute/facade/loader/python-snapshot.js", esModule = embed "sys/97154655cb68c44d37f225a18d0bf995387da9b48f0c27356eb3239553951d55"),
        (name = "third_party/open-compute/facade/loader/shared.js", esModule = embed "sys/ad6ec6871acd01776f6fe414510e0126266f15f95480ce1172c3fa34babfef6e"),
        (name = "third_party/open-compute/facade/loader/snapshot.js", esModule = embed "sys/197e8b61660cae5b8829f707ea091adbc206f9ceec3a2bdb105ecbd65cde0a58"),
      ],
      bindings = [
        (name = "BINDING_BACKEND", service = "binding-backend"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "facade-email", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/facade-email.js", esModule = embed "sys/4335174f6a26021f7bbe4a4f356c6a58382f354489c4b8bbfc3b0fc742db37b9"),
        (name = "facade/deadline.js", esModule = embed "sys/e2179b8bad1a28e87f2fa5016361062adbb7a29c53183fcd9574bf6b2adc30ee"),
        (name = "facade/email-wire.js", esModule = embed "sys/67f66ebe06fc962eaafabad383c18467d718fe29c97f768a9970bbe9d79e69d2"),
        (name = "facade/props.js", esModule = embed "sys/f89460a2990c7f42849f017de9e45e996b166a436ddc2916c6bb5f2e9f73e92c"),
        (name = "facade/queue-wire.js", esModule = embed "sys/03e39f43d67f734985a7ac5681d6c01f5790b568fddace60c9f0f4c6c0f72290"),
        (name = "third_party/open-compute/facade/bindings/json-body.js", esModule = embed "sys/d1386702d201992e704b8218a5b37327d2ae27abd056076c1c942376be0e226c"),
        (name = "third_party/open-compute/facade/loader/python-snapshot.js", esModule = embed "sys/97154655cb68c44d37f225a18d0bf995387da9b48f0c27356eb3239553951d55"),
        (name = "third_party/open-compute/facade/loader/shared.js", esModule = embed "sys/ad6ec6871acd01776f6fe414510e0126266f15f95480ce1172c3fa34babfef6e"),
        (name = "third_party/open-compute/facade/loader/snapshot.js", esModule = embed "sys/197e8b61660cae5b8829f707ea091adbc206f9ceec3a2bdb105ecbd65cde0a58"),
      ],
      bindings = [
        (name = "BINDING_BACKEND", service = "binding-backend"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "facade-k2", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/facade-k2.js", esModule = embed "sys/750307159dcd7355ec31898877e4ae955a8a5302cb51a8a9a058f0bf0ab4f33c"),
        (name = "facade/deadline.js", esModule = embed "sys/e2179b8bad1a28e87f2fa5016361062adbb7a29c53183fcd9574bf6b2adc30ee"),
        (name = "facade/k2-wire.js", esModule = embed "sys/cfb93e89e9ef7632aa6e714389e7a01d17e3622bbb1d6db85a059ab13d1f8df8"),
        (name = "facade/props.js", esModule = embed "sys/f89460a2990c7f42849f017de9e45e996b166a436ddc2916c6bb5f2e9f73e92c"),
        (name = "facade/queue-wire.js", esModule = embed "sys/03e39f43d67f734985a7ac5681d6c01f5790b568fddace60c9f0f4c6c0f72290"),
        (name = "third_party/open-compute/facade/bindings/json-body.js", esModule = embed "sys/d1386702d201992e704b8218a5b37327d2ae27abd056076c1c942376be0e226c"),
        (name = "third_party/open-compute/facade/loader/python-snapshot.js", esModule = embed "sys/97154655cb68c44d37f225a18d0bf995387da9b48f0c27356eb3239553951d55"),
        (name = "third_party/open-compute/facade/loader/shared.js", esModule = embed "sys/ad6ec6871acd01776f6fe414510e0126266f15f95480ce1172c3fa34babfef6e"),
        (name = "third_party/open-compute/facade/loader/snapshot.js", esModule = embed "sys/197e8b61660cae5b8829f707ea091adbc206f9ceec3a2bdb105ecbd65cde0a58"),
      ],
      bindings = [
        (name = "BINDING_BACKEND", service = "binding-backend"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "facade-kv", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/facade-kv.js", esModule = embed "sys/8d67b68ac0ad90a075cda7f4b1c49f7bf10254a8f1bca4e8af8ff3acf98893e4"),
        (name = "facade/props.js", esModule = embed "sys/f89460a2990c7f42849f017de9e45e996b166a436ddc2916c6bb5f2e9f73e92c"),
        (name = "third_party/open-compute/facade/bindings/json-body.js", esModule = embed "sys/d1386702d201992e704b8218a5b37327d2ae27abd056076c1c942376be0e226c"),
        (name = "third_party/open-compute/facade/kv/native-adapter.js", esModule = embed "sys/312c11e42c98ee866c2582d8e4a5c6762750c481a6da10112d35d46bfdf417c2"),
        (name = "third_party/open-compute/facade/kv/transport.js", esModule = embed "sys/030cb1245c6c4ad6de8cf29b8e20a7e0bff6fd807cbee71af78a09baeb7a3225"),
        (name = "third_party/open-compute/facade/loader/python-snapshot.js", esModule = embed "sys/97154655cb68c44d37f225a18d0bf995387da9b48f0c27356eb3239553951d55"),
        (name = "third_party/open-compute/facade/loader/shared.js", esModule = embed "sys/ad6ec6871acd01776f6fe414510e0126266f15f95480ce1172c3fa34babfef6e"),
        (name = "third_party/open-compute/facade/loader/snapshot.js", esModule = embed "sys/197e8b61660cae5b8829f707ea091adbc206f9ceec3a2bdb105ecbd65cde0a58"),
      ],
      bindings = [
        (name = "BINDING_BACKEND", service = "binding-backend"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "facade-pg", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/facade-pg.js", esModule = embed "sys/8633197cc0dad407103e5633ba1bc9a7b2a12998c540b2ad77e22dd275739b83"),
        (name = "facade/deadline.js", esModule = embed "sys/e2179b8bad1a28e87f2fa5016361062adbb7a29c53183fcd9574bf6b2adc30ee"),
        (name = "facade/props.js", esModule = embed "sys/f89460a2990c7f42849f017de9e45e996b166a436ddc2916c6bb5f2e9f73e92c"),
        (name = "third_party/open-compute/facade/bindings/json-body.js", esModule = embed "sys/d1386702d201992e704b8218a5b37327d2ae27abd056076c1c942376be0e226c"),
        (name = "third_party/open-compute/facade/loader/python-snapshot.js", esModule = embed "sys/97154655cb68c44d37f225a18d0bf995387da9b48f0c27356eb3239553951d55"),
        (name = "third_party/open-compute/facade/loader/shared.js", esModule = embed "sys/ad6ec6871acd01776f6fe414510e0126266f15f95480ce1172c3fa34babfef6e"),
        (name = "third_party/open-compute/facade/loader/snapshot.js", esModule = embed "sys/197e8b61660cae5b8829f707ea091adbc206f9ceec3a2bdb105ecbd65cde0a58"),
      ],
      bindings = [
        (name = "BINDING_BACKEND", service = "binding-backend"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "facade-queue", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/facade-queue.js", esModule = embed "sys/f1f5e707bb17e4a2fe208294a869735a08f36fc0539a827cfd3bf2e24393ac6d"),
        (name = "facade/deadline.js", esModule = embed "sys/e2179b8bad1a28e87f2fa5016361062adbb7a29c53183fcd9574bf6b2adc30ee"),
        (name = "facade/props.js", esModule = embed "sys/f89460a2990c7f42849f017de9e45e996b166a436ddc2916c6bb5f2e9f73e92c"),
        (name = "facade/queue-wire.js", esModule = embed "sys/03e39f43d67f734985a7ac5681d6c01f5790b568fddace60c9f0f4c6c0f72290"),
        (name = "third_party/open-compute/facade/bindings/json-body.js", esModule = embed "sys/d1386702d201992e704b8218a5b37327d2ae27abd056076c1c942376be0e226c"),
        (name = "third_party/open-compute/facade/loader/python-snapshot.js", esModule = embed "sys/97154655cb68c44d37f225a18d0bf995387da9b48f0c27356eb3239553951d55"),
        (name = "third_party/open-compute/facade/loader/shared.js", esModule = embed "sys/ad6ec6871acd01776f6fe414510e0126266f15f95480ce1172c3fa34babfef6e"),
        (name = "third_party/open-compute/facade/loader/snapshot.js", esModule = embed "sys/197e8b61660cae5b8829f707ea091adbc206f9ceec3a2bdb105ecbd65cde0a58"),
      ],
      bindings = [
        (name = "BINDING_BACKEND", service = "binding-backend"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "facade-r2", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/facade-r2.js", esModule = embed "sys/679ee5883d8e5fdcd5b7256184a42d9a8c7b0424494ae0888c27c8c9bb800035"),
        (name = "facade/props.js", esModule = embed "sys/f89460a2990c7f42849f017de9e45e996b166a436ddc2916c6bb5f2e9f73e92c"),
        (name = "third_party/open-compute/facade/bindings/json-body.js", esModule = embed "sys/d1386702d201992e704b8218a5b37327d2ae27abd056076c1c942376be0e226c"),
        (name = "third_party/open-compute/facade/bindings/private-transport.js", esModule = embed "sys/70d4f22624409df080a43825bc7fdebe429416fce17f66d8a75e8e51efd2afd6"),
        (name = "third_party/open-compute/facade/loader/python-snapshot.js", esModule = embed "sys/97154655cb68c44d37f225a18d0bf995387da9b48f0c27356eb3239553951d55"),
        (name = "third_party/open-compute/facade/loader/shared.js", esModule = embed "sys/ad6ec6871acd01776f6fe414510e0126266f15f95480ce1172c3fa34babfef6e"),
        (name = "third_party/open-compute/facade/loader/snapshot.js", esModule = embed "sys/197e8b61660cae5b8829f707ea091adbc206f9ceec3a2bdb105ecbd65cde0a58"),
        (name = "third_party/open-compute/facade/r2/native-adapter.js", esModule = embed "sys/0c2186678c5893c56a127a6154e7c0728bc68e4f436c6cb8a8e6a721607e827b"),
        (name = "third_party/open-compute/facade/r2/transport.js", esModule = embed "sys/c1299e7a51c29c821481b89bcc336eec9c1c78b42cd0ebeef974a3df31502c5d"),
        (name = "third_party/open-compute/facade/r2/validation.js", esModule = embed "sys/ca8ee1404354248a5ccb82c988508ae57ce8c00f2ece1822dc0302edc31fb546"),
      ],
      bindings = [
        (name = "BINDING_BACKEND", service = "binding-backend"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "facade-ratelimit", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/facade-ratelimit.js", esModule = embed "sys/f461934cce694d59dc08783881e04a1b9f61e52a1c2a5db50999294849f7e244"),
        (name = "facade/ratelimit-core.js", esModule = embed "sys/b320e26f1354a5f92d12abee57283c43693274e4df618f92e8823f7f2812cd7f"),
        (name = "third_party/open-compute/facade/bindings/json-body.js", esModule = embed "sys/d1386702d201992e704b8218a5b37327d2ae27abd056076c1c942376be0e226c"),
        (name = "third_party/open-compute/facade/loader/python-snapshot.js", esModule = embed "sys/97154655cb68c44d37f225a18d0bf995387da9b48f0c27356eb3239553951d55"),
        (name = "third_party/open-compute/facade/loader/shared.js", esModule = embed "sys/ad6ec6871acd01776f6fe414510e0126266f15f95480ce1172c3fa34babfef6e"),
        (name = "third_party/open-compute/facade/loader/snapshot.js", esModule = embed "sys/197e8b61660cae5b8829f707ea091adbc206f9ceec3a2bdb105ecbd65cde0a58"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "vrt-egress", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/egress/egress.js", esModule = embed "sys/fbea734df54b11e66b2157123c5fb7f36d4f624d6cf2ea4f0691df548bc4c3d1"),
        (name = "facade/egress/ip.js", esModule = embed "sys/6575c6e66245aebea369d5e97ae7ffc6783652044ed2adc2be462c9df3e7b97b"),
        (name = "facade/egress/policy.js", esModule = embed "sys/e6dc4ee5d55668354c4a180e570142fb0a0df195e6228baa038c4d7a96d72fef"),
        (name = "facade/egress/shared.js", esModule = embed "sys/353d7aa02d71f0745c83d132dd884378b3ff2fc2ec2215ebdd8e08d5e4ab89dc"),
      ],
      bindings = [
        (name = "VRT_EGRESS_POLICY", text = "{\"allow\":[{\"host\":\"api.vibeploy.internal\",\"port\":8443}],\"allowedPorts\":[80,443],\"denyNames\":[\"*.tenant.example\",\"blocked.example\"],\"internalTcpAllow\":[],\"internalTcpTargets\":[],\"requestTimeoutMs\":30000,\"tcpAllowedPorts\":[43],\"tcpConnectTimeoutMs\":10000,\"tcpIdleTimeoutMs\":60000,\"tcpMaxConcurrent\":64}"),
      ],
      globalOutbound = "outbound-network",
    )),
    (name = "vrt-no-network", worker = (
      compatibilityDate = "2026-09-08",
      compatibilityFlags = ["experimental", "service_binding_extra_handlers"],
      modules = [
        (name = "facade/egress/deny.js", esModule = embed "sys/a66f0364c5bfeaf8a039a25f2a6b9c4c56939886b592ddbfd332ca01331495af"),
        (name = "facade/egress/shared.js", esModule = embed "sys/353d7aa02d71f0745c83d132dd884378b3ff2fc2ec2215ebdd8e08d5e4ab89dc"),
      ],
      globalOutbound = (name = "vrt-no-network", entrypoint = "DenyAll"),
    )),
    (name = "binding-backend", external = (http = ())),
    (name = "outbound-network", network = (
      allow = ["public"],
      deny = ["private", "local", "unix", "unix-abstract"],
      tlsOptions = (trustBrowserCas = true),
    )),
    (name = "internal-network", network = (
      allow = ["local"],
      deny = ["unix", "unix-abstract"],
    )),
  ],
  sockets = [
    (name = "sock-w-alpha-1", address = "unix:sock/w-alpha-1.sock", http = (forwardedProtoHeader = "x-forwarded-proto", cfBlobHeader = "x-vrt-cf-blob"), service = "worker-w-alpha-1"),
    (name = "sock-cron-w-alpha-1", address = "unix:sock/w-alpha-1-cron.sock", http = (), service = "facade-cron-w-alpha-1"),
    (name = "sock-w-alpha-2", address = "unix:sock/w-alpha-2.sock", http = (forwardedProtoHeader = "x-forwarded-proto", cfBlobHeader = "x-vrt-cf-blob"), service = "worker-w-alpha-2"),
    (name = "sock-w-alpha-3", address = "unix:sock/w-alpha-3.sock", http = (forwardedProtoHeader = "x-forwarded-proto", cfBlobHeader = "x-vrt-cf-blob"), service = "worker-w-alpha-3"),
  ],
);
