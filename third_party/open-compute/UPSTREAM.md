# open-compute vendored subset — provenance

- 上游仓库：<https://github.com/elliothux/open-compute>
- 固定 commit：`8352f0cf9a43bcea1a443be3e6088706b1ea31b7`
- 许可证：Apache-2.0（`LICENSE` 原样复制；上游根目录无 `NOTICE` 文件）
- 只读 checkout：`~/.cache/vrt-upstream/open-compute`（本次 vendor 的来源）
- 设计依据：vrt phase0 vendor 清单（内部文档，未随仓库发布）
  （T4 只取「KV、R2 transport 及其直接依赖的最小 JS 子集」；
  config.capnp / host-extension.capnp / reference / conformance 等其余条目
  留给后续任务）

## 复制范围

保持上游 `packages/runtime/src/` 相对路径，落在 `facade/` 下。运行时闭包：

- `kv/transport.ts`（`KVNamespace`）→ `kv/native-adapter.ts`、
  `loader/shared.ts`、`cloudflare:workers`
- `r2/transport.ts`（`makeR2TransportBase`）→ `r2/native-adapter.ts`、
  `r2/validation.ts`、`bindings/private-transport.ts`、`cloudflare:workers`
- `kv/native-adapter.ts` → `bindings/json-body.ts`
- `loader/shared.ts` → `loader/python-snapshot.ts`、`loader/snapshot.ts`
  （`bindingError`/`INTERNAL_HEADERS` 的携带闭包，facade 不调用其
  snapshot/Python 函数，仅 import 图要求模块存在）
- `bindings/private-transport.ts` → `loader/shared.ts`
- `bindings/json-body.ts` → 无依赖

`.d.ts` 为类型契约参考（`import type` 在运行时被擦除，不进 worker 模块
清单）；它们自身再引用的传递类型文件（如 `queues/protocol.d.ts`）有意
未复制，不影响运行。

| 文件 | sha256 |
| --- | --- |
| facade/bindings/json-body.ts | d825baf1890f95f520fa6b548b7f1487021e62e287e91670cf050b8bfdd862cd |
| facade/bindings/private-transport.ts | d3c81cd926da20e2e8529c72617a95dae8591102293a0d885773e8a6a52a2bf9 |
| facade/bindings/protocol.d.ts | 1d824d9b8f72f50d90f418c64812cfd2ea2632a625d71b83025af21dc3159f39 |
| facade/durable-objects/protocol.d.ts | 5668f0d87258be514a772742e58723479cbf5f5a7d5fa55ef031930fbdc2c2ec |
| facade/kv/native-adapter.ts | 7118df2fa817b6dfa7b6210dd04e9056af62753cc6d17f03b2389c2148117c42 |
| facade/kv/transport.ts | 7c8c6e7eb6d50b17bf70ee1e471d6a24005fe5990389a1349eea2c96b23ba1ee |
| facade/loader/protocol.d.ts | afc7d895ff354907348b6f09d2739a8bf7422df038979ed8b0c8de4f98b11222 |
| facade/loader/python-snapshot.ts | a69bc46cd34708e1b57848f60fde07d51b2a331f5c9ed5e5c23067a57bd8d292 |
| facade/loader/shared.ts | 0bcc1aef30a74df9d90237e4d2ef8276ab346bb0645207575746d9a0dd9feca5 |
| facade/loader/snapshot.ts | 61cb57e3b0295e97d078553a321a5c3c6c88aeda66cdfb228de62fcb9cdb885b |
| facade/r2/native-adapter.ts | c76662432f27af9df7b333e8399737590485a547828eec3469759043b6103eae |
| facade/r2/protocol.d.ts | 6ba944f8a412ecb0862cfc28f09aac58a3cacbd3366d32cfb4adf55f53303411 |
| facade/r2/transport.ts | 57807485b32d145439c8ff563a67c708d44833c7d686c241851d0b82ba26186a |
| facade/r2/validation.ts | 9ae80f5607a29493d0f776d2d243bad2b127569e3a74908d055e39d29a189d84 |

`LICENSE`：sha256 见 `shasum -a 256 third_party/open-compute/LICENSE`。

## 本地修改清单

**空。** vendored 文件与上游逐字节一致；所有平台适配（props→身份头、
包装 `BINDING_BACKEND`、assets/pg transport、装配清单）都在
`facade/` 自写层，不改动本目录内容。

## 同步步骤

1. 在 `~/.cache/vrt-upstream/open-compute` checkout 新 commit（只读）。
2. 按上表逐个 `cp` 覆盖对应文件；新增/删除依赖时先重算 import 闭包
   （`facade/tools/gen-manifest.mjs` 会因
   解析失败暴露缺失）。
3. 重算上表 sha256（`shasum -a 256 facade/**/*.ts`），更新本文件与
   commit 号；如有本地补丁（当前无）逐条重列。
4. 重新生成 `facade/{dist/,manifest.json}`
   （`cd facade && node tools/gen-manifest.mjs`），
   跑 `cd facade && node --test`、
   `node ../contract/gen-facade-requests.mjs`（如有线格式变化）与
   `cargo test --workspace --offline`。
