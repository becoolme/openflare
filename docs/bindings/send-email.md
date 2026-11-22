# E-mail sending (`send_email`)

`env.<binding>` is the Cloudflare Email Sending binding:
`send(message)` relays one message through the provider's configured
upstream adapter. It resolves `{messageId}` on acceptance and throws
on every failure — local argument misuse throws a `TypeError` (no
`.code`), service rejections throw an `Error` carrying a `.code`
(`E_*` or `BACKEND_UNAVAILABLE`), matching the CF contract exactly.

**Status: Partial** — the full send surface works (recipients,
`replyTo`, HTML+text, attachments, custom headers, CF send-bindings
restrictions). There is no inbound Email Routing/Email Workers side —
this binding only *sends*.

## Wrangler configuration

```toml
[[send_email]]
name = "EMAIL"
# Exactly one of these two may appear (mutually exclusive):
# destination_address           — pin every send to this recipient (dev)
# allowed_destination_addresses = ["user@example.com"]
# allowed_sender_addresses      = ["hello@example.com"]
```

Native `vrt.toml` equivalent — the restriction set is camelCase and
signed into desired state:

```toml
[bindings.send_email.EMAIL]
allowedDestinationAddresses = ["user@example.com"]
# allowedSenderAddresses    = ["hello@example.com"]
# destinationAddress        = "dev@example.com"   # XOR with the allowlist
```

`name` is required; `destination_address`,
`allowed_destination_addresses` and `allowed_sender_addresses` are
optional (absent → unrestricted by the binding, upstream policy
still applies). All addresses are validated as email addresses at
normalize; `destination_address` + `allowed_destination_addresses`
together is rejected.

## Using it

```js
export default {
  async fetch(req, env) {
    const r = await env.EMAIL.send({
      from: { email: "hello@example.com", name: "Hello" },
      to: ["a@example.com", { email: "b@example.com", name: "B" }],
      cc: "c@example.com",
      subject: "hi",
      text: "plain body",
      html: "<b>html body</b>",
      replyTo: "reply@example.com",
      attachments: [{
        content: "aGVsbG8=",           // base64
        filename: "a.txt",
        type: "text/plain",
        disposition: "attachment",     // or "inline" + contentId
      }],
      headers: { "x-tag": "t1" },
    });
    return Response.json(r); // {messageId}
  },
};
```

## Behaviour notes

- `send()` resolves `{messageId}`; on failure it throws — an `Error`
  carrying `.code` for service rejections, a `TypeError` (no `.code`)
  for local argument misuse (missing `from`/`subject`, no recipient,
  non-string fields, malformed attachments/headers).
- `error.retryable` on the failure envelope tells the caller whether a
  retry can help; the platform performs no automatic redelivery —
  retry policy belongs to the caller or the upstream adapter.
- `destination_address` pins every send to that single recipient
  (dev/testing). `allowed_destination_addresses` and
  `allowed_sender_addresses` are allowlists enforced provider-side on
  every send — a `to`/`cc`/`bcc` address outside the list →
  `E_RECIPIENT_NOT_ALLOWED`; a `from` outside the sender list →
  `E_SENDER_NOT_VERIFIED`.

## Limits

| Limit | Value |
|---|---|
| Recipients per send (to+cc+bcc) | 50 |
| Attachments per send | 32 |
| Total message size (decoded) | 25 MiB |
| Custom headers | ≤ 20 entries, ≤ 16 KiB total |
| Header name / value | 100 B / 2048 B |

## Where it runs

The binding renders as a workerd `wrapped` binding over
`cloudflare-internal:send-email` whose inner `fetcher` service binding
points at the `facade-email` system worker
(`vrt-config/src/capnp.rs`, `facade/facade-email.ts`). Signed props
carry `bindingId` = `{workerId}~{name}` and the write permission; the
facade is deny-routed and accepts only `POST /send`.

The facade drains the body bounded, strips user-controllable
`x-vrt-*` / `x-open-compute-*` headers, mints the provider request id,
and forwards to `/internal/bindings/v1/email/<bindingId>/send` on the
provider socket — the same `BINDING_BACKEND` channel as d1/queue/k2.
Replies pass through verbatim: `{success:true, messageId}` or
`{success:false, error:{code,message,retryable}}`.

## Backends

`vrt-provider`'s `email` route validates the message, applies the
signed restriction set, then hands off to the upstream adapter
selected by `[email] backend` in provider config:

| `backend` | Behaviour |
|---|---|
| `log` | Dev adapter — logs the send, returns a local id, never delivers |
| `resend` | `POST {endpoint}/emails` (default `https://api.resend.com`); key from `apiKeyEnv` (default `VRT_EMAIL_API_KEY`) |
| `cf` | Cloudflare Email API send URL; requires `endpoint` + `apiKeyEnv` token |
| `smtp` | SMTP relay — `host` required, `tls` = `plain`\|`starttls`\|`implicit` (ports 587/465), `usernameEnv`+`passwordEnv` together, `helo` |

Secrets are always env-var **names**, never inline values. Absent
`[email]` config → the route fails closed (`BACKEND_UNAVAILABLE`,
503) — an authorized send is never silently dropped.

## Unsupported keys and error shape

- `[[send_email]]` accepts `name`, `destination_address`,
  `allowed_destination_addresses`, `allowed_sender_addresses` only —
  anything else → `VRT_CONFIG_UNSUPPORTED_FIELD` (`send_email[i].<key>`).
- Missing/invalid `name`, non-email addresses, or the
  pin+allowlist combination → `VRT_CONFIG_INVALID`.
- Runtime codes: `E_VALIDATION_ERROR`, `E_TOO_MANY_RECIPIENTS`,
  `E_TOO_MANY_ATTACHMENTS`, `E_CONTENT_TOO_LARGE`,
  `E_HEADER_NAME_INVALID`/`E_HEADER_VALUE_INVALID`/
  `E_HEADER_USE_API_FIELD`, `E_HEADERS_TOO_MANY`/`E_HEADERS_TOO_LARGE`,
  `E_SENDER_NOT_VERIFIED`, `E_RECIPIENT_NOT_ALLOWED`,
  `E_RATE_LIMIT_EXCEEDED`, `E_DELIVERY_FAILED`,
  `E_INTERNAL_SERVER_ERROR`, `BACKEND_UNAVAILABLE`. Adapter failures
  surface as `E_DELIVERY_FAILED`/`E_RATE_LIMIT_EXCEEDED` with
  `retryable` mapped from the upstream status.

## Examples and tests

- `facade/facade-email.ts` + `facade/email-wire.ts` — route allowlist,
  header strip + request-id mint, envelope pass-through.
- `vrt-provider/src/email.rs` — message validation, restriction
  enforcement, the `log`/`resend`/`cf`/`smtp` adapters.
- `vrt-provider/tests/email_send.rs` — provider suite: restrictions,
  limit edges, adapter outcomes and retryable mapping (14 tests).
- `vrt-local/src/project_cfg.rs` — `[[send_email]]` normalize (key
  allowlist, mutual exclusion, address validation).
- `vrt-host/tests/bindings_workerd.rs` —
  `send_email_binding_traverses_shim_end_to_end`: real-workerd smoke
  through the `cloudflare-internal:send-email` shim
  (`VRT_WORKERD_BIN` gated).
- `snapshot-protocol/src/compute_desired.rs` — `SendEmailBinding`
  desired model.

## Known differences from Cloudflare

- **Sending only.** Inbound Email Routing / Email Workers
  (`event.reply()`, catch-all routes) are not implemented; this is
  the outbound `send_email` binding only.
- **Delivery is adapter-owned.** Cloudflare's internal queueing, DKIM
  signing and deliverability pipeline live in whatever upstream the
  operator configures (`resend`/`cf`/`smtp`) — the platform enforces
  the CF wire and restriction contract, not delivery policy.
- **`messageId` is adapter-derived**, not a Cloudflare message id.
