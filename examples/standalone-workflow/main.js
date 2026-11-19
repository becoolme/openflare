// W1 e2e — `OrderFlow` exports a WorkflowEntrypoint subclass in the
// SAME worker (W1 same-worker constraint). Step "boom" throws on its
// first attempt to exercise engine retry; `step.sleep` gives a ~2s
// window the e2e uses to restart workerd/vrt-host and prove the
// persisted engine state resumes the instance to completion.
import { WorkflowEntrypoint } from "cloudflare:workers";

let boomAttempts = 0;

export class OrderFlow extends WorkflowEntrypoint {
  async run(event, step) {
    const a = await step.do("first", async () => "a");
    const b = await step.do(
      "boom",
      { retries: { limit: 3, delay: "50 milliseconds", backoff: "constant" } },
      async () => {
        boomAttempts += 1;
        if (boomAttempts === 1) throw new Error("boom-once");
        return "b";
      },
    );
    await step.sleep("pause", "2 seconds");
    return `${a}-${b}-done`;
  }
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ping") return new Response("wf-ok");
    if (url.pathname === "/create") {
      const inst = await env.WF.create();
      return Response.json({ id: inst.id });
    }
    if (url.pathname === "/status") {
      const inst = await env.WF.get(url.searchParams.get("id"));
      return Response.json(await inst.status());
    }
    return new Response("nf", { status: 404 });
  }
};
