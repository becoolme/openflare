// DO alarm probe — arm via storage.setAlarm; workerd's AlarmScheduler
// invokes alarm() which bumps a durable counter we can poll.
import { DurableObject } from "cloudflare:workers";

export class AlarmBox extends DurableObject {
  async fetch(req) {
    const url = new URL(req.url);
    const s = this.ctx.storage;
    const j = (v) => Response.json(v);
    if (url.pathname === "/arm") {
      const inMs = Number(url.searchParams.get("ms") ?? "300");
      await s.setAlarm(Date.now() + inMs);
      return j({ armed: true });
    }
    if (url.pathname === "/armed-at") {
      const at = await s.getAlarm();
      return j({ scheduled: at, isNumber: typeof at === "number" });
    }
    if (url.pathname === "/disarm") {
      await s.deleteAlarm();
      return j({ after: await s.getAlarm() });
    }
    if (url.pathname === "/fired") {
      return j({ fired: (await s.get("alarmFired")) ?? 0 });
    }
    return j({ ok: true });
  }

  async alarm() {
    const n = ((await this.ctx.storage.get("alarmFired")) ?? 0) + 1;
    await this.ctx.storage.put("alarmFired", n);
  }
}

export default {
  async fetch(req, env) {
    const stub = env.ALARMS.get(env.ALARMS.idFromName("box"));
    return stub.fetch(req);
  },
};
