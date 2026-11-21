// Persisted-alarm probe — a self re-arming alarm chain. The case starts
// the chain, marks the pre-restart observations in durable storage,
// restarts the runtime, and then asserts that the next alarm fires in
// the NEW process while the storage written before the restart is still
// there.
//
// Module state lives for one workerd process, so `bootId` (created on the
// first handler call) identifies the process that served a request. Every
// volatile value (timestamps, ids) is normalized into a boolean before it
// leaves the worker — the case expectations stay static.
import { DurableObject } from "cloudflare:workers";

const CHAIN_MS = 200;
// Generated lazily in a handler: workerd forbids random values in the
// global scope. The first request in a process fixes the id, and it
// stays the same for every later request served by that process.
let bootId = null;
function boot() {
  if (bootId === null) bootId = crypto.randomUUID();
  return bootId;
}

export class AlarmChain extends DurableObject {
  async fetch(req) {
    const url = new URL(req.url);
    const s = this.ctx.storage;
    const j = (v) => Response.json(v);
    switch (url.pathname) {
      case "/reset": {
        // Storage outlives redeploys; clear what an earlier run left so
        // the counter and alarm record start from zero.
        await s.delete(["value", "fired", "chain", "firedAtMark", "markedBootId", "lastFiredBootId"]);
        await s.deleteAlarm();
        return j({ ok: true });
      }
      case "/value/inc": {
        const value = ((await s.get("value")) ?? 0) + 1;
        await s.put("value", value);
        return j({ value });
      }
      case "/chain/start": {
        await s.put("chain", true);
        await s.setAlarm(Date.now() + CHAIN_MS);
        return j({ chain: true });
      }
      case "/chain/stop": {
        // Disarm before reading back; at most one in-flight alarm can
        // still land, and it will see chain=false and not re-arm.
        await s.put("chain", false);
        await s.deleteAlarm();
        return j({ chain: await s.get("chain") });
      }
      case "/mark": {
        // Freeze the pre-restart observations in durable storage. The
        // post-restart assertions compare against THESE values, not
        // against anything the runner remembered between steps.
        await s.put({
          firedAtMark: (await s.get("fired")) ?? 0,
          markedBootId: boot(),
        });
        return j({ marked: true });
      }
      case "/state": {
        const [value, fired, chain, firedAtMark, markedBootId, lastFiredBootId, alarm] =
          await Promise.all([
            s.get("value"),
            s.get("fired"),
            s.get("chain"),
            s.get("firedAtMark"),
            s.get("markedBootId"),
            s.get("lastFiredBootId"),
            s.getAlarm(),
          ]);
        const firedCount = fired ?? 0;
        const id = boot();
        const restarted = markedBootId != null && markedBootId !== id;
        return j({
          bootId: id,
          value: value ?? 0,
          fired: firedCount,
          firedAtMark: firedAtMark ?? 0,
          // The alarm() record written before the restart is still here.
          firedRecordPersisted: firedCount > 0,
          // setAlarm's durable record is still armed.
          alarmArmed: alarm !== null,
          chain: chain === true,
          // The last alarm() ran in this process (not the previous one).
          firedInCurrentProcess: lastFiredBootId === id,
          // Exactly what the case asserts: a different process, an alarm
          // that fired here after this mark, and the chain still armed.
          restartedSinceMark: restarted,
          chainAliveAfterRestart:
            restarted && chain === true && firedCount > (firedAtMark ?? 0) &&
            lastFiredBootId === id,
        });
      }
      default:
        return j({ ok: true });
    }
  }

  async alarm() {
    const s = this.ctx.storage;
    // Re-arm before recording: if the process is stopped inside the
    // handler, the chain is already armed again for the next process.
    if ((await s.get("chain")) === true) await s.setAlarm(Date.now() + CHAIN_MS);
    await s.put({
      fired: ((await s.get("fired")) ?? 0) + 1,
      lastFiredBootId: boot(),
    });
  }
}

export default {
  async fetch(req, env) {
    const stub = env.ALARMS.get(env.ALARMS.idFromName("alarm-chain"));
    return stub.fetch(req);
  },
};
