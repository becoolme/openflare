// P3c-1 e2e — one DO class exercising the required surface:
// storage.get/put, storage.sql.exec, transaction,
// blockConcurrencyWhile, and the alarm() handler (fired by workerd's
// built-in AlarmScheduler; armed alarms persist in metadata.sqlite
// across restarts).
export class Counter {
  constructor(state) {
    this.state = state
    this.ready = state.blockConcurrencyWhile(async () => {
      await state.storage.put('initialized', true)
    })
  }
  async fetch(req) {
    await this.ready
    const url = new URL(req.url)
    const s = this.state.storage
    if (url.pathname === '/inc') {
      const n = ((await s.get('n')) ?? 0) + 1
      await s.put('n', n)
      s.sql.exec('CREATE TABLE IF NOT EXISTS t(k INTEGER)')
      s.sql.exec('INSERT INTO t VALUES (?)', n)
      return Response.json({ n })
    }
    if (url.pathname === '/get') {
      return Response.json({ n: (await s.get('n')) ?? 0 })
    }
    if (url.pathname === '/alarm') {
      const ms = parseInt(url.searchParams.get('ms') ?? '800', 10)
      await s.setAlarm(Date.now() + ms)
      return Response.json({ ok: true })
    }
    if (url.pathname === '/alarm-fired') {
      return Response.json({ fired: (await s.get('alarmFired')) ?? 0 })
    }
    return Response.json({ ok: true })
  }
  async alarm() {
    const n = ((await this.state.storage.get('alarmFired')) ?? 0) + 1
    await this.state.storage.put('alarmFired', n)
  }
}
export default {
  async fetch(req, env) {
    const stub = env.COUNTER.get(env.COUNTER.idFromName('one'))
    return stub.fetch(req)
  }
}
