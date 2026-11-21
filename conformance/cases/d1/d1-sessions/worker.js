// D1 Sessions probe — env.DB is the real workerd D1Database (wrapped
// binding cloudflare-internal:d1-api → facade-d1 → provider → vrt-sql),
// so `withSession()` / `getBookmark()` are workerd's own client code and
// the bookmark token is the one our data plane minted.
const DDL = "CREATE TABLE IF NOT EXISTS sess (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)";

const isBookmark = (b) => typeof b === "string" && b.length > 0 && b.trim() === b;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const j = (v) => Response.json(v);
    try {
      if (url.pathname === "/reset") {
        // D1 outlives a redeploy; a rerun must start from an empty table.
        await env.DB.exec("DROP TABLE IF EXISTS sess");
        await env.DB.exec(DDL);
        return j({ ok: true });
      }
      if (url.pathname === "/sessions") {
        // 1. Default session ("first-unconstrained"): the documented
        //    getBookmark() contract is null until a query has run.
        const def = env.DB.withSession();
        const defaultBefore = def.getBookmark();
        const ins = await def.prepare("INSERT INTO sess (v) VALUES (?)").bind("one").run();
        const defaultAfter = def.getBookmark();
        const sameSessionRead = await def.prepare("SELECT v FROM sess ORDER BY id").all();

        // 2. A bookmark resumes a new session: it reports the bookmark it
        //    was given until a query returns a newer one.
        const resumed = env.DB.withSession(defaultAfter);
        const resumedBefore = resumed.getBookmark();
        const resumedRead = await resumed.prepare("SELECT v FROM sess ORDER BY id").all();

        // 3. A later write advances the bookmark (bookmarks are
        //    lexicographically ordered oldest→newest), and a session
        //    resumed from the OLD bookmark still sees the new row.
        const writer = env.DB.withSession(defaultAfter);
        await writer.prepare("INSERT INTO sess (v) VALUES (?)").bind("two").run();
        const afterWrite = writer.getBookmark();

        const behind = env.DB.withSession(defaultAfter);
        const behindRow = await behind.prepare("SELECT COUNT(*) AS n FROM sess").first();
        const behindBookmark = behind.getBookmark();

        // 4. The session surface documented for D1DatabaseSession:
        //    batch() works and also advances the bookmark.
        const batched = env.DB.withSession();
        const batchRes = await batched.batch([
          batched.prepare("INSERT INTO sess (v) VALUES (?)").bind("three"),
          batched.prepare("SELECT COUNT(*) AS n FROM sess"),
        ]);
        const batchBookmark = batched.getBookmark();

        // 5. Both documented constraint strings are accepted; each starts
        //    with a null bookmark and learns one from its first query.
        const firstPrimary = env.DB.withSession("first-primary");
        const firstPrimaryBefore = firstPrimary.getBookmark();
        const firstPrimaryRow = await firstPrimary
          .prepare("SELECT COUNT(*) AS n FROM sess")
          .first();
        const firstPrimaryAfter = firstPrimary.getBookmark();

        const unconstrained = env.DB.withSession("first-unconstrained");
        const unconstrainedBefore = unconstrained.getBookmark();
        await unconstrained.prepare("SELECT 1 AS one").first();
        const unconstrainedAfter = unconstrained.getBookmark();

        return j({
          insertOk: ins.success === true,
          defaultNullBefore: defaultBefore === null,
          defaultBookmarkAfter: isBookmark(defaultAfter),
          sameSessionReads: sameSessionRead.results.map((r) => r.v),
          resumeReturnsGivenBookmark: resumedBefore === defaultAfter,
          resumedReadSeesCommittedRows: resumedRead.results.map((r) => r.v),
          // A bookmark minted before the second INSERT must sort strictly
          // before the one that insert produced.
          advancedAfterWrite:
            isBookmark(defaultAfter) &&
            isBookmark(afterWrite) &&
            afterWrite.localeCompare(defaultAfter) > 0,
          staleSessionSeesLatest:
            behindRow?.n === 2 &&
            isBookmark(behindBookmark) &&
            isBookmark(afterWrite) &&
            behindBookmark.localeCompare(afterWrite) >= 0,
          batchOk: batchRes.length === 2 && batchRes[1].results?.[0]?.n === 3,
          batchBookmarkIsString: isBookmark(batchBookmark),
          firstPrimary: {
            nullBefore: firstPrimaryBefore === null,
            stringAfter: isBookmark(firstPrimaryAfter),
            rowIsObject: typeof firstPrimaryRow === "object" && firstPrimaryRow !== null,
          },
          firstUnconstrained: {
            nullBefore: unconstrainedBefore === null,
            stringAfter: isBookmark(unconstrainedAfter),
          },
        });
      }
      return new Response("nf", { status: 404 });
    } catch (e) {
      return j({ error: String(e?.message ?? e), name: e?.name });
    }
  },
};
