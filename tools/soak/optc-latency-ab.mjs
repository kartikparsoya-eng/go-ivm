#!/usr/bin/env node
// Option-C client-perceived latency harness (TS-only vs Go-primary A/B).
//
// Drives the REAL dashboard SPA (real @xyne/shared schema, real fat dashboard
// queries, real traefik -> sync-proxy -> zero-cache wire) via Playwright, then
// harvests the Zero client SDK's OWN latency telemetry through the inspector:
//
//   window.__zero.inspector.metrics()                  (pooled TDigests)
//     query-materialization-end-to-end  -> client-perceived HYDRATION RTT
//     query-update-client               -> client-perceived ADVANCE/poke RTT
//     query-materialization-server      -> server-internal hydrate ms
//     query-update-server               -> server-internal advance ms
//   window.__zero.inspector.clientGroup.queries()      (per-query breakdown)
//     q.hydrateTotal / q.hydrateServer / q.updateClientP50/P95 / rowCount / zql
//
// The window.__zero hook is added (uncommitted, sandbox-only) in
// dashboard/src/providers/ZeroProvider.tsx. Requires the rust-test sandbox up.
//
// Run A (Go-primary, live):   LABEL=go-primary N_CGS=3 node tools/soak/optc-latency-ab.mjs
// Run B (after ENABLED=false): LABEL=ts-baseline N_CGS=3 node tools/soak/optc-latency-ab.mjs
//
// Output: tools/soak/optc-results/optc-<LABEL>-<ts>.json  (+ console summary).

import {mkdirSync, writeFileSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {fileURLToPath} from 'node:url';

const PLAYWRIGHT_IMPORT =
  process.env.PLAYWRIGHT_IMPORT ??
  '/Users/kartik.parsoya/Documents/Go-RS/mono/node_modules/playwright/index.mjs';
const {chromium} = await import(PLAYWRIGHT_IMPORT);

const __dirname = dirname(fileURLToPath(import.meta.url));

const LABEL = process.env.LABEL ?? 'run';
const N_CGS = parseInt(process.env.N_CGS ?? '3', 10);
const HYDRATE_DWELL_S = parseInt(process.env.HYDRATE_DWELL_S ?? '8', 10);
const ADVANCE_S = parseInt(process.env.ADVANCE_S ?? '30', 10);
const ADVANCE_MSG_INTERVAL_S = parseFloat(process.env.ADVANCE_MSG_INTERVAL_S ?? '2');
const BASE = process.env.BASE_URL ?? 'http://rust-test.localhost';
const OUT_DIR = process.env.OUT_DIR ?? join(__dirname, 'optc-results');

// Channels to visit per CG (each = a distinct set of fat dashboard query
// hydrations). Pulled from multi-cg-load.mjs (known-good ids in this sandbox).
const CHANNELS = [
  'cmpmblj2z002c10zqxpdasvfd',
  'cmpmjimyo00a110zqzd2u18dp',
  'cmpmjrgn900bc10zqzcsdjxs8',
  'cmp2cqlq900f7iphvij992i5e',
];
// FAT_THREAD = "<channelId>/<conversationId>". When set, each CG visits ONLY this
// thread deep-link (/{ws}/chat/dir/<channelId>/<conversationId>) instead of the
// channel list. The route mounts ThreadPannel -> queries.threadConversation,
// whose `messages` relation is UNCAPPED (shared queries.ts:322-329), so a
// conversation with N messages hydrates all N rows in one query -> the
// query-materialization-server TDigest is dominated by that single fat hydrate,
// giving a clean high-volume Go-vs-TS discriminator. Each CG = a fresh browser
// context = a distinct client group = a COLD server-side materialization, so
// N_CGS controls the number of cold fat-hydrate samples.
const FAT_THREAD = process.env.FAT_THREAD ?? '';
const WORKSPACE = 'cmp2cccas0015yifcnrkufm63';

const sleep = ms => new Promise(r => setTimeout(r, ms));

function quantile(sorted, p) {
  if (sorted.length === 0) return null;
  if (sorted.length === 1) return sorted[0];
  const idx = (sorted.length - 1) * p;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

function summarize(values) {
  const v = values.filter(x => typeof x === 'number' && !Number.isNaN(x)).sort((a, b) => a - b);
  return {
    n: v.length,
    p50: round(quantile(v, 0.5)),
    p95: round(quantile(v, 0.95)),
    max: round(v.length ? v[v.length - 1] : null),
    min: round(v.length ? v[0] : null),
  };
}

function round(x) {
  return typeof x === 'number' && !Number.isNaN(x) ? Math.round(x * 100) / 100 : null;
}

// Runs inside the page. Polls for window.__zero, then reads pooled TDigest
// percentiles + per-query rows. Returns plain JSON (no class instances).
async function harvestInPage(timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  // eslint-disable-next-line no-undef
  const z = () => window.__zero;
  while (!z() && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 250));
  }
  if (!z()) return {error: 'window.__zero never appeared'};

  const withTimeout = (p, ms) =>
    Promise.race([
      p,
      new Promise((_, rej) => setTimeout(() => rej(new Error('inspect-timeout')), ms)),
    ]);

  const qval = (d, p) => {
    try {
      if (d && typeof d.quantile === 'function') {
        const n = d.quantile(p);
        return Number.isNaN(n) ? null : n;
      }
    } catch {
      /* ignore */
    }
    return null;
  };

  const out = {clientGroupID: null, pooled: null, queries: [], errors: []};
  try {
    out.clientGroupID = await z().clientGroupID;
  } catch (e) {
    out.errors.push('clientGroupID: ' + (e?.message ?? String(e)));
  }

  try {
    const m = await withTimeout(z().inspector.metrics(), 15000);
    out.pooled = {
      hydrateE2E_p50: qval(m['query-materialization-end-to-end'], 0.5),
      hydrateE2E_p95: qval(m['query-materialization-end-to-end'], 0.95),
      hydrateServer_p50: qval(m['query-materialization-server'], 0.5),
      hydrateServer_p95: qval(m['query-materialization-server'], 0.95),
      advanceClient_p50: qval(m['query-update-client'], 0.5),
      advanceClient_p95: qval(m['query-update-client'], 0.95),
      advanceServer_p50: qval(m['query-update-server'], 0.5),
      advanceServer_p95: qval(m['query-update-server'], 0.95),
    };
  } catch (e) {
    out.errors.push('metrics: ' + (e?.message ?? String(e)));
  }

  try {
    const qs = await withTimeout(z().inspector.clientGroup.queries(), 15000);
    out.queries = qs.map(q => ({
      id: q.id,
      got: q.got,
      rowCount: q.rowCount,
      hydrateTotal: q.hydrateTotal,
      hydrateServer: q.hydrateServer,
      updateClientP50: q.updateClientP50,
      updateClientP95: q.updateClientP95,
      zql: (q.clientZQL ?? q.serverZQL ?? '').slice(0, 120),
    }));
  } catch (e) {
    out.errors.push('queries: ' + (e?.message ?? String(e)));
  }
  return out;
}

async function login(page) {
  await page.goto(`${BASE}/auth`);
  const res = await page.evaluate(async () => {
    const r = await fetch('/api/test/auth/login', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      credentials: 'include',
      body: JSON.stringify({email: 'sandbox@xyne.ai', password: 'dev-admin-password'}),
    });
    const j = await r.json().catch(() => ({}));
    return {status: r.status, user: j?.user};
  });
  if (res.status !== 200) throw new Error(`login=${res.status}`);
  await page.evaluate(u => {
    localStorage.setItem('user_id', u.id);
    localStorage.setItem('user_email', u.email);
    localStorage.setItem(`lastActiveWorkspaceId_${u.email}`, u.workspaceId);
    localStorage.setItem(`lastActiveWorkspaceName_${u.email}`, 'Test Workspace');
  }, res.user);
  return res.user?.workspaceId || WORKSPACE;
}

async function gotoChannelAndHydrate(page, ws, channel, tag) {
  await page.goto(`${BASE}/${ws}/chat/dir/${channel}`);
  try {
    await page.waitForLoadState('networkidle', {timeout: 20000});
  } catch {
    /* best effort */
  }
  // Wait for composer => view is mounted and its queries are subscribing.
  let ready = false;
  for (let i = 0; i < 40; i++) {
    ready = await page.evaluate(
      () => !!document.querySelector('[contenteditable="true"][aria-label="Message input"]'),
    );
    if (ready) break;
    await sleep(500);
  }
  await sleep(HYDRATE_DWELL_S * 1000); // let fat queries reach "got"
  return ready;
}

async function driveSends(page, tag, seconds) {
  const start = Date.now();
  let sent = 0;
  let n = 0;
  while ((Date.now() - start) / 1000 < seconds) {
    n++;
    try {
      const focused = await page.evaluate(() => {
        const ed = document.querySelector('[contenteditable="true"][aria-label="Message input"]');
        if (!ed) return false;
        ed.focus();
        return document.activeElement === ed;
      });
      if (focused) {
        await page.keyboard.type(`${tag} optc-adv #${n}`, {delay: 5});
        await sleep(150);
        const r = await page.evaluate(() => {
          const btn = document.querySelector('button[aria-label="Send message"]');
          if (btn && !btn.disabled) {
            btn.click();
            return true;
          }
          return false;
        });
        if (r) sent++;
      }
    } catch {
      /* ignore individual send failures */
    }
    await sleep(ADVANCE_MSG_INTERVAL_S * 1000);
  }
  return sent;
}

async function runCG(browser, cgIndex) {
  const tag = `cg${cgIndex}`;
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  const result = {tag, channelsVisited: [], hydratePhase: null, advancePhase: null, sent: 0, error: null};
  try {
    const ws = await login(page);
    // Visit several channels to accumulate many query hydrations in this CG,
    // OR (when FAT_THREAD set) visit ONLY the fat thread deep-link so the
    // hydrate metric reflects the single uncapped 1k-row threadConversation.
    const targets = FAT_THREAD ? [FAT_THREAD] : CHANNELS;
    for (const channel of targets) {
      const ready = await gotoChannelAndHydrate(page, ws, channel, tag);
      result.channelsVisited.push({channel: channel.slice(0, 40), ready});
    }
    // Snapshot HYDRATION metrics (advance digests still ~empty here).
    result.hydratePhase = await page.evaluate(harvestInPage, 30000);
    console.log(`[${tag}] hydrate harvested: ${result.hydratePhase?.queries?.length ?? 0} queries, ` +
      `e2e p50=${result.hydratePhase?.pooled?.hydrateE2E_p50}ms p95=${result.hydratePhase?.pooled?.hydrateE2E_p95}ms`);

    // ADVANCE phase: drive mutations on the last channel, then re-harvest so
    // query-update-client digests are populated.
    result.sent = await driveSends(page, tag, ADVANCE_S);
    await sleep(2000);
    result.advancePhase = await page.evaluate(harvestInPage, 30000);
    console.log(`[${tag}] advance harvested: sent=${result.sent}, ` +
      `client p50=${result.advancePhase?.pooled?.advanceClient_p50}ms p95=${result.advancePhase?.pooled?.advanceClient_p95}ms`);
  } catch (e) {
    result.error = e?.message ?? String(e);
    console.error(`[${tag}] ERROR: ${result.error}`);
  } finally {
    await ctx.close();
  }
  return result;
}

(async () => {
  console.log(`\n=== Option-C client-latency A/B  [LABEL=${LABEL}] ===`);
  console.log(`  CGs=${N_CGS}  hydrateDwell=${HYDRATE_DWELL_S}s  advance=${ADVANCE_S}s  target=${BASE}\n`);
  mkdirSync(OUT_DIR, {recursive: true});
  const browser = await chromium.launch({headless: true});
  // FAT_THREAD legs measure a single cold fat hydrate per CG; running CGs in
  // parallel makes their hydrates contend on the server (observed 18x inflation:
  // 0.42s isolated -> 7.8s with 4-way contention), which destroys the per-hydrate
  // measurement. Force SEQUENTIAL CG execution for fat-thread legs (or when
  // SEQUENTIAL=1) so each cold hydrate runs alone. Default stays parallel.
  // CONCURRENT=1 OVERRIDES the FAT_THREAD-forced-sequential default: it runs all
  // N_CGS via Promise.all so their cold fat hydrates hit the server SIMULTANEOUSLY.
  // For the concurrent-CG throughput A/B the contention IS the measurement (Go
  // fans each CG out to its own goroutine/engine across 14 cores; TS-only runs
  // hydrate compute on the Node event loop shared by ZERO_NUM_SYNC_WORKERS=2).
  const CONCURRENT = (process.env.CONCURRENT ?? '') === '1';
  const SEQUENTIAL = !CONCURRENT &&
    ((process.env.SEQUENTIAL ?? '') === '1' || FAT_THREAD !== '');
  console.log(`  mode=${SEQUENTIAL ? 'SEQUENTIAL' : 'CONCURRENT(Promise.all)'}  (CONCURRENT=${CONCURRENT ? 1 : 0} FAT_THREAD=${FAT_THREAD || '<none>'})`);
  let cgs = [];
  try {
    if (SEQUENTIAL) {
      for (let i = 0; i < N_CGS; i++) {
        cgs.push(await runCG(browser, i));
      }
    } else {
      cgs = await Promise.all(Array.from({length: N_CGS}, (_, i) => runCG(browser, i)));
    }
  } finally {
    await browser.close();
  }

  // Aggregate across CGs. Pool per-query values (cross-check) + collect the
  // per-CG pooled TDigest percentiles (the client's own aggregation).
  // Headline = pooled client-SDK TDigest percentiles (each CG's own pool),
  // aggregated (median) across CGs. These come from the WORKING inspector
  // metrics() RPC. The per-query queries() path is best-effort: it errors out
  // under a client(npm)<->server(mono) inspect-protocol valita skew
  // (server omits `query-materialization-server` per-query), so hydTotals is
  // usually empty -- not the headline.
  const e2eP50s = [], e2eP95s = [];        // query-materialization-end-to-end (client hydrate RTT)
  const hydSrvP50s = [], hydSrvP95s = [];  // query-materialization-server (server-internal hydrate)
  const advCliP50s = [], advCliP95s = [];  // query-update-client (client LOCAL apply, not RTT)
  const advSrvP50s = [], advSrvP95s = [];  // query-update-server (Go-attributed; approx; often empty)
  const hydTotals = [];                    // per-query client RTT (best-effort; usually empty)
  let gotQueries = 0, totalQueries = 0, totalRows = 0, totalSent = 0;

  const pushIf = (arr, v) => {
    if (typeof v === 'number' && !Number.isNaN(v)) arr.push(v);
  };

  for (const cg of cgs) {
    totalSent += cg.sent ?? 0;
    // Hydration metrics: read from the hydrate phase (advance phase re-subscribes
    // can pollute the e2e digest with near-zero re-got samples).
    const hp = cg.hydratePhase;
    if (hp?.pooled) {
      pushIf(e2eP50s, hp.pooled.hydrateE2E_p50);
      pushIf(e2eP95s, hp.pooled.hydrateE2E_p95);
      pushIf(hydSrvP50s, hp.pooled.hydrateServer_p50);
      pushIf(hydSrvP95s, hp.pooled.hydrateServer_p95);
    }
    for (const q of hp?.queries ?? []) {
      totalQueries++;
      if (q.got) gotQueries++;
      if (typeof q.rowCount === 'number') totalRows += q.rowCount;
      if (typeof q.hydrateTotal === 'number') hydTotals.push(q.hydrateTotal);
    }
    // Advance metrics: read from the advance phase (digests accumulate during sends).
    const ap = cg.advancePhase;
    if (ap?.pooled) {
      pushIf(advCliP50s, ap.pooled.advanceClient_p50);
      pushIf(advCliP95s, ap.pooled.advanceClient_p95);
      pushIf(advSrvP50s, ap.pooled.advanceServer_p50);
      pushIf(advSrvP95s, ap.pooled.advanceServer_p95);
    }
  }

  const report = {
    label: LABEL,
    timestamp: new Date().toISOString(),
    config: {N_CGS, HYDRATE_DWELL_S, ADVANCE_S, BASE},
    totals: {totalQueries, gotQueries, totalRows, totalSent},
    // Each *_acrossCGs is summarize() over the per-CG pooled percentiles, so its
    // .p50 == median across CGs of that percentile.
    hydration: {
      // query-materialization-end-to-end: subscribe -> first "got", the true
      // client-perceived hydrate RTT (network+server+wire+decode). HEADLINE.
      clientRTT_e2e_p50_acrossCGs: summarize(e2eP50s),
      clientRTT_e2e_p95_acrossCGs: summarize(e2eP95s),
      // query-materialization-server: server-internal hydrate ms.
      serverInternal_p50_acrossCGs: summarize(hydSrvP50s),
      serverInternal_p95_acrossCGs: summarize(hydSrvP95s),
      // best-effort; usually empty (inspect queries() schema skew).
      perQueryClientRTT_pool: summarize(hydTotals),
    },
    advance: {
      // query-update-client = client LOCAL push-apply only, NOT a network RTT.
      clientLocalApply_p50_acrossCGs: summarize(advCliP50s),
      clientLocalApply_p95_acrossCGs: summarize(advCliP95s),
      // query-update-server = Go per-(table,op) timing attributed to top-level-
      // table queries (pipeline-driver.ts:3452). APPROXIMATE; null/empty when no
      // inspected query's TOP-LEVEL table matched the mutated table.
      serverCompute_p50_acrossCGs: summarize(advSrvP50s),
      serverCompute_p95_acrossCGs: summarize(advSrvP95s),
    },
    perCG: cgs,
  };

  const outPath = join(OUT_DIR, `optc-${LABEL}-${Date.now()}.json`);
  writeFileSync(outPath, JSON.stringify(report, null, 2));

  const H = report.hydration, A = report.advance;
  console.log(`\n=== SUMMARY [${LABEL}]  (pooled client-SDK metrics, median across ${N_CGS} CG) ===`);
  console.log(`  queries got/total: ${gotQueries}/${totalQueries}  (~${totalRows} rows)   advance mutations sent: ${totalSent}`);
  console.log(`  HYDRATION  client-perceived RTT  (query-materialization-end-to-end):`);
  console.log(`               p50=${H.clientRTT_e2e_p50_acrossCGs.p50}ms   p95=${H.clientRTT_e2e_p95_acrossCGs.p50}ms   (${H.clientRTT_e2e_p50_acrossCGs.n} CGs)`);
  console.log(`  HYDRATION  server-internal       (query-materialization-server):`);
  console.log(`               p50=${H.serverInternal_p50_acrossCGs.p50}ms   p95=${H.serverInternal_p95_acrossCGs.p50}ms`);
  console.log(`  ADVANCE    client local-apply    (query-update-client; NOT a network RTT):`);
  console.log(`               p50=${A.clientLocalApply_p50_acrossCGs.p50}ms   p95=${A.clientLocalApply_p95_acrossCGs.p50}ms`);
  console.log(`  ADVANCE    server-compute        (query-update-server; Go-attributed, approx; null=unattributed):`);
  console.log(`               p50=${A.serverCompute_p50_acrossCGs.p50}ms   p95=${A.serverCompute_p95_acrossCGs.p50}ms`);
  console.log(`  -> ${outPath}\n`);
})();
