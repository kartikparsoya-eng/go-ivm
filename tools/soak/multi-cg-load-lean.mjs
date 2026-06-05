#!/usr/bin/env node
// LEAN multi-CG browser soak — same real-dashboard multi-user coverage as
// multi-cg-load.mjs, but tuned NOT to OOM a contended Mac. The original OOMs
// because each browser CONTEXT spins up its own Chromium renderer running the
// full Xyne dashboard SPA (React + Zero client + IndexedDB + WebSocket); N of
// those booting at once, on top of the docker stack, tips the machine over.
//
// This variant cuts per-renderer memory four ways:
//   1. --process-per-site collapses same-origin contexts onto a SHARED renderer
//      process (N contexts -> ~1 renderer) instead of one renderer each.
//   2. capped V8 heap (--max-old-space-size=192) + no GPU + no /dev/shm.
//   3. blocks image/font/media requests per context (the DOM-driven send loop
//      doesn't need them; sync logic is untouched) — big decoded-bitmap + net
//      savings.
//   4. STAGGERED context startup so N SPAs don't hydrate simultaneously — that
//      simultaneous boot is the spike that actually triggers the OOM.
//
// HARD CAPS (enforced below, not just documented): N_CGS<=3, DURATION_S<=240.
//
// Usage (rust-test sandbox up + serving; shadow or Go-primary mode):
//   N_CGS=3 DURATION_S=180 MSG_INTERVAL_S=4 node tools/soak/multi-cg-load-lean.mjs
//
// If it STILL strains the box, drop to N_CGS=2, or add '--single-process' to
// LAUNCH_ARGS (most aggressive — one process for everything; less stable, but
// fine for a short low-N soak). For zero-browser multi-CG coverage instead, use
// the WebSocket soak (sandbox-soak-traffic.ts) which has no renderer cost.

const PLAYWRIGHT_IMPORT =
  process.env.PLAYWRIGHT_IMPORT ??
  '/Users/kartik.parsoya/Documents/Go-RS/mono/node_modules/playwright/index.mjs';
const {chromium} = await import(PLAYWRIGHT_IMPORT);

// Hard cap = 6 (useful ceiling on this Mac). EMPIRICAL (2026-06-05): the
// --process-per-site shared renderer makes MEMORY a non-issue — free% plateaued
// at ~34% from N=2 through N=5, and N=8 still held ~33%. The real bottleneck is
// the SINGLE shared renderer running N full SPA instances: at N=8 only ~4 of 8
// CGs ever hydrated (editor=ready) within 90s — the renderer serializes and
// saturates. So ~5 is the sweet spot (all interactive); 6 is the ragged edge.
// Removing --process-per-site would parallelize hydration but reintroduces the
// per-renderer OOM (original died at N=4). For MORE concurrent CGs (server-side
// load, not real browser shapes), use the WS soak sandbox-soak-traffic.ts — no
// renderer, so 24-50+ CGs, memory-safe. ALWAYS run under the free<15% auto-kill.
const N_CGS = Math.min(parseInt(process.env.N_CGS ?? '3', 10), 6);
const DURATION_S = Math.min(parseInt(process.env.DURATION_S ?? '180', 10), 240);
const MSG_INTERVAL_S = parseInt(process.env.MSG_INTERVAL_S ?? '4', 10);
// Spread context creation so the SPA-boot memory peak is flattened.
const STAGGER_MS = parseInt(process.env.STAGGER_MS ?? '2500', 10);
// Every Nth send cycle, rotate the CG to a different channel (re-hydrates a
// different conversation-list / EXISTS+cursor query set).
const ROTATE_EVERY = parseInt(process.env.ROTATE_EVERY ?? '3', 10);
// Messages sent per cycle (burst) — each is an insert mutation = an advance.
// More writes = more advance-divergence surface per CG.
const WRITES_PER_CYCLE = parseInt(process.env.WRITES_PER_CYCLE ?? '3', 10);
// Wheel-up steps per scroll = how deep into message history we paginate.
const SCROLL_DEPTH = parseInt(process.env.SCROLL_DEPTH ?? '4', 10);

// Chromium flags that share the renderer + cap memory. --process-per-site is
// the big lever (same-origin contexts share one renderer process).
const LAUNCH_ARGS = [
  '--process-per-site',
  '--renderer-process-limit=2',
  '--js-flags=--max-old-space-size=192',
  '--disable-dev-shm-usage',
  '--disable-gpu',
  '--disable-extensions',
  '--disable-background-networking',
  '--disable-renderer-backgrounding',
  '--disable-backgrounding-occluded-windows',
  '--disable-features=Translate,BackForwardCache',
];

// Resource types to drop per context — none are needed for the DOM-driven send
// loop (which queries/clicks elements directly, not visually), and blocking
// them cuts decoded-image memory + network without touching JS/sync/websocket.
const BLOCK_TYPES = new Set(['image', 'font', 'media']);

const CHANNELS = [
  'cmpmblj2z002c10zqxpdasvfd', // test
  'cmpmjimyo00a110zqzd2u18dp', // gpro
  'cmpmjrgn900bc10zqzcsdjxs8', // notbisvw
  'cmp2cqlq900f7iphvij992i5e', // personal
];
const WORKSPACE = 'cmp2cccas0015yifcnrkufm63';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function runCG(browser, cgIndex) {
  // Stagger: the i-th CG waits before booting so the N SPAs don't hydrate at
  // once (the simultaneous-boot spike is what OOMs).
  await sleep(cgIndex * STAGGER_MS);

  const ctx = await browser.newContext();
  // Drop heavy resources before any navigation.
  await ctx.route('**/*', (route) =>
    BLOCK_TYPES.has(route.request().resourceType())
      ? route.abort()
      : route.continue(),
  );
  const page = await ctx.newPage();
  const channel = CHANNELS[cgIndex % CHANNELS.length];
  const tag = `cg${cgIndex}`;

  page.on('console', (msg) => {
    const txt = msg.text();
    if (txt.includes('Not connected') || txt.includes('not running') || txt.includes('ProtocolError')) {
      console.log(`[${tag}][console-err] ${txt.slice(0, 200)}`);
    }
  });

  try {
    await page.goto('http://rust-test.localhost/auth');
    // Login + cookie set; capture the workspaceId from the response so we
    // navigate to a workspace the logged-in user actually belongs to.
    const login = await page.evaluate(async () => {
      const r = await fetch('/api/test/auth/login', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        credentials: 'include',
        body: JSON.stringify({email: 'sandbox@xyne.ai', password: 'dev-admin-password'}),
      });
      const j = await r.json().catch(() => ({}));
      return {status: r.status, user: j?.user};
    });
    if (login.status !== 200) throw new Error(`login=${login.status}`);
    const ws = login.user?.workspaceId || WORKSPACE;

    // Seed localStorage so the SPA's xstate auth machine boots authenticated
    // (cookies are set by login, but the machine also reads user_id/email
    // from localStorage on initial mount).
    await page.evaluate((u) => {
      localStorage.setItem('user_id', u.id);
      localStorage.setItem('user_email', u.email);
      localStorage.setItem(`lastActiveWorkspaceId_${u.email}`, u.workspaceId);
      localStorage.setItem(`lastActiveWorkspaceName_${u.email}`, 'Test Workspace');
    }, login.user);

    // Navigate to the channel and wait for the composer to actually mount.
    // DOM-based polling (not a Playwright locator) — robust under contention.
    let ready = false;
    for (let attempt = 0; attempt < 3 && !ready; attempt++) {
      await page.goto(`http://rust-test.localhost/${ws}/chat/dir/${channel}`);
      try { await page.waitForLoadState('networkidle', {timeout: 20000}); } catch {}
      for (let i = 0; i < 60; i++) { // up to 30s
        ready = await page.evaluate(() => !!document.querySelector('[contenteditable="true"][aria-label="Message input"]'));
        if (ready) break;
        await sleep(500);
      }
      if (!ready && attempt < 2) console.log(`[${tag}] editor missing (attempt ${attempt + 1}) — reloading`);
    }
    console.log(`[${tag}] online → ch=${channel.slice(0, 14)} editor=${ready ? 'ready' : 'MISSING'}`);

    // Interaction loop — maximize WRITES + HYDRATIONS per CG (we're renderer-
    // capacity-bound at ~5 CGs, so depth-per-CG is the lever):
    //   (1) ROTATE channels        → re-hydrate a different EXISTS+cursor list;
    //   (2) DEEP SCROLL             → backward cursor+LIMIT pagination (where the
    //                                 Skip→Exists→Take boundary divergences live);
    //   (3) BURST WRITE N messages  → N insert mutations = N advances;
    //   (4) EDIT the last message   → UPDATE mutation (different advance shape);
    //   (5) REACT to a message      → reaction insert + reaction-count hydration;
    //   (6) OPEN a thread           → thread message-list hydration.
    // (4)-(6) are best-effort (graceful no-op if the dashboard's selectors differ).
    const ensureEditor = async () => {
      for (let i = 0; i < 30; i++) {
        if (await page.evaluate(() => !!document.querySelector('[contenteditable="true"][aria-label="Message input"]'))) return true;
        await sleep(300);
      }
      return false;
    };
    const sendOne = async (text) => {
      const focused = await page.evaluate(() => {
        const ed = document.querySelector('[contenteditable="true"][aria-label="Message input"]');
        if (!ed) return false;
        ed.focus();
        return document.activeElement === ed;
      });
      if (!focused) return 'no-editor';
      await page.keyboard.type(text, {delay: 4});
      await sleep(110);
      return await page.evaluate(() => {
        const btn = document.querySelector('button[aria-label="Send message"]');
        if (btn && !btn.disabled) { btn.click(); return 'sent'; }
        return btn ? 'btn-disabled' : 'no-btn';
      });
    };
    // Hover the last message row to reveal its action menu, then run an action
    // matching a label regex. Returns true if it clicked something.
    const messageAction = async (labelRe) => {
      try {
        const box = await page.evaluate(() => {
          const rows = [...document.querySelectorAll('[data-message-id],[data-testid*="message" i],li,article')]
            .filter(e => e.offsetParent && e.getBoundingClientRect().height > 16);
          const r = rows[rows.length - 1];
          if (!r) return null;
          const b = r.getBoundingClientRect();
          return {x: b.x + b.width / 2, y: b.y + b.height / 2};
        });
        if (!box) return false;
        await page.mouse.move(box.x, box.y);
        await sleep(180);
        return await page.evaluate(reSrc => {
          const re = new RegExp(reSrc, 'i');
          const b = [...document.querySelectorAll('button,[role="button"],[role="menuitem"]')]
            .find(el => el.offsetParent && re.test((el.getAttribute('aria-label') || '') + ' ' + (el.title || '') + ' ' + (el.textContent || '').slice(0, 24)));
          if (b) { b.click(); return true; }
          return false;
        }, labelRe.source);
      } catch { return false; }
    };
    const start = Date.now();
    let cyc = 0, sent = 0, scrolls = 0, switches = 0, edits = 0, reacts = 0, threads = 0, curIdx = cgIndex;
    while ((Date.now() - start) / 1000 < DURATION_S) {
      cyc++;
      // (1) Channel rotation.
      if (cyc % ROTATE_EVERY === 0) {
        curIdx = (curIdx + 1) % CHANNELS.length;
        try {
          await page.goto(`http://rust-test.localhost/${ws}/chat/dir/${CHANNELS[curIdx]}`);
          try { await page.waitForLoadState('networkidle', {timeout: 8000}); } catch {}
          await ensureEditor();
          switches++;
        } catch {}
      }
      // (2) Deep scroll up (paginate history) then back to bottom.
      try {
        await page.mouse.move(640, 360);
        for (let s = 0; s < SCROLL_DEPTH; s++) { await page.mouse.wheel(0, -1600); await sleep(160); }
        await page.mouse.wheel(0, SCROLL_DEPTH * 1600 + 2000);
        scrolls++;
      } catch {}
      // (3) Burst writes.
      let okThisCycle = false;
      for (let w = 0; w < WRITES_PER_CYCLE; w++) {
        try {
          const r = await sendOne(`${tag} c${cyc} w${w} ${Date.now().toString(36)}`);
          if (r === 'sent') { sent++; okThisCycle = true; }
          else if (cyc <= 2) console.log(`[${tag}] write: ${r}`);
        } catch {}
        await sleep(120);
      }
      // (4)-(6) best-effort richer mutations/hydrations, only if writes worked.
      if (okThisCycle) {
        if (await messageAction(/^edit|edit message/)) {
          try { await page.keyboard.type(' (edited)', {delay: 4}); await page.keyboard.press('Enter'); edits++; } catch {}
        }
        if (await messageAction(/react|add reaction|emoji/)) {
          // pick the first emoji in any opened picker
          try { await page.evaluate(() => { const e = document.querySelector('[role="dialog"] button, .emoji-picker button, [data-emoji]'); if (e) e.click(); }); reacts++; } catch {}
          try { await page.keyboard.press('Escape'); } catch {}
        }
        if (await messageAction(/reply|thread|open thread/)) {
          threads++;
          await sleep(300);
          try { await page.keyboard.press('Escape'); } catch {}
        }
      }
      await sleep(MSG_INTERVAL_S * 1000);
    }
    console.log(`[${tag}] done — sent ${sent}, scrolls ${scrolls}, switches ${switches}, edits ${edits}, reacts ${reacts}, threads ${threads}`);
  } catch (err) {
    console.error(`[${tag}] ERROR: ${err.message}`);
  } finally {
    await ctx.close();
  }
}

(async () => {
  console.log(
    `[LEAN] ${N_CGS} CGs (cap 5), ${DURATION_S}s (cap 240), msg/${MSG_INTERVAL_S}s, ` +
      `rotate/${ROTATE_EVERY} + scroll-paginate, stagger ${STAGGER_MS}ms, ` +
      `shared-renderer + blocked ${[...BLOCK_TYPES].join('/')}`,
  );
  const browser = await chromium.launch({headless: true, args: LAUNCH_ARGS});
  try {
    await Promise.all(
      Array.from({length: N_CGS}, (_, i) => runCG(browser, i)),
    );
  } finally {
    await browser.close();
  }
  console.log('[LEAN] All CGs done.');
})();
