#!/usr/bin/env node
// Spawns N parallel isolated Playwright browser contexts against the
// rust-test sandbox. Each context = its own cookie jar / localStorage =
// fresh clientGroupID = a separate CG inside the shared Go sidecar.
//
// Each CG logs in, opens a different channel, and sends one message every
// N seconds for the soak duration. Stresses the shared-sidecar concurrency
// model: per-CG MemorySource state, per-CG goroutine queue, shared advance
// fanout, drift audit cycling across CGs.
//
// Usage (rust-test sandbox must be up + serving; shadow or Go-primary mode):
//   N_CGS=4 DURATION_S=180 MSG_INTERVAL_S=4 node tools/soak/multi-cg-load.mjs
//
// LIMITS on a contended Mac: keep N_CGS<=4 and DURATION_S<=240 — Playwright
// Chrome contexts + docker can OOM Warp/the system above that.
// PLAYWRIGHT_IMPORT overrides where Playwright is resolved from (default: the
// mono checkout's node_modules). Reconstructed + hardened 2026-06-04: the
// editor-selector "drift" was actually a startup focus race (now raw DOM
// .focus) + intermittent composer non-render under contention (now
// networkidle wait + reload-retry) → 100% send success.

const PLAYWRIGHT_IMPORT =
  process.env.PLAYWRIGHT_IMPORT ??
  '/Users/kartik.parsoya/Documents/Go-RS/mono/node_modules/playwright/index.mjs';
const {chromium} = await import(PLAYWRIGHT_IMPORT);

const N_CGS = parseInt(process.env.N_CGS ?? '5', 10);
const DURATION_S = parseInt(process.env.DURATION_S ?? '120', 10);
const MSG_INTERVAL_S = parseInt(process.env.MSG_INTERVAL_S ?? '4', 10);

const CHANNELS = [
  'cmpmblj2z002c10zqxpdasvfd', // test
  'cmpmjimyo00a110zqzd2u18dp', // gpro
  'cmpmjrgn900bc10zqzcsdjxs8', // notbisvw
  'cmp2cqlq900f7iphvij992i5e', // personal
];
const WORKSPACE = 'cmp2cccas0015yifcnrkufm63';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function runCG(browser, cgIndex) {
  const ctx = await browser.newContext();
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
    // The selector itself ([contenteditable][aria-label="Message input"]) is
    // correct — the flakiness is that under 4 concurrent headless Chromes the
    // SPA sometimes finishes its initial render WITHOUT the composer. So:
    //   1. wait for networkidle to let the SPA hydrate,
    //   2. poll the raw DOM for the editor (not a Playwright locator — its
    //      strict/visible actionability check proved unreliable here),
    //   3. if still missing, reload once and retry (recovers a failed render).
    // Poll for the EDITOR only — the send button isn't in the DOM until text
    // is typed, so gating on it would never resolve.
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

    // Send loop. Tiptap responds to real keyboard events, not execCommand —
    // we need page.keyboard.type() so React state updates and the send button
    // enables. We still click via DOM since Playwright locator lookups across
    // many CGs are slow.
    const start = Date.now();
    let msgN = 0;
    let sent = 0;
    while ((Date.now() - start) / 1000 < DURATION_S) {
      const text = `${tag} msg #${++msgN}`;
      try {
        // Raw DOM .focus() — bypasses Playwright locator actionability
        // (stable + uncovered) checks, which timed out at 2s under 4-browser
        // contention while the channel was still settling, eating the first
        // ~10 sends per CG. keyboard.type then drives Tiptap's React state.
        const focused = await page.evaluate(() => {
          const ed = document.querySelector('[contenteditable="true"][aria-label="Message input"]');
          if (!ed) return false;
          ed.focus();
          return document.activeElement === ed;
        });
        if (!focused) {
          if (msgN <= 3 || msgN % 20 === 0) console.log(`[${tag}] msg ${msgN}: no-editor`);
          await sleep(MSG_INTERVAL_S * 1000);
          continue;
        }
        await page.keyboard.type(text, {delay: 5});
        // Brief pause so React re-renders and enables the send button.
        await sleep(150);
        const result = await page.evaluate(() => {
          const btn = document.querySelector('button[aria-label="Send message"]');
          if (btn && !btn.disabled) { btn.click(); return 'sent'; }
          return btn ? 'btn-disabled' : 'no-btn';
        });
        if (result === 'sent') sent++;
        else if (msgN <= 3 || msgN % 20 === 0) console.log(`[${tag}] msg ${msgN}: ${result}`);
      } catch (e) {
        if (msgN <= 3) console.log(`[${tag}] msg ${msgN} err: ${e.message?.slice(0, 100)}`);
      }
      await sleep(MSG_INTERVAL_S * 1000);
    }
    console.log(`[${tag}] done — attempted ${msgN}, sent ${sent}`);
  } catch (err) {
    console.error(`[${tag}] ERROR: ${err.message}`);
  } finally {
    await ctx.close();
  }
}

(async () => {
  console.log(`Starting ${N_CGS} parallel CGs for ${DURATION_S}s, msg every ${MSG_INTERVAL_S}s`);
  const browser = await chromium.launch({headless: true});
  try {
    await Promise.all(
      Array.from({length: N_CGS}, (_, i) => runCG(browser, i)),
    );
  } finally {
    await browser.close();
  }
  console.log('All CGs done.');
})();
