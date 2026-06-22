#!/usr/bin/env node
// Throwaway probe: determine the minimal fix for the Vite "Failed to fetch
// dynamically imported module" error when calling inspector.metrics()/queries()
// (the lazy import('./lazy-inspector.ts') in @rocicorp/zero inspector.ts:66).
// Tests: (1) retry after optimize settles, (2) retry after page.reload().

const PW = '/Users/kartik.parsoya/Documents/Go-RS/mono/node_modules/playwright/index.mjs';
const {chromium} = await import(PW);
const BASE = 'http://rust-test.localhost';
const CHANNEL = 'cmpmblj2z002c10zqxpdasvfd';
const sleep = ms => new Promise(r => setTimeout(r, ms));

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
  return res.user?.workspaceId;
}

async function gotoHydrate(page, ws) {
  await page.goto(`${BASE}/${ws}/chat/dir/${CHANNEL}`);
  for (let i = 0; i < 40; i++) {
    const ready = await page.evaluate(
      () => !!document.querySelector('[contenteditable="true"][aria-label="Message input"]'),
    );
    if (ready) break;
    await sleep(500);
  }
}

async function probe(page, tag) {
  return page.evaluate(async t => {
    const out = {tag: t, hasZero: false, cgid: null, metricsErr: null, queriesN: null, queriesErr: null};
    const z = window.__zero;
    if (!z) return out;
    out.hasZero = true;
    try { out.cgid = await z.clientGroupID; } catch (e) { out.cgid = 'ERR:' + e.message; }
    try { await z.inspector.metrics(); out.metricsErr = 'OK'; }
    catch (e) { out.metricsErr = e?.message ?? String(e); }
    try { const qs = await z.inspector.clientGroup.queries(); out.queriesN = qs.length; }
    catch (e) { out.queriesErr = e?.message ?? String(e); }
    return out;
  }, tag);
}

(async () => {
  const browser = await chromium.launch({headless: true});
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  try {
    const ws = await login(page);
    await gotoHydrate(page, ws);
    await sleep(5000);

    console.log('A (first call, expect fail):', JSON.stringify(await probe(page, 'A')));
    await sleep(5000); // let Vite optimize settle
    console.log('B (retry no reload):       ', JSON.stringify(await probe(page, 'B')));

    await page.reload();
    for (let i = 0; i < 40; i++) {
      const ok = await page.evaluate(() => !!window.__zero);
      if (ok) break;
      await sleep(500);
    }
    await sleep(6000); // re-hydrate after reload
    console.log('C (after reload):          ', JSON.stringify(await probe(page, 'C')));
  } catch (e) {
    console.error('PROBE ERROR:', e.message);
  } finally {
    await ctx.close();
    await browser.close();
  }
})();
