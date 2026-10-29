#!/usr/bin/env node
// Measure the throwaway viewer with explicitly synthetic fixture HTML.
const fs = require('fs');
const {pathToFileURL} = require('url');
const {performance} = require('perf_hooks');
const puppeteer = require(process.env.PUPPETEER_MODULE || '/tmp/box-ingress-mermaid-20261020/node_modules/puppeteer');
const html = process.argv[2];
const metadata = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
const inputFile = process.argv[4];
const timeoutMs = Number(process.env.BOX_BENCHMARK_TIMEOUT_MS || 30000);
const result = {html, scenario: metadata.scenario, events: metadata.event_count,
  actions: metadata.action_count ?? metadata.command_count, timeoutMs};
result.htmlBytes = fs.statSync(html).size;
result.loadMode = inputFile ? 'file import to ready' : 'HTML navigation to ready';
if (inputFile) {
  result.inputFile = inputFile;
  result.inputBytes = fs.statSync(inputFile).size;
}
result.latencyMethod = 'API handler and DOM update time; allow animation frames between samples. Browser paint time is excluded.';
let browser, deadline, memoryTimer, peakRendererRssKiB = 0, peakBrowserTreeRssKiB = 0;
let browserPids = new Set();
let printed = false;
const began = performance.now();
function report() {
  if (printed) return;
  printed = true;
  result.wallMs = Math.round(performance.now() - began);
  result.peakRendererRssKiB = peakRendererRssKiB;
  result.peakBrowserTreeRssKiB = peakBrowserTreeRssKiB;
  result.memoryMethod = 'Sampled browser descendant /proc RSS every 500 ms; tree RSS can double-count shared pages. Sampling adds small unmeasured overhead.';
  console.log(JSON.stringify(result, null, 2));
}
function sampleMemory() {
  if (!browser?.process()) return;
  try {
    const pending = [browser.process().pid], members = new Set(), tree = [];
    while (pending.length) {
      const pid = pending.pop();
      if (members.has(pid)) continue;
      members.add(pid);
      try {
        const children = fs.readFileSync(`/proc/${pid}/task/${pid}/children`, 'utf8').trim();
        if (children) pending.push(...children.split(/\s+/).map(Number));
        const status = fs.readFileSync(`/proc/${pid}/status`, 'utf8');
        const rss = Number(status.match(/^VmRSS:\s+(\d+)/m)?.[1] || 0);
        const cmd = fs.readFileSync(`/proc/${pid}/cmdline`, 'utf8');
        tree.push({pid, rss, renderer: cmd.includes('--type=renderer')});
      } catch {}
    }
    browserPids = members;
    peakRendererRssKiB = Math.max(peakRendererRssKiB, ...tree.filter(p => p.renderer).map(p => p.rss));
    peakBrowserTreeRssKiB = Math.max(peakBrowserTreeRssKiB, tree.reduce((n, p) => n + p.rss, 0));
  } catch {}
}
function stopBrowser() {
  for (const pid of [...browserPids].reverse()) {
    try { process.kill(pid, 'SIGKILL'); } catch {}
  }
  browser?.process()?.kill('SIGKILL');
}
async function main() {
  browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--js-flags=--expose-gc'],
    env: {...process.env, LD_LIBRARY_PATH: ['/lib64', '/usr/lib64', process.env.LD_LIBRARY_PATH].filter(Boolean).join(':')},
  });
  memoryTimer = setInterval(sampleMemory, 500);
  deadline = setTimeout(() => {
    result.status = 'timeout'; result.stage ||= 'navigation';
    sampleMemory(); report(); stopBrowser(); process.exit(2);
  }, timeoutMs);
  const page = await browser.newPage();
  await page.setViewport({width: 1440, height: 1000});
  await page.evaluateOnNewDocument(() => {
    window.__benchmarkStarted = performance.now();
    window.__benchmarkLongTasks = [];
    window.__benchmarkHeartbeat = {maxDelay: 0, samples: 0};
    let previous = performance.now();
    setInterval(() => {
      const now = performance.now(), beat = window.__benchmarkHeartbeat;
      beat.maxDelay = Math.max(beat.maxDelay, now - previous - 20);
      beat.samples++; previous = now;
    }, 20);
    new PerformanceObserver(list => {
      window.__benchmarkLongTasks.push(...list.getEntries().map(e => e.duration));
    }).observe({entryTypes: ['longtask']});
  });
  const errors = [];
  page.on('pageerror', e => errors.push(String(e)));
  result.stage = 'navigation and indexing';
  const navigation = performance.now();
  await page.goto(pathToFileURL(html).href, {waitUntil: 'domcontentloaded', timeout: timeoutMs});
  await page.waitForFunction(() => !!window.boxExecutionPrototype, {timeout: timeoutMs});
  await page.evaluate(async () => { await window.boxExecutionPrototype.ready; });
  result.loadMs = Math.round(performance.now() - navigation);
  if (inputFile) {
    result.initialPageLoadMs = result.loadMs;
    await page.$eval('#file', input => { input.onchange = null; });
    await (await page.$('#file')).uploadFile(inputFile);
    await page.evaluate(() => {
      window.__benchmarkLongTasks.length = 0;
      window.__benchmarkHeartbeat.maxDelay = 0;
      window.__benchmarkHeartbeat.samples = 0;
    });
    result.stage = 'importing execution telemetry';
    const imported = performance.now();
    await page.evaluate(async () => {
      await window.boxExecutionPrototype.openFile(document.getElementById('file').files[0]);
    });
    result.loadMs = Math.round(performance.now() - imported);
  }
  result.loadResponsiveness = await page.evaluate(() => ({
    ...window.__benchmarkHeartbeat,
    longestTaskMs: Math.max(0, ...window.__benchmarkLongTasks),
  }));
  result.internalLoadStats = await page.evaluate(() => {
    const state = window.boxExecutionPrototype.getState();
    return state.timings || state.loadStats || null;
  });
  const cdp = await page.createCDPSession();
  await cdp.send('Performance.enable');
  let metrics = await cdp.send('Performance.getMetrics');
  result.heapAfterLoadBytes = metrics.metrics.find(m => m.name === 'JSHeapUsedSize')?.value;
  result.stage = 'action switches and seeks';
  result.interactions = await page.evaluate(async metadata => {
    const api = window.boxExecutionPrototype;
    const durations = {switch: [], seek: [], timeSeek: [], step: []};
    const checks = [];
    const counts = [];
    const painted = () => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    const rootCount = metadata.action_count ?? metadata.command_count;
    const expectedId = (trace, frame) => {
      const prefix = `command-${trace}:`;
      if (metadata.scenario === 'many') {
        return prefix + ['broker', 'shell', 'exec', 'exec-result', 'read', 'filesystem',
          'filesystem', 'spawn', 'leaf', 'gateway', 'http', 'http-result', 'request',
          'response', 'gateway', 'leaf', 'leaf', 'completed', 'shell', 'broker'][frame];
      }
      if (frame === 0 || frame === metadata.events_per_command - 1) return prefix + 'broker';
      if (frame === 1 || frame === metadata.events_per_command - 2) return prefix + 'shell';
      const index = Math.floor((frame - 2) / 4), position = (frame - 2) % 4;
      return prefix + (position === 0 ? `decision-${index}` : position === 1
        ? `result-${index}` : `${index % 64 === 0 ? 'leaf' : 'filesystem'}-${index}`);
    };
    const traceIndices = [...new Set([0, Math.floor(rootCount / 2), rootCount - 1])];
    const stateSummary = () => {
      const s = api.getState();
      return {traceIndex: s.actionIndex ?? s.traceIndex, traceCount: s.actionCount ?? s.traceCount,
        frame: s.frame, events: s.events, totalEvents: s.totalEvents,
        command: s.action ?? s.command, currentId: s.currentEvent?.id,
        currentIndex: s.currentEvent?._index, currentTs: s.currentEvent?.ts,
        visibleLeaves: s.visibleLeaves ?? s.leaves,
        nodes: s.highlightedNodes, edges: s.edges?.map(e => ({from: e.from, to: e.to, id: e.event?.id}))};
    };
    for (const trace of traceIndices) {
      let t;
      if (rootCount > 1) {
        t = performance.now(); await (api.setAction || api.setTrace)(trace); durations.switch.push(performance.now() - t);
        await painted();
      }
      const length = metadata.events_per_command;
      for (const frame of [...new Set([0, Math.floor(length / 2), length - 1])]) {
        t = performance.now(); await api.setFrame(frame); durations.seek.push(performance.now() - t);
        await painted();
        const s = stateSummary();
        const prefix = `command-${trace}:`;
        const foreignEdge = s.edges?.find(e => e.id && !e.id.startsWith(prefix));
        checks.push({
          trace, frame, passed: s.traceIndex === trace && s.frame === frame
            && s.events === length && s.command === `synthetic-command ${trace}`
            && s.currentId === expectedId(trace, frame)
            && s.currentTs === 1700000000000 + (metadata.scenario === 'many' ? trace * 2 + frame * 0.5 : frame)
            && !foreignEdge,
          state: s, foreignEdge,
        });
        counts.push({
          trace, frame, dom: document.getElementsByTagName('*').length,
          timeline: document.querySelectorAll('#timeline [data-frame]').length,
          actionRows: document.querySelectorAll('#action-list .action-row, #command-list .command-row').length,
          selectElements: document.querySelectorAll('select').length,
          actionSelectElements: document.querySelectorAll('#command-list select, #action-list select, select#command, select#action').length,
          visibleLeaves: s.visibleLeaves,
          svg: document.querySelectorAll('#diagram *').length,
        });
      }
    }
    for (let i = 0; i < 20; i++) {
      const frame = Math.floor((i * 7919 % 10007) / 10007 * (metadata.events_per_command - 1));
      let t = performance.now(); await api.setFrame(frame); durations.seek.push(performance.now() - t);
      await painted();
      const time = api.getState().time;
      t = performance.now(); await api.seek(time); durations.timeSeek.push(performance.now() - t);
      await painted();
      t = performance.now(); await api.setFrame(Math.min(frame + 1, metadata.events_per_command - 1)); durations.step.push(performance.now() - t);
      await painted();
    }
    return {durations, checks, counts, longTasks: window.__benchmarkLongTasks,
      heapBrowser: performance.memory?.usedJSHeapSize};
  }, metadata);
  metrics = await cdp.send('Performance.getMetrics');
  result.heapAfterInteractionBytes = metrics.metrics.find(m => m.name === 'JSHeapUsedSize')?.value;
  await page.evaluate(() => globalThis.gc?.());
  metrics = await cdp.send('Performance.getMetrics');
  result.heapAfterGcBytes = metrics.metrics.find(m => m.name === 'JSHeapUsedSize')?.value;
  const percentile = (values, p) => [...values].sort((a,b) => a-b)[Math.min(values.length - 1, Math.floor(values.length * p))];
  result.latencyMs = Object.fromEntries(Object.entries(result.interactions.durations).filter(([,values]) => values.length).map(([name, values]) => [
    name, {samples: values.length, median: percentile(values, 0.5), p95: percentile(values, 0.95), maximum: Math.max(...values)}
  ]));
  result.maxDom = Math.max(...result.interactions.counts.map(x => x.dom));
  result.domBounds = {
    timelineRows: Math.max(...result.interactions.counts.map(x => x.timeline)),
    actionRows: Math.max(...result.interactions.counts.map(x => x.actionRows)),
    selectElements: Math.max(...result.interactions.counts.map(x => x.selectElements)),
    actionSelectElements: Math.max(...result.interactions.counts.map(x => x.actionSelectElements)),
    visibleLeaves: Math.max(...result.interactions.counts.map(x => x.visibleLeaves)),
    passed: result.interactions.counts.every(x => x.timeline <= 80 && x.actionRows <= 40
      && x.actionSelectElements === 0 && x.visibleLeaves <= 60),
  };
  result.errors = errors;
  result.status = result.interactions.checks.every(x => x.passed) && result.domBounds.passed
    && !errors.length ? 'passed' : 'failed';
  result.stage = 'complete';
  sampleMemory(); clearTimeout(deadline); clearInterval(memoryTimer);
  report(); await browser.close();
}
main().catch(async error => {
  result.status = 'error'; result.error = String(error);
  clearTimeout(deadline); clearInterval(memoryTimer); sampleMemory(); report();
  stopBrowser();
  process.exitCode = 1;
});
