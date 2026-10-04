import { spawn } from "node:child_process";
import { createCipheriv, createDecipheriv, createHmac, randomBytes } from "node:crypto";
import { unlink, writeFile } from "node:fs/promises";
import os from "node:os";

const BASE = (process.env.BASE || "").replace(/\/$/, "");
const JOB = process.env.JOB;
const SECRET = process.env.HFV_DISPATCH_KEY;

if (!BASE || !JOB || !SECRET) {
  console.error("BASE, JOB and HFV_DISPATCH_KEY are required");
  process.exit(2);
}

function signed(method, path) {
  const ts = String(Date.now());
  const sig = createHmac("sha256", SECRET).update(`${ts}.${method}.${path}`).digest("hex");
  return { "x-hfv-ts": ts, "x-hfv-sig": sig };
}

async function call(method, path, body) {
  let lastErr;
  for (let attempt = 1; attempt <= 6; attempt++) {
    try {
      const res = await fetch(BASE + path, {
        method,
        headers: { ...signed(method, path), ...(body ? { "content-type": "application/json" } : {}) },
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(30000),
      });
      if (res.status === 410 || res.status === 404 || res.status === 401) {
        throw Object.assign(new Error(`hf-vault answered ${res.status} for ${method} ${path}`), { fatal: true });
      }
      if (!res.ok) throw new Error(`hf-vault answered ${res.status}`);
      return await res.json();
    } catch (e) {
      lastErr = e;
      if (e.fatal) throw e;
      await new Promise((r) => setTimeout(r, 1500 * attempt));
    }
  }
  throw lastErr;
}

class Gate {
  constructor(n) {
    this.n = n;
    this.q = [];
  }
  async acquire() {
    if (this.n > 0) {
      this.n--;
      return;
    }
    await new Promise((r) => this.q.push(r));
  }
  release() {
    const next = this.q.shift();
    if (next) next();
    else this.n++;
  }
}

const plan = await call("GET", `/api/dispatch/plan/${JOB}`);
console.log(`::add-mask::${plan.token}`);
console.log(`::add-mask::${plan.key}`);

const T = plan.tuning;
const STALL_MS = T.stallMs > 0 ? T.stallMs : 6000;
const HEDGE_MS = T.hedgeMs > 0 ? T.hedgeMs : 2500;
const MAX_ATTEMPTS = T.attempts > 0 ? T.attempts : 10;
const MAX_SKIP = T.maxSkip > 0 ? T.maxSkip : 0;
const MAX_FAILED_SEGMENTS = 25;
const skippedNames = [];
const skippedQueue = [];
const OUT_KEY = Buffer.from(plan.key, "hex");
const OUT_IV = Buffer.from(plan.iv, "hex");
const total = plan.tracks.reduce((n, t) => n + t.segments.length + (t.init ? 1 : 0), 0);
console.log(`job ${JOB}: ${plan.tracks.length} track(s), ${total} object(s), cores=${os.cpus().length}`);

const t0 = performance.now();
const since = () => (performance.now() - t0) / 1000;
const stats = { downBytes: 0, upBytes: 0, commits: 0, commitMs: 0 };
const doneQueue = [];
let aborted = false;
let abortReason = null;
const failures = [];

function abort(reason) {
  if (!aborted) {
    aborted = true;
    abortReason = reason;
  }
}

function seqIv(seq) {
  const iv = Buffer.alloc(16);
  iv.writeUInt32BE(seq >>> 0, 12);
  return iv;
}

async function fetchOnce(url, headers, range, outerSignal) {
  const controller = new AbortController();
  let timer = setTimeout(() => controller.abort(new Error("no response")), 20000);
  const arm = () => {
    clearTimeout(timer);
    timer = setTimeout(() => controller.abort(new Error(`no data for ${STALL_MS / 1000}s`)), STALL_MS);
  };
  const onOuter = () => controller.abort(new Error("cancelled"));
  if (outerSignal) {
    if (outerSignal.aborted) onOuter();
    else outerSignal.addEventListener("abort", onOuter, { once: true });
  }
  try {
    const h = { ...headers };
    if (range) h.Range = `bytes=${range[0]}-${range[0] + range[1] - 1}`;
    const res = await fetch(url, { headers: h, signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    if (range && res.status !== 206) throw new Error(`byte range ignored (HTTP ${res.status})`);
    arm();
    const chunks = [];
    const reader = res.body.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      arm();
      chunks.push(value);
    }
    return Buffer.concat(chunks);
  } finally {
    clearTimeout(timer);
    if (outerSignal) outerSignal.removeEventListener("abort", onOuter);
  }
}

function hedgedFetch(item) {
  return new Promise((resolve, reject) => {
    const controllers = [];
    let settled = false;
    let launched = 0;
    let failed = 0;
    let timer = null;
    const finish = (fn, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      for (const c of controllers) c.abort();
      fn(value);
    };
    const launch = () => {
      if (launched >= 2 || settled) return;
      launched++;
      const c = new AbortController();
      controllers.push(c);
      fetchOnce(item.u, plan.headers, item.r, c.signal).then(
        (buf) => finish(resolve, buf),
        (err) => {
          if (settled) return;
          failed++;
          if (failed >= launched) {
            if (launched < 2) launch();
            else finish(reject, err);
          }
        },
      );
    };
    timer = setTimeout(launch, HEDGE_MS);
    launch();
  });
}

async function download(item) {
  let lastErr;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    if (aborted) throw new Error(abortReason || "aborted");
    try {
      return await hedgedFetch(item);
    } catch (e) {
      lastErr = e;
      await new Promise((r) => setTimeout(r, Math.min(500 * attempt, 2500)));
    }
  }
  throw new Error(`download failed after ${MAX_ATTEMPTS} attempts: ${lastErr?.message}`);
}

function transform(raw, track, item, isInit) {
  let data = raw;
  if (track.srcKey) {
    const iv = item.iv ? Buffer.from(item.iv, "hex") : track.srcIv ? Buffer.from(track.srcIv, "hex") : seqIv(item.q);
    const d = createDecipheriv("aes-128-cbc", Buffer.from(track.srcKey, "hex"), iv);
    data = Buffer.concat([d.update(raw), d.final()]);
  }
  const c = createCipheriv("aes-128-cbc", OUT_KEY, OUT_IV);
  return Buffer.concat([c.update(data), c.final()]);
}

class PyWorker {
  constructor() {
    this.proc = null;
    this.pending = new Map();
    this.id = 1;
    this.buf = "";
  }
  start() {
    if (this.proc) return;
    this.proc = spawn("python3", ["uploader.py"], {
      stdio: ["pipe", "pipe", "inherit"],
      env: { HF_XET_HIGH_PERFORMANCE: "1", HF_HUB_DISABLE_PROGRESS_BARS: "1", ...process.env },
    });
    this.proc.stdout.on("data", (chunk) => {
      this.buf += chunk.toString();
      let i;
      while ((i = this.buf.indexOf("\n")) >= 0) {
        const line = this.buf.slice(0, i).trim();
        this.buf = this.buf.slice(i + 1);
        if (!line) continue;
        let m;
        try {
          m = JSON.parse(line);
        } catch {
          continue;
        }
        const p = this.pending.get(m.id);
        if (!p) continue;
        this.pending.delete(m.id);
        if (m.ok) p.resolve();
        else p.reject(new Error(m.error));
      }
    });
    this.proc.on("exit", () => {
      for (const p of this.pending.values()) p.reject(new Error("uploader exited"));
      this.pending.clear();
      this.proc = null;
    });
  }
  reset() {
    const p = this.proc;
    this.proc = null;
    this.buf = "";
    for (const pending of this.pending.values()) pending.reject(new Error("uploader reset"));
    this.pending.clear();
    if (p) {
      p.removeAllListeners("exit");
      p.stdout.removeAllListeners("data");
      p.kill();
    }
  }
  commit(req) {
    this.start();
    const id = this.id++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.proc.stdin.write(JSON.stringify({ id, ...req }) + "\n");
    });
  }
}

const idle = Array.from({ length: T.workers }, () => new PyWorker());
const queue = { items: [], bytes: 0, firstAt: 0 };
let pumpTimer = null;

function eligible(now) {
  if (queue.items.length === 0) return false;
  return queue.items.length >= T.minFiles || queue.bytes >= T.minMb * 1048576 || now - queue.firstAt >= T.ageMs;
}

function schedule(path, buf) {
  if (queue.items.length === 0) queue.firstAt = performance.now();
  return new Promise((resolve, reject) => {
    queue.items.push({ path, buf, resolve, reject });
    queue.bytes += buf.length;
    pump();
  });
}

function pump() {
  const now = performance.now();
  while (idle.length > 0 && eligible(now)) {
    const batch = [];
    let bytes = 0;
    while (queue.items.length > 0 && batch.length < T.batchFiles && bytes < T.batchMb * 1048576) {
      const item = queue.items.shift();
      batch.push(item);
      bytes += item.buf.length;
    }
    queue.bytes -= bytes;
    queue.firstAt = now;
    void runBatch(idle.pop(), batch);
  }
  if (!pumpTimer && queue.items.length > 0) {
    pumpTimer = setTimeout(() => {
      pumpTimer = null;
      pump();
    }, 100);
  }
}

async function runBatch(worker, batch) {
  const temps = batch.map(() => `/tmp/up_${randomBytes(8).toString("hex")}`);
  const started = performance.now();
  try {
    await Promise.all(batch.map((b, i) => writeFile(temps[i], b.buf)));
    let lastErr;
    for (let attempt = 1; attempt <= 4; attempt++) {
      try {
        await worker.commit({ repo: plan.repo, token: plan.token, title: "t", items: batch.map((b, i) => ({ path: b.path, tempPath: temps[i] })) });
        lastErr = null;
        break;
      } catch (e) {
        lastErr = e;
        worker.reset();
        await new Promise((r) => setTimeout(r, 3000 * attempt));
      }
    }
    if (lastErr) throw lastErr;
    stats.commits++;
    stats.commitMs += performance.now() - started;
    for (const b of batch) b.resolve();
  } catch (e) {
    for (const b of batch) b.reject(e);
  } finally {
    await Promise.all(temps.map((t) => unlink(t).catch(() => undefined)));
    idle.push(worker);
    pump();
  }
}

const dlGate = new Gate(T.downloads);
const backlogGate = new Gate(T.backlog);

async function processItem(track, item, isInit) {
  await backlogGate.acquire();
  try {
    if (aborted) throw new Error(abortReason || "aborted");
    await dlGate.acquire();
    let raw;
    try {
      raw = await download(item);
    } finally {
      dlGate.release();
    }
    stats.downBytes += raw.length;
    const enc = transform(raw, track, item, isInit);
    await schedule(item.p, enc);
    stats.upBytes += enc.length;
    doneQueue.push({ n: item.n, s: enc.length });
  } catch (e) {
    if (!isInit && !aborted && skippedNames.length < MAX_SKIP) {
      skippedNames.push(item.n);
      skippedQueue.push(item.n);
      console.log(`skipping stalled segment ${item.n} after ${MAX_ATTEMPTS} attempts: ${e.message}`);
      return;
    }
    failures.push(`${item.n}: ${e.message}`);
    if (failures.length >= MAX_FAILED_SEGMENTS) abort(`${failures.length} segments failed, first: ${failures[0]}`);
    throw e;
  } finally {
    backlogGate.release();
  }
}

let reporting = false;
async function flush() {
  if (reporting) return;
  reporting = true;
  try {
    const batch = doneQueue.splice(0, doneQueue.length);
    const skipBatch = skippedQueue.splice(0, skippedQueue.length);
    try {
      const res = await call("POST", `/api/dispatch/progress/${JOB}`, { done: batch, skipped: skipBatch });
      if (res.cancel) abort("hf-vault cancelled the job");
    } catch (e) {
      doneQueue.unshift(...batch);
      skippedQueue.unshift(...skipBatch);
      if (e.fatal) abort(e.message);
    }
  } finally {
    reporting = false;
  }
}

const reporter = setInterval(() => void flush(), 2000);
let lastDown = 0;
let lastUp = 0;
const ticker = setInterval(() => {
  console.log(
    `t=${since().toFixed(0)}s down=${(stats.downBytes / 1e6).toFixed(0)}MB(+${((stats.downBytes - lastDown) / 5e6).toFixed(0)}MB/s) committed=${(stats.upBytes / 1e6).toFixed(0)}MB(+${((stats.upBytes - lastUp) / 5e6).toFixed(0)}MB/s) commits=${stats.commits} pendingReport=${doneQueue.length}`,
  );
  lastDown = stats.downBytes;
  lastUp = stats.upBytes;
}, 5000);

const work = [];
for (const track of plan.tracks) {
  if (track.init) work.push(processItem(track, track.init, true));
  for (const item of track.segments) work.push(processItem(track, item, false));
}

const settled = await Promise.allSettled(work);
clearInterval(ticker);
const ok = settled.every((r) => r.status === "fulfilled") && !aborted;

for (let i = 0; i < 5 && (doneQueue.length > 0 || skippedQueue.length > 0); i++) {
  reporting = false;
  await flush();
  if (doneQueue.length > 0 || skippedQueue.length > 0) await new Promise((r) => setTimeout(r, 1000));
}
clearInterval(reporter);

const wall = since();
const summary = {
  downMb: Math.round(stats.downBytes / 1e6),
  upMb: Math.round(stats.upBytes / 1e6),
  wallSeconds: Number(wall.toFixed(1)),
  commits: stats.commits,
  cores: os.cpus().length,
  skipped: skippedNames.length,
};
console.log(`${ok ? "finished" : "failed"}: ${JSON.stringify(summary)}${ok ? "" : ` first error: ${failures[0] || abortReason}`}`);

try {
  await call("POST", `/api/dispatch/finish/${JOB}`, {
    ok,
    error: ok ? undefined : `${failures.length} segment(s) failed: ${failures[0] || abortReason || "unknown"}`.slice(0, 400),
    stats: summary,
    done: doneQueue.splice(0, doneQueue.length),
    skipped: skippedNames,
  });
} catch (e) {
  console.error(`could not report completion: ${e.message}`);
  process.exit(1);
}
process.exit(ok ? 0 : 1);
