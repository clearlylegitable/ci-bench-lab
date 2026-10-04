import { spawn } from "node:child_process";
import { createCipheriv, randomBytes } from "node:crypto";
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { unlink, writeFile } from "node:fs/promises";
import os from "node:os";

const MODE = process.env.MODE || "full";
const SEGMENTS = Number(process.env.SEGMENTS) || 150;
const UPLOAD_WORKERS = Number(process.env.UPLOAD_WORKERS) || 8;
const PER_JOB = Number(process.env.PER_JOB) || 96;
const GLOBAL = Number(process.env.GLOBAL) || 288;
const BATCH_FILES = Number(process.env.BATCH_FILES) || 128;
const BATCH_BYTES = (Number(process.env.BATCH_MB) || 512) * 1024 * 1024;
const MIN_FILES = Number(process.env.MIN_FILES) || 12;
const MIN_BYTES = (Number(process.env.MIN_MB) || 48) * 1024 * 1024;
const MAX_AGE_MS = Number(process.env.MAX_AGE_MS) || 700;
const RUN_TAG = process.env.RUN_TAG || randomBytes(3).toString("hex");

const SPEC = JSON.parse(process.env.SPEC_JSON);
const HEADERS = { ...SPEC.headers };
const jobs = SPEC.jobs.map((j, i) => ({ ...j, id: `m${i}` }));
const accounts = [
  { repo: process.env.HF_REPO_1, token: process.env.HF_TOKEN_1 },
  { repo: process.env.HF_REPO_2, token: process.env.HF_TOKEN_2 },
].filter((a) => a.repo && a.token);

class Semaphore {
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

const globalLimiter = new Semaphore(GLOBAL);

const T0 = performance.now();
const since = () => Number(((performance.now() - T0) / 1000).toFixed(1));
const marks = {};
const markFirst = (k) => {
  if (marks[k] === undefined) marks[k] = since();
};
const markLast = (k) => {
  marks[k] = since();
};
const pending = { downloading: 0, queuedForUpload: 0 };

class PyWorker {
  constructor() {
    this.proc = null;
    this.pending = new Map();
    this.id = 1;
    this.buf = "";
  }
  start() {
    if (this.proc) return;
    this.proc = spawn("python3", ["uploader.py"], { stdio: ["pipe", "pipe", "inherit"], env: { HF_XET_HIGH_PERFORMANCE: "1", ...process.env } });
    this.proc.stdout.on("data", (chunk) => {
      this.buf += chunk.toString();
      let i;
      while ((i = this.buf.indexOf("\n")) >= 0) {
        const line = this.buf.slice(0, i).trim();
        this.buf = this.buf.slice(i + 1);
        if (!line) continue;
        const m = JSON.parse(line);
        const p = this.pending.get(m.id);
        if (!p) continue;
        this.pending.delete(m.id);
        if (m.ok) p.resolve();
        else p.reject(new Error(m.error));
      }
    });
    this.proc.on("exit", () => {
      for (const p of this.pending.values()) p.reject(new Error("worker exited"));
      this.pending.clear();
      this.proc = null;
    });
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

const idleWorkers = Array.from({ length: UPLOAD_WORKERS }, () => new PyWorker());
const commitStats = { count: 0, commitMs: 0, bytes: 0, files: 0, minFiles: 1e9, maxFiles: 0 };
const uploadedByAccount = new Map();

class Scheduler {
  constructor() {
    this.queues = new Map();
    this.timer = null;
  }
  add(account, name, buf) {
    let q = this.queues.get(account.repo);
    if (!q) {
      q = { account, items: [], bytes: 0, firstAt: 0 };
      this.queues.set(account.repo, q);
    }
    if (q.items.length === 0) q.firstAt = performance.now();
    return new Promise((resolve, reject) => {
      q.items.push({ name, buf, resolve, reject });
      q.bytes += buf.length;
      this.pump();
    });
  }
  eligible(q, now) {
    if (q.items.length === 0) return false;
    return q.items.length >= MIN_FILES || q.bytes >= MIN_BYTES || now - q.firstAt >= MAX_AGE_MS;
  }
  pump() {
    const now = performance.now();
    while (idleWorkers.length > 0) {
      let best = null;
      for (const q of this.queues.values()) {
        if (this.eligible(q, now) && (!best || q.bytes > best.bytes)) best = q;
      }
      if (!best) break;
      const batch = [];
      let bytes = 0;
      while (best.items.length > 0 && batch.length < BATCH_FILES && bytes < BATCH_BYTES) {
        const item = best.items.shift();
        batch.push(item);
        bytes += item.buf.length;
      }
      best.bytes -= bytes;
      best.firstAt = now;
      void this.run(idleWorkers.pop(), best.account, batch);
    }
    if (!this.timer && [...this.queues.values()].some((q) => q.items.length > 0)) {
      this.timer = setTimeout(() => {
        this.timer = null;
        this.pump();
      }, 100);
    }
  }
  async run(worker, account, batch) {
    const temps = batch.map(() => `/tmp/up_${randomBytes(8).toString("hex")}`);
    const t0 = performance.now();
    try {
      await Promise.all(batch.map((b, i) => writeFile(temps[i], b.buf)));
      await worker.commit({ repo: account.repo, token: account.token, title: "t", items: batch.map((b, i) => ({ path: b.name, tempPath: temps[i] })) });
      commitStats.count++;
      commitStats.commitMs += performance.now() - t0;
      commitStats.files += batch.length;
      commitStats.minFiles = Math.min(commitStats.minFiles, batch.length);
      commitStats.maxFiles = Math.max(commitStats.maxFiles, batch.length);
      commitStats.bytes += batch.reduce((n, b) => n + b.buf.length, 0);
      const list = uploadedByAccount.get(account.repo) || [];
      for (const b of batch) list.push(b.name);
      uploadedByAccount.set(account.repo, list);
      for (const b of batch) b.resolve();
    } catch (e) {
      for (const b of batch) b.reject(e);
    } finally {
      await Promise.all(temps.map((t) => unlink(t).catch(() => undefined)));
      idleWorkers.push(worker);
      this.pump();
    }
  }
}

const scheduler = new Scheduler();

async function getText(url) {
  const res = await fetch(url, { headers: HEADERS, signal: AbortSignal.timeout(20000) });
  if (!res.ok) throw new Error(`playlist http ${res.status}`);
  return res.text();
}

function parseSegments(text, base) {
  const out = [];
  let init = null;
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (line.startsWith("#EXT-X-MAP:")) {
      const m = /URI="([^"]+)"/.exec(line);
      if (m) init = new URL(m[1], base).href;
    } else if (line && !line.startsWith("#")) {
      out.push(new URL(line, base).href);
    }
  }
  return { init, segs: out };
}

async function download(url) {
  for (let attempt = 1; attempt <= 6; attempt++) {
    try {
      const res = await fetch(url, { headers: HEADERS, signal: AbortSignal.timeout(30000) });
      if (!res.ok) throw new Error(`http ${res.status}`);
      return Buffer.from(await res.arrayBuffer());
    } catch (e) {
      if (attempt === 6) throw e;
      await new Promise((r) => setTimeout(r, 1000 * attempt));
    }
  }
}

function encrypt(buf, key, iv) {
  const c = createCipheriv("aes-128-cbc", key, iv);
  return Buffer.concat([c.update(buf), c.final()]);
}

const sizes = {};
const totals = { downBytes: 0, upBytes: 0 };

async function runTrack(job, trackIndex, jobLimiter, account, key, iv) {
  const track = job.tracks[trackIndex];
  const text = await getText(track.url);
  const { init, segs } = parseSegments(text, track.url);
  const urls = [...(init ? [init] : []), ...segs.slice(0, SEGMENTS)];
  const sizeKey = `${job.id}:${trackIndex}`;
  const nameOf = (i) => `_t${RUN_TAG}_${MODE}_${job.id}_${trackIndex}_${i}`;

  if (MODE === "upload") {
    const recorded = JSON.parse(readFileSync("/tmp/sizes.json", "utf8"))[sizeKey] || [];
    await Promise.all(recorded.map(async (size, i) => {
      await jobLimiter.acquire();
      await globalLimiter.acquire();
      try {
        await scheduler.add(account, nameOf(i), randomBytes(size));
        totals.upBytes += size;
      } finally {
        globalLimiter.release();
        jobLimiter.release();
      }
    }));
    return;
  }

  sizes[sizeKey] = new Array(urls.length).fill(0);
  await Promise.all(urls.map(async (url, i) => {
    await jobLimiter.acquire();
    await globalLimiter.acquire();
    try {
      markFirst("firstSegmentRequested");
      pending.downloading++;
      const raw = await download(url);
      pending.downloading--;
      markFirst("firstSegmentDownloaded");
      markLast("lastSegmentDownloaded");
      totals.downBytes += raw.length;
      sizes[sizeKey][i] = raw.length;
      if (MODE === "full") {
        const enc = encrypt(raw, key, iv);
        pending.queuedForUpload++;
        await scheduler.add(account, nameOf(i), enc);
        pending.queuedForUpload--;
        markFirst("firstSegmentCommitted");
        markLast("lastSegmentCommitted");
        totals.upBytes += enc.length;
      }
    } finally {
      globalLimiter.release();
      jobLimiter.release();
    }
  }));
}

function cpuSnapshot() {
  return os.cpus().reduce((a, c) => {
    a.user += c.times.user + c.times.nice;
    a.sys += c.times.sys + c.times.irq;
    a.idle += c.times.idle;
    return a;
  }, { user: 0, sys: 0, idle: 0 });
}

const cpu0 = cpuSnapshot();
const t0 = performance.now();
const finished = {};

let lastDown = 0;
let lastUp = 0;
const ticker = setInterval(() => {
  const c = cpuSnapshot();
  const t = (performance.now() - t0) / 1000;
  console.log(
    `t=${t.toFixed(0)}s down=${(totals.downBytes / 1e6).toFixed(0)}MB (+${((totals.downBytes - lastDown) / 5e6).toFixed(0)}MB/s) committed=${(totals.upBytes / 1e6).toFixed(0)}MB (+${((totals.upBytes - lastUp) / 5e6).toFixed(0)}MB/s) dlInflight=${pending.downloading} waitingCommit=${pending.queuedForUpload} commits=${commitStats.count} cpuIdle=${Math.round(((c.idle - cpu0.idle) / ((c.user - cpu0.user) + (c.sys - cpu0.sys) + (c.idle - cpu0.idle))) * 100)}%`,
  );
  lastDown = totals.downBytes;
  lastUp = totals.upBytes;
}, 5000);

await Promise.all(jobs.map(async (job, idx) => {
  const account = accounts[idx % accounts.length];
  const jobLimiter = new Semaphore(PER_JOB);
  const key = randomBytes(16);
  const iv = randomBytes(16);
  await Promise.all(job.tracks.map((_, ti) => runTrack(job, ti, jobLimiter, account, key, iv)));
  finished[job.id] = Number(((performance.now() - t0) / 1000).toFixed(1));
}));

const wall = (performance.now() - t0) / 1000;
const cpu1 = cpuSnapshot();
const dU = cpu1.user - cpu0.user;
const dS = cpu1.sys - cpu0.sys;
const dI = cpu1.idle - cpu0.idle;
const dT = dU + dS + dI;

if (MODE === "download") writeFileSync("/tmp/sizes.json", JSON.stringify(sizes));

const prior = existsSync("/tmp/uploaded.json") ? JSON.parse(readFileSync("/tmp/uploaded.json", "utf8")) : {};
for (const [repo, names] of uploadedByAccount.entries()) prior[repo] = [...(prior[repo] || []), ...names];
writeFileSync("/tmp/uploaded.json", JSON.stringify(prior));

const result = {
  mode: MODE,
  workers: UPLOAD_WORKERS,
  cores: os.cpus().length,
  memGB: Number((os.totalmem() / 1e9).toFixed(1)),
  wallSeconds: Number(wall.toFixed(1)),
  downMB: Number((totals.downBytes / 1e6).toFixed(0)),
  upMB: Number((totals.upBytes / 1e6).toFixed(0)),
  downMBps: Number((totals.downBytes / 1e6 / wall).toFixed(1)),
  upMBps: Number((totals.upBytes / 1e6 / wall).toFixed(1)),
  perMovieFinishSeconds: finished,
  timeline: marks,
  cpuPct: { user: Math.round((dU / dT) * 100), sys: Math.round((dS / dT) * 100), idle: Math.round((dI / dT) * 100) },
  commits: commitStats.count
    ? { count: commitStats.count, avgFiles: Number((commitStats.files / commitStats.count).toFixed(1)), minFiles: commitStats.minFiles, maxFiles: commitStats.maxFiles, avgMB: Number((commitStats.bytes / commitStats.count / 1e6).toFixed(0)), avgCommitSec: Number((commitStats.commitMs / commitStats.count / 1000).toFixed(1)) }
    : null,
};
clearInterval(ticker);
clearInterval(ticker);
console.log(`RESULT_JSON ${JSON.stringify(result)}`);
process.exit(0);
