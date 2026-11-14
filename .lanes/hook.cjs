"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// packages/hook/src/hook.ts
var hook_exports = {};
__export(hook_exports, {
  checkoutLock: () => checkoutLock,
  contextOutput: () => contextOutput,
  defaultOutput: () => defaultOutput,
  editSpan: () => editSpan,
  editedFiles: () => editedFiles,
  fromAgent: () => fromAgent,
  loadConfig: () => loadConfig,
  loadState: () => loadState,
  predictFiles: () => predictFiles,
  pruneSessions: () => pruneSessions,
  redactSecrets: () => redactSecrets,
  staleDependencies: () => staleDependencies,
  updateState: () => updateState
});
module.exports = __toCommonJS(hook_exports);
var import_node_child_process3 = require("node:child_process");
var crypto4 = __toESM(require("node:crypto"), 1);
var fs6 = __toESM(require("node:fs"), 1);
var http = __toESM(require("node:http"), 1);
var os2 = __toESM(require("node:os"), 1);
var path6 = __toESM(require("node:path"), 1);

// packages/hook/src/redact.ts
var R = "[redacted]";
var LONG = "(?:password|passwd|passphrase|secret|token|api[_ -]?key|access[_-]?key|private[_-]?key|credentials?)";
var WORD = `(?:${LONG}|pass|pwd|pw)`;
var NAME = String.raw`(?:[\w-]*${LONG}[\w-]*|\b(?:pass|pwd|pw)\b)`;
var VALUE = String.raw`(?:"[^"]*"|'[^']*'|\S+)`;
var shaped = (v) => /^["']|\d|[+/=@#$%^&*~_-]|.[A-Z]/.test(v);
function redactSecrets(text) {
  return text.replace(/-----BEGIN [A-Z ]*-----.*/gs, R).replace(/\b(?:ssh-(?:rsa|ed25519|dss)|ecdsa-sha2-\S+)\s+\S+/g, R).replace(/\beyJ[\w-]+\.[\w-]+\.[\w-]*/g, R).replace(/https:\/\/hooks\.slack\.com\/services\/\S+/g, R).replace(/\bAIza[\w-]{35}\b/g, R).replace(/\bGOCSPX-[\w-]{10,}/g, R).replace(/\bnpm_[A-Za-z0-9]{30,}\b/g, R).replace(/\b(sk|pk|rk)[-_](live|test|ant|proj)?[-_]?[A-Za-z0-9_-]{16,}/g, R).replace(/\b(ghp|gho|ghu|ghs|ghr|github_pat|glpat|xox[abprs])[-_][A-Za-z0-9_-]{16,}/g, R).replace(/\b(AKIA|ASIA)[0-9A-Z]{16}\b/g, R).replace(/\b(Authorization\s*:\s*(?:Bearer|Basic|Token)\s+)\S+/gi, `$1${R}`).replace(/\b(Bearer|Basic)\s+([\w.~+/=-]{8,})/gi, (m, scheme, v) => /[\d.~+/=_-]/.test(v) ? `${scheme} ${R}` : m).replace(/\b(mysql|mysqldump|mariadb)\b([^\n]*?\s-p)\S+/g, `$1$2${R}`).replace(new RegExp(String.raw`(${NAME})(\s*=\s*)${VALUE}`, "gi"), `$1$2${R}`).replace(new RegExp(String.raw`(${NAME})(\s*:\s*)(${VALUE})`, "gi"), (m, name, sep2, v) => shaped(v) ? `${name}${sep2}${R}` : m).replace(new RegExp(String.raw`(--?[\w-]*(?:password|passwd|passphrase|pass|pwd)[\w-]*\s+)\S+`, "gi"), `$1${R}`).replace(new RegExp(String.raw`(--?${NAME}\s+)(\S+)`, "gi"), (m, flag, v) => shaped(v) ? `${flag}${R}` : m).replace(new RegExp(String.raw`(\b${WORD}\s+is\s+)\S+`, "gi"), `$1${R}`).replace(/(\bkey\s+is\s+)(\S+)/gi, (m, pre, v) => shaped(v) ? `${pre}${R}` : m).replace(/(\bcredentials?\s+(?:are|is)\s+)\S+/gi, `$1${R}`).replace(new RegExp(String.raw`(\S+)(\s+is\s+the\s+(?:password|passphrase|secret|token|key)\b)`, "gi"), (m, v, rest) => shaped(v) ? `${R}${rest}` : m).replace(new RegExp(String.raw`(\b(?:password|passwd|passphrase|pass|pwd|pw)\s+)(\S{4,})`, "gi"), (m, pre, v) => shaped(v) ? `${pre}${R}` : m).replace(/:\/\/[^/\s:@]+:[^/\s@]+@/g, `://${R}@`).replace(/\b[A-Za-z0-9+_-]{40,}\b/g, (m) => /\d/.test(m) && /[A-Za-z]/.test(m) && !/^[0-9a-f]{40}$|^[0-9a-f]{64}$/i.test(m) ? R : m);
}

// packages/hook/src/agents/antigravity.ts
function fromAntigravity(raw) {
  const call = raw.toolCall ?? {};
  const file2 = typeof call.args?.TargetFile === "string" ? call.args.TargetFile : void 0;
  return {
    session_id: raw.conversationId,
    cwd: raw.workspacePaths?.[0],
    ...typeof call.name === "string" && { tool_name: call.name === "write_to_file" ? "Write" : call.name, tool_input: { file_path: file2 } }
  };
}
function antigravityContext(hookEventName, text) {
  return hookEventName === "SessionStart" ? { injectSteps: [{ ephemeralMessage: text }] } : void 0;
}

// packages/hook/src/gitops.ts
var import_node_child_process2 = require("node:child_process");
var crypto2 = __toESM(require("node:crypto"), 1);
var fs3 = __toESM(require("node:fs"), 1);
var path3 = __toESM(require("node:path"), 1);

// packages/core/src/local.ts
var crypto = __toESM(require("node:crypto"), 1);
var fs = __toESM(require("node:fs"), 1);
var os = __toESM(require("node:os"), 1);
var path = __toESM(require("node:path"), 1);
function lanesHome() {
  return process.env.LANES_HOME ?? path.join(os.homedir(), ".lanes");
}
function privateDir(...sub) {
  const dir = path.join(lanesHome(), ...sub);
  fs.mkdirSync(dir, { recursive: true, mode: 448 });
  const st = fs.lstatSync(dir);
  if (!st.isDirectory() || process.getuid && st.uid !== process.getuid()) {
    throw new Error(`${dir} is not a private directory owned by this user`);
  }
  if (process.platform !== "win32" && (st.mode & 63) !== 0) fs.chmodSync(dir, 448);
  return dir;
}
var sleepSync = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
function readToken(file2) {
  try {
    const token = fs.readFileSync(file2, "utf8").trim();
    if (token && process.platform !== "win32" && (fs.statSync(file2).mode & 63) !== 0) fs.chmodSync(file2, 384);
    return token || null;
  } catch {
    return null;
  }
}
function loadOrCreateToken() {
  if (process.env.LANES_TOKEN) return process.env.LANES_TOKEN;
  const file2 = path.join(privateDir(), "token");
  const existing = readToken(file2);
  if (existing) return existing;
  const token = crypto.randomBytes(32).toString("base64url");
  const tmp = `${file2}.${process.pid}.${crypto.randomBytes(4).toString("hex")}`;
  fs.writeFileSync(tmp, token, { mode: 384 });
  try {
    if (fs.existsSync(file2) && fs.statSync(file2).size === 0) fs.renameSync(tmp, file2);
    else fs.linkSync(tmp, file2);
  } catch {
    if (!fs.existsSync(file2)) fs.renameSync(tmp, file2);
  } finally {
    fs.rmSync(tmp, { force: true });
  }
  for (let i = 0; i < 40; i++) {
    const winner = readToken(file2);
    if (winner) return winner;
    sleepSync(25);
  }
  return token;
}
function alive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return err.code === "EPERM";
  }
}
function ownerGone(dir) {
  try {
    const read = () => Number(fs.readFileSync(path.join(dir, "owner"), "utf8"));
    const pid = read();
    return Number.isInteger(pid) && pid > 0 && !alive(pid) && read() === pid;
  } catch {
    return false;
  }
}
function removeLock(dir) {
  const aside = `${dir}.del-${process.pid}-${Date.now()}`;
  try {
    fs.renameSync(dir, aside);
  } catch (err) {
    if (err.code === "ENOENT") return;
    try {
      fs.rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 20 });
    } catch {
    }
    return;
  }
  try {
    fs.rmSync(aside, { recursive: true, force: true, maxRetries: 10, retryDelay: 20 });
  } catch {
  }
}
function takeOverIfDead(dir, staleMs) {
  const gate = `${dir}.take`;
  try {
    fs.mkdirSync(gate);
  } catch {
    try {
      if (Date.now() - fs.statSync(gate).mtimeMs > 5e3) fs.rmSync(gate, { recursive: true, force: true });
    } catch {
    }
    return;
  }
  try {
    if (ownerGone(dir) || Date.now() - fs.statSync(dir).mtimeMs > staleMs) removeLock(dir);
  } catch {
  } finally {
    fs.rmSync(gate, { recursive: true, force: true });
  }
}
async function withLock(name, fn, waitMs = 5e3, staleMs = 9e4) {
  let lock = null;
  try {
    const dir = path.join(privateDir("locks"), name.replace(/[^\w-]/g, "_"));
    for (const started = Date.now(); ; ) {
      try {
        const mine = `${dir}.new-${process.pid}-${Date.now()}`;
        fs.mkdirSync(mine);
        try {
          fs.writeFileSync(path.join(mine, "owner"), String(process.pid));
          fs.renameSync(mine, dir);
        } catch (err) {
          fs.rmSync(mine, { recursive: true, force: true });
          throw err;
        }
        lock = dir;
        break;
      } catch {
        takeOverIfDead(dir, staleMs);
        if (Date.now() - started > waitMs) break;
        await new Promise((r) => setTimeout(r, 20));
      }
    }
  } catch {
  }
  try {
    return await fn();
  } finally {
    if (lock) removeLock(lock);
  }
}
function loadConfig() {
  let file2 = {};
  try {
    const parsed = JSON.parse(fs.readFileSync(path.join(lanesHome(), "config.json"), "utf8"));
    if (parsed && typeof parsed === "object") file2 = parsed;
  } catch {
  }
  const text = (key) => typeof file2[key] === "string" && file2[key] ? file2[key] : void 0;
  return {
    serverUrl: (process.env.LANES_SERVER ?? text("serverUrl") ?? "http://127.0.0.1:4317").replace(/\/+$/, ""),
    user: process.env.LANES_USER ?? text("user"),
    email: process.env.LANES_EMAIL ?? text("email"),
    token: process.env.LANES_TOKEN ?? text("token")
  };
}
var isLoopback = (url) => ["127.0.0.1", "localhost", "[::1]"].includes(new URL(url).hostname);
function isLocalServer(url) {
  let host;
  try {
    if (isLoopback(url)) return true;
    host = new URL(url).hostname.replace(/^\[|\]$/g, "").toLowerCase();
  } catch {
    return false;
  }
  const me = os.hostname().toLowerCase();
  if (host === me || host === `${me}.local`) return true;
  return Object.values(os.networkInterfaces()).some((list) => (list ?? []).some((a) => a.address.toLowerCase() === host));
}
function tokenFor(config) {
  return config.token ?? (isLocalServer(config.serverUrl) ? loadOrCreateToken() : void 0);
}

// packages/core/src/repo.ts
var import_node_child_process = require("node:child_process");
var fs2 = __toESM(require("node:fs"), 1);
var path2 = __toESM(require("node:path"), 1);
var byCheckout = /* @__PURE__ */ new Map();
function findRepo(target) {
  let dir = path2.resolve(target);
  while (!isDirectory(dir)) {
    const parent = path2.dirname(dir);
    if (parent === dir) return null;
    dir = parent;
  }
  for (let d = dir; ; d = path2.dirname(d)) {
    if (fs2.existsSync(path2.join(d, ".git"))) return repoAt(d);
    if (path2.dirname(d) === d) return null;
  }
}
function relativePath(repo, file2) {
  const rel = path2.relative(repo.root, path2.resolve(file2));
  if (!rel || rel === ".." || rel.startsWith(".." + path2.sep) || path2.isAbsolute(rel)) return null;
  return rel.split(path2.sep).join("/");
}
function locate(file2) {
  const real = onDisk(file2);
  const repo = findRepo(real);
  const rel = repo && relativePath(repo, real);
  return repo && rel ? { repo, path: rel } : null;
}
function onDisk(file2) {
  const rest = [];
  let dir = path2.resolve(file2);
  for (; ; ) {
    try {
      return path2.join(fs2.realpathSync.native(dir), ...rest);
    } catch {
      const parent = path2.dirname(dir);
      if (parent === dir) return path2.resolve(file2);
      rest.unshift(path2.basename(dir));
      dir = parent;
    }
  }
}
function normalizeRemote(url) {
  let u = url.trim();
  const scp = /^[\w.-]+@([^:/]+):(?!\/)(.+)$/.exec(u);
  if (scp) {
    u = `${scp[1]}/${scp[2]}`;
  } else {
    u = u.replace(/^[a-z][a-z0-9+.-]*:\/\//i, "").replace(/^[^@/]*@/, "").replace(/^([^/:]+):\d+\//, "$1/");
  }
  return u.replace(/\.git$/i, "").replace(/\/+$/, "").toLowerCase();
}
var diskCache = () => path2.join(privateDir("cache"), "repos.json");
function readDiskCache() {
  try {
    return JSON.parse(fs2.readFileSync(diskCache(), "utf8"));
  } catch {
    return {};
  }
}
function mtime(file2) {
  try {
    return fs2.statSync(file2).mtimeMs;
  } catch {
    return null;
  }
}
function writeDiskCache(disk) {
  try {
    const file2 = diskCache();
    const tmp = `${file2}.${process.pid}`;
    fs2.writeFileSync(tmp, JSON.stringify(disk), { mode: 384 });
    fs2.renameSync(tmp, file2);
  } catch {
  }
}
function repoAt(checkout) {
  const cached = byCheckout.get(checkout);
  if (cached !== void 0) return cached;
  const disk = readDiskCache();
  const hit = disk[checkout];
  if (hit && mtime(hit.config) === hit.mtimeMs) {
    byCheckout.set(checkout, hit.info);
    return hit.info;
  }
  const info = resolveRepo(checkout, disk);
  byCheckout.set(checkout, info);
  return info;
}
function gitDirOf(root) {
  try {
    const dot = path2.join(root, ".git");
    if (fs2.statSync(dot).isDirectory()) return dot;
    const m = /^gitdir:\s*(.+?)\s*$/m.exec(fs2.readFileSync(dot, "utf8"));
    return m ? path2.resolve(root, m[1]) : null;
  } catch {
    return null;
  }
}
function hasOrigin(root) {
  try {
    const gitDir = gitDirOf(root);
    if (!gitDir) return null;
    const common = fs2.statSync(path2.join(root, ".git")).isDirectory() ? gitDir : path2.resolve(gitDir, fs2.readFileSync(path2.join(gitDir, "commondir"), "utf8").trim());
    const config = fs2.readFileSync(path2.join(common, "config"), "utf8");
    if (/^\s*\[\s*(include|worktree)/im.test(config)) return null;
    return /^\s*\[\s*remote\s+"origin"\s*\]/im.test(config);
  } catch {
    return null;
  }
}
function originFromConfig(config) {
  if (/^\s*\[\s*(include|worktree)|^\s*(bare\s*=\s*true|worktree\s*=)/im.test(config)) return null;
  let inOrigin = false;
  let url = null;
  for (const line of config.split(/\r?\n/)) {
    const section = /^\s*\[\s*([^\s\]"]+)(?:\s+"([^"\\]*)")?\s*\]/.exec(line);
    if (section) {
      inOrigin = section[1].toLowerCase() === "remote" && section[2] === "origin";
      continue;
    }
    const value = inOrigin ? /^\s*url\s*=\s*(.*?)\s*$/i.exec(line)?.[1] : void 0;
    if (value === void 0) continue;
    if (/["#;]/.test(value)) return null;
    const marked = value.replace(/\\\\/g, "");
    if (marked.includes("\\")) return null;
    url = marked.replace(/\u0001/g, "\\");
  }
  return url;
}
function resolveFast(checkout) {
  try {
    const gitDir = gitDirOf(checkout);
    if (!gitDir) return null;
    const common = fs2.statSync(path2.join(checkout, ".git")).isDirectory() ? gitDir : path2.resolve(gitDir, fs2.readFileSync(path2.join(gitDir, "commondir"), "utf8").trim());
    const config = path2.join(common, "config");
    const origin = originFromConfig(fs2.readFileSync(config, "utf8"));
    if (!origin) return null;
    const root = onDisk(checkout);
    return { info: { id: normalizeRemote(origin), root, label: path2.basename(root) }, config };
  } catch {
    return null;
  }
}
function resolveRepo(checkout, disk) {
  const fast = resolveFast(checkout);
  const fastMtime = fast && mtime(fast.config);
  if (fast && fastMtime !== null) {
    writeDiskCache({ ...disk, [checkout]: { info: fast.info, config: fast.config, mtimeMs: fastMtime } });
    return fast.info;
  }
  let info = null;
  const out = git(checkout, ["rev-parse", "--path-format=absolute", "--show-toplevel", "--git-common-dir"]);
  if (out) {
    const [top, commonDir] = out.split(/\r?\n/);
    const root = onDisk(top);
    const origin = git(checkout, ["config", "--get", "remote.origin.url"]);
    const mainCheckout = path2.dirname(path2.resolve(commonDir));
    info = { id: origin ? normalizeRemote(origin) : normalizeLocal(mainCheckout), root, label: path2.basename(root) };
    const config = path2.join(path2.resolve(commonDir), "config");
    const mtimeMs = mtime(config);
    if (mtimeMs !== null) writeDiskCache({ ...disk, [checkout]: { info, config, mtimeMs } });
  }
  return info;
}
function normalizeLocal(dir) {
  const p = dir.split(path2.sep).join("/");
  return process.platform === "win32" ? p.toLowerCase() : p;
}
function isDirectory(p) {
  try {
    return fs2.statSync(p).isDirectory();
  } catch {
    return false;
  }
}
function git(dir, args) {
  try {
    return (0, import_node_child_process.execFileSync)("git", ["-C", dir, ...args], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
      windowsHide: true
    }).trim() || null;
  } catch {
    return null;
  }
}
function teamContextOn(root) {
  try {
    return JSON.parse(fs2.readFileSync(path2.join(root, ".lanes", "team.json"), "utf8")).context === true;
  } catch {
    return false;
  }
}

// packages/core/src/ranges.ts
var MAX_RANGES = 50;
function mergeRanges(list, max = MAX_RANGES) {
  const out = [];
  for (const [a, b] of [...list].sort((x, y) => x[0] - y[0])) {
    const last = out[out.length - 1];
    if (last && a <= last[1] + 1) last[1] = Math.max(last[1], b);
    else out.push([a, b]);
  }
  if (out.length > max) out.splice(max - 1, out.length, [out[max - 1][0], out[out.length - 1][1]]);
  return out;
}

// packages/hook/src/gitops.ts
var INTEGRATION_BRANCH = process.env.LANES_INTEGRATION ?? "lanes/main";
function remoteOf(cwd) {
  const env = process.env.LANES_REMOTE;
  if (env !== void 0) return env || null;
  const known = hasOrigin(cwd);
  if (known !== null) return known ? "origin" : null;
  return tryGit(cwd, ["remote", "get-url", "origin"]) === null ? null : "origin";
}
function integrationRef(cwd) {
  const REMOTE = remoteOf(cwd);
  if (!REMOTE) {
    const sha2 = tryGit(cwd, ["rev-parse", "--verify", INTEGRATION_BRANCH]);
    return sha2 === null ? null : { ref: INTEGRATION_BRANCH, sha: sha2 };
  }
  const tracking = `refs/remotes/${REMOTE}/${INTEGRATION_BRANCH}`;
  const advertised = netGit(cwd, ["ls-remote", "--refs", REMOTE, `refs/heads/${INTEGRATION_BRANCH}`]);
  const tip = advertised?.split(/\s/)[0];
  if (!tip) return null;
  let sha = tryGit(cwd, ["rev-parse", "--verify", "--quiet", tracking]);
  if (sha !== tip) {
    if (netGit(cwd, ["fetch", "--quiet", REMOTE, `+refs/heads/${INTEGRATION_BRANCH}:${tracking}`]) === null) return null;
    sha = tryGit(cwd, ["rev-parse", "--verify", "--quiet", tracking]);
  }
  return sha === null ? null : { ref: tracking, sha };
}
var MAX_BUFFER = 512 * 1024 * 1024;
var netTimeout = (kind) => Number(process.env.LANES_NET_TIMEOUT_MS ?? (kind === "push" ? 2e4 : 1e4));
var LANES_GIT = ["-c", "core.longpaths=true", "-c", "maintenance.auto=false", "-c", "gc.auto=0"];
function gitRaw(cwd, args, env = {}, timeout) {
  return (0, import_node_child_process2.execFileSync)("git", LANES_GIT.concat(args), {
    cwd,
    timeout,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    windowsHide: true,
    maxBuffer: MAX_BUFFER,
    env: { ...process.env, ...env }
  });
}
function git2(cwd, args, env = {}) {
  return gitRaw(cwd, args, env).trim();
}
function gitPaths(cwd, args) {
  return gitRaw(cwd, [...args, "-z"]).split("\0").filter(Boolean);
}
function listFiles(cwd) {
  try {
    return gitPaths(cwd, ["ls-files"]);
  } catch {
    return [];
  }
}
var originDownFile = (cwd) => path3.join(privateDir("cache"), `origin-down-${crypto2.createHash("sha1").update(cwd).digest("hex").slice(0, 16)}.json`);
function originDownUntil(cwd) {
  try {
    return JSON.parse(fs3.readFileSync(originDownFile(cwd), "utf8"));
  } catch {
    return { until: 0, failures: 0 };
  }
}
function markOrigin(cwd, reachable) {
  try {
    const file2 = originDownFile(cwd);
    if (reachable) return void fs3.rmSync(file2, { force: true });
    const { failures } = originDownUntil(cwd);
    fs3.writeFileSync(file2, JSON.stringify({ until: Date.now() + Math.min(3e5, 3e4 * 2 ** failures), failures: failures + 1 }), { mode: 384 });
  } catch {
  }
}
function unreachable(err) {
  const e = err;
  return e.code === "ETIMEDOUT" || e.signal === "SIGTERM" || /could not resolve host|connection (refused|timed out|reset)|network is unreachable|failed to connect|operation timed out/i.test(e.stderr ?? "");
}
function netGit(cwd, args) {
  if (Date.now() < originDownUntil(cwd).until) return null;
  try {
    const out = gitRaw(cwd, args, {}, netTimeout(args[0] === "push" ? "push" : "fetch")).trim();
    markOrigin(cwd, true);
    return out;
  } catch (err) {
    if (unreachable(err)) markOrigin(cwd, false);
    return null;
  }
}
function tryGit(cwd, args, env = {}) {
  try {
    return git2(cwd, args, env);
  } catch {
    return null;
  }
}
function runGit(cwd, args, env = {}) {
  try {
    return { ok: true, out: git2(cwd, args, env) };
  } catch (err) {
    const e = err;
    return { ok: false, error: `${e.stderr ?? ""}${e.stdout ?? ""}`.trim().slice(0, 500) || e.message };
  }
}
function identityEnv(who) {
  return {
    GIT_CONFIG_COUNT: "1",
    GIT_CONFIG_KEY_0: "commit.gpgsign",
    GIT_CONFIG_VALUE_0: "false",
    GIT_AUTHOR_NAME: who.name,
    GIT_AUTHOR_EMAIL: who.email,
    GIT_COMMITTER_NAME: who.name,
    GIT_COMMITTER_EMAIL: who.email
  };
}
var MAX_PUBLISHED = 2e3;
var publishedFile = (cwd) => {
  const dir = gitDirOf(cwd);
  return dir ? path3.join(dir, "lanes-published") : null;
};
function publishedSet(cwd) {
  try {
    return new Set(fs3.readFileSync(publishedFile(cwd), "utf8").split("\n").filter(Boolean));
  } catch {
    return /* @__PURE__ */ new Set();
  }
}
function rememberPublished(cwd, hashes) {
  try {
    const file2 = publishedFile(cwd);
    if (!file2 || hashes.length === 0) return;
    const all = [...publishedSet(cwd), ...hashes];
    if (all.length > MAX_PUBLISHED) fs3.writeFileSync(file2, all.slice(-MAX_PUBLISHED / 2).join("\n") + "\n");
    else fs3.appendFileSync(file2, hashes.join("\n") + "\n");
  } catch {
  }
}
var MAX_REPLAY = () => Number(process.env.LANES_MAX_REPLAY ?? 100);
var MAX_REBASE = 20;
var blockedFile = (cwd) => {
  const dir = gitDirOf(cwd);
  return dir ? path3.join(dir, "lanes-blocked") : null;
};
function blockedSet(cwd) {
  try {
    return new Set(fs3.readFileSync(blockedFile(cwd), "utf8").split("\n").filter(Boolean));
  } catch {
    return /* @__PURE__ */ new Set();
  }
}
function rememberBlocked(cwd, hashes) {
  try {
    const file2 = blockedFile(cwd);
    if (file2 && (hashes.length > 0 || fs3.existsSync(file2))) fs3.writeFileSync(file2, hashes.slice(-MAX_PUBLISHED).join("\n") + "\n");
  } catch {
  }
}
function isOwnPublished(cwd, commit) {
  return publishedSet(cwd).has(commit);
}
function onlyOwnIncoming(cwd, sha) {
  const own = publishedSet(cwd);
  if (own.size === 0) return false;
  try {
    const incoming = gitRaw(cwd, ["rev-list", `HEAD..${sha}`]).split("\n").filter(Boolean);
    return incoming.length > 0 && incoming.every((h) => own.has(h));
  } catch {
    return false;
  }
}
function isAncestor(cwd, commit, of = "HEAD") {
  return tryGit(cwd, ["merge-base", "--is-ancestor", "--end-of-options", commit, of]) !== null;
}
function fileHash(file2) {
  try {
    return crypto2.createHash("sha1").update(fs3.readFileSync(file2)).digest("hex");
  } catch {
    return null;
  }
}
function resolveConflicts(text, onlyInsertions = false) {
  const out = [];
  let part = "text";
  let ours = [], base = [], theirs = [];
  for (const line of text.split("\n")) {
    if (part === "text") {
      if (line.startsWith("<<<<<<< ")) {
        part = "ours";
        ours = [];
        base = [];
        theirs = [];
      } else out.push(line);
    } else if (part === "ours" && line.startsWith("||||||| ")) part = "base";
    else if (part === "base" && line.startsWith("=======")) part = "theirs";
    else if (part === "theirs" && line.startsWith(">>>>>>> ")) {
      if (onlyInsertions && base.length > 0) return null;
      const left = [...base];
      const added = theirs.filter((l) => {
        const i = left.indexOf(l);
        if (i < 0) return true;
        left.splice(i, 1);
        return false;
      });
      out.push(...ours, ...added.filter((l) => !ours.includes(l)));
      part = "text";
    } else (part === "ours" ? ours : part === "base" ? base : theirs).push(line);
  }
  return out.join("\n");
}
function mergeStaleWrite(cwd, rel, turnStart) {
  const show = (rev) => {
    try {
      return (0, import_node_child_process2.execFileSync)("git", LANES_GIT.concat(["show", "--end-of-options", `${rev}:${rel}`]), { cwd, stdio: ["ignore", "pipe", "pipe"], windowsHide: true, maxBuffer: MAX_BUFFER });
    } catch {
      return null;
    }
  };
  const theirs = show("HEAD");
  const base = /^[0-9a-f]{40,64}$/.test(turnStart) ? show(turnStart) : null;
  if (!theirs || !base || theirs.equals(base)) return "same";
  let ours;
  try {
    ours = fs3.readFileSync(path3.join(cwd, rel));
  } catch {
    return "same";
  }
  if (ours.equals(theirs) || ours.includes(0) || theirs.includes(0)) return "same";
  const merged = mergeBoth(ours, base, theirs);
  if (!merged) return "conflict";
  if (merged.equals(ours)) return "same";
  fs3.writeFileSync(path3.join(cwd, rel), merged);
  return "merged";
}
function mergeBoth(ours, base, theirs, onlyInsertions = false) {
  if (ours.includes(0) || base.includes(0) || theirs.includes(0)) return null;
  const dir = fs3.mkdtempSync(path3.join(privateDir("tmp"), "merge-"));
  try {
    for (const [name, data] of [["ours", ours], ["base", base], ["theirs", theirs]]) fs3.writeFileSync(path3.join(dir, name), data, { mode: 384 });
    const run2 = (0, import_node_child_process2.spawnSync)("git", ["merge-file", "-p", "--diff3", "-L", "ours", "-L", "base", "-L", "theirs", "ours", "base", "theirs"], { cwd: dir, windowsHide: true, maxBuffer: MAX_BUFFER });
    if (run2.status === null || run2.status < 0 || run2.status > 127) return null;
    if (run2.status === 0) return run2.stdout;
    const resolved = resolveConflicts(run2.stdout.toString("latin1"), onlyInsertions);
    return resolved === null ? null : Buffer.from(resolved, "latin1");
  } finally {
    fs3.rmSync(dir, { recursive: true, force: true });
  }
}
function blobAt(cwd, rev, p) {
  try {
    return (0, import_node_child_process2.execFileSync)("git", LANES_GIT.concat(["show", "--end-of-options", `${rev}:${p}`]), { cwd, stdio: ["ignore", "pipe", "pipe"], windowsHide: true, maxBuffer: MAX_BUFFER });
  } catch {
    return null;
  }
}
function keepBoth(ours, base, theirs) {
  return ours && base && theirs ? mergeBoth(ours, base, theirs, true) : null;
}
var INFLIGHT_MS = 10 * 6e4;
var sha16 = (text) => crypto2.createHash("sha1").update(text).digest("hex").slice(0, 16);
var inflightDir = (cwd) => privateDir("inflight", sha16(cwd));
function markEditInFlight(cwd, session, rel, on) {
  const dir = inflightDir(cwd);
  if (on && rel !== void 0) return fs3.writeFileSync(path3.join(dir, `${sha16(session)}-${sha16(rel)}`), "");
  for (const name of fs3.readdirSync(dir)) {
    if (name.startsWith(`${sha16(session)}-`) && (rel === void 0 || name.endsWith(`-${sha16(rel)}`))) fs3.rmSync(path3.join(dir, name), { force: true });
  }
}
function editInFlight(cwd, self) {
  const dir = inflightDir(cwd);
  for (const name of fs3.readdirSync(dir)) {
    if (self !== void 0 && name.startsWith(`${sha16(self)}-`)) continue;
    try {
      if (Date.now() - fs3.statSync(path3.join(dir, name)).mtimeMs < INFLIGHT_MS) return true;
      fs3.rmSync(path3.join(dir, name), { force: true });
    } catch {
    }
  }
  return false;
}
function syncFromIntegration(cwd, who, session) {
  const target = integrationRef(cwd);
  const REMOTE = remoteOf(cwd);
  if (target === null) return REMOTE ? { ok: false, error: `cannot fetch ${INTEGRATION_BRANCH} from ${REMOTE}` } : { ok: true };
  if (isAncestor(cwd, target.sha) || onlyOwnIncoming(cwd, target.sha)) return { ok: true };
  const res = rebaseOnto(cwd, target.ref, identityEnv(who), true, false, session);
  return res.ok ? { ok: true } : { ok: false, error: res.error, ...res.inFlight && { skipped: true } };
}
function operationInProgress(cwd) {
  const gitDir = gitDirOf(cwd);
  if (gitDir) {
    for (const [name, label] of [["rebase-merge", "rebase"], ["rebase-apply", "rebase"], ["MERGE_HEAD", "merge"], ["CHERRY_PICK_HEAD", "cherry-pick"], ["REVERT_HEAD", "revert"]]) {
      if (fs3.existsSync(path3.join(gitDir, name))) return label;
    }
    return null;
  }
  for (const [name, label] of [["rebase-merge", "rebase"], ["rebase-apply", "rebase"], ["MERGE_HEAD", "merge"], ["CHERRY_PICK_HEAD", "cherry-pick"], ["REVERT_HEAD", "revert"]]) {
    const where = tryGit(cwd, ["rev-parse", "--git-path", name]);
    if (where && fs3.existsSync(path3.resolve(cwd, where))) return label;
  }
  return null;
}
function rebaseOnto(cwd, ref, env, mergePushed = true, sharedWins = false, session) {
  const busy = operationInProgress(cwd);
  if (busy) return { ok: false, skipped: "busy", error: `a ${busy} is in progress in this checkout` };
  if (editInFlight(cwd, session)) return { ok: false, overlap: true, inFlight: true, error: "another agent is editing a file in this checkout" };
  let pushed = false;
  try {
    const fork = tryGit(cwd, ["merge-base", "HEAD", ref]);
    if (fork) {
      const own = Number(git2(cwd, ["rev-list", "--count", `${fork}..HEAD`]));
      const unpushed = Number(git2(cwd, ["rev-list", "--count", "HEAD", "--not", fork, "--remotes"]));
      pushed = unpushed < own || own > MAX_REBASE;
    }
    const incoming = new Set(fork ? gitPaths(cwd, ["diff", "--name-only", "--no-renames", fork, ref]) : []);
    const clash = changedPaths(cwd).filter((p) => incoming.has(p));
    if (clash.length > 0) return { ok: false, overlap: true, error: `uncommitted changes in ${clash.slice(0, 3).join(", ")} overlap the incoming ones` };
  } catch (err) {
    return { ok: false, error: err.message };
  }
  if (pushed && !mergePushed) return { ok: false, skipped: "pushed", error: "this branch has commits that are already pushed; not rewriting them" };
  if (pushed) return mergeIn(cwd, ref, env, sharedWins);
  const res = runGit(cwd, ["rebase", "--quiet", "--autostash", ref], env);
  if (res.ok) return { ok: true };
  tryGit(cwd, ["rebase", "--abort"]);
  return { ok: false, error: res.error };
}
function mergeIn(cwd, ref, env, sharedWins) {
  const merged = runGit(cwd, ["merge", "--quiet", "--no-edit", ref], env);
  if (merged.ok) return { ok: true };
  const clashing = tryGit(cwd, ["diff", "--name-only", "--diff-filter=U", "-z"])?.split("\0").filter(Boolean) ?? [];
  const both = clashing.length > 0 ? clashing.map((p) => [p, keepBoth(blobAt(cwd, ":2", p), blobAt(cwd, ":1", p), blobAt(cwd, ":3", p))]) : [];
  if (both.length > 0 && both.every(([, data]) => data)) {
    try {
      for (const [p, data] of both) {
        fs3.writeFileSync(path3.join(cwd, p), data);
        git2(cwd, ["add", "--", `:(literal)${p}`]);
      }
      git2(cwd, ["commit", "--quiet", "--no-edit"], env);
      return { ok: true };
    } catch {
    }
  }
  tryGit(cwd, ["merge", "--abort"]);
  if (sharedWins && clashing.length > 0) {
    const mine = tryGit(cwd, ["rev-parse", "HEAD"]);
    const again = runGit(cwd, ["merge", "--quiet", "--no-edit", "-X", "theirs", ref], env);
    if (again.ok && mine) {
      tryGit(cwd, ["update-ref", `refs/lanes/backup/${mine}`, mine]);
      return { ok: true, reapplied: clashing };
    }
    tryGit(cwd, ["merge", "--abort"]);
  }
  return { ok: false, skipped: "pushed", error: merged.error };
}
function stagedFiles(cwd, pathspec) {
  return gitRaw(cwd, ["diff", "--cached", "--name-only", "--no-renames", "-z", ...pathspec]).split("\0").filter(Boolean);
}
function filesHash(files) {
  return crypto2.createHash("sha1").update([...files].sort().join("\0")).digest("hex");
}
function lanesRange(cwd, range) {
  const out = gitRaw(cwd, ["log", "--reverse", "--no-renames", "-z", "--name-only", "--format=%x01%H%x02%B%x03", range]);
  const records = out.split("").slice(1);
  const mine = [];
  const messages = /* @__PURE__ */ new Map();
  for (const record of records) {
    const [hash, rest] = record.split("");
    const [message, names = ""] = rest.split("");
    if (!/^Lanes-Agent: /m.test(message)) continue;
    const recorded = /^Lanes-Files: ([0-9a-f]{40})$/m.exec(message)?.[1];
    const files = names.split("\0").map((n) => n.replace(/^\n/, "")).filter(Boolean);
    if (!recorded || filesHash(files) === recorded) {
      mine.push(hash);
      messages.set(hash, message);
    }
  }
  return { mine, total: records.length, messages };
}
function changedPaths(cwd, only) {
  const spec = only ? ["--", ...only.map((p) => `:(literal)${p}`)] : [];
  const out = gitRaw(cwd, ["status", "--porcelain=v1", "-z", "--untracked-files=all", ...spec]);
  const fields = out.split("\0");
  const paths = [];
  for (let i = 0; i < fields.length; i++) {
    const entry = fields[i];
    if (entry.length < 4) continue;
    paths.push(entry.slice(3));
    if (entry[0] === "R" || entry[0] === "C") paths.push(fields[++i]);
  }
  return paths;
}
function parseHunks(diff, max = MAX_RANGES) {
  const out = /* @__PURE__ */ Object.create(null);
  let file2 = null;
  for (const line of diff.split("\n")) {
    if (line.startsWith("+++ ")) file2 = line.startsWith("+++ b/") ? line.slice(6).replace(/[\t\r]+$/, "") : null;
    else if (line.startsWith("diff --git ")) file2 = null;
    const hunk = file2 !== null && /^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@/.exec(line);
    if (!hunk) continue;
    const start = Number(hunk[1]);
    const count = hunk[2] === void 0 ? 1 : Number(hunk[2]);
    (out[file2] ??= []).push(count === 0 ? [Math.max(start, 1), Math.max(start, 1)] : [start, start + count - 1]);
  }
  for (const f in out) out[f] = mergeRanges(out[f], max);
  return out;
}
function changedRanges(cwd, from, to, paths) {
  if (paths.length === 0) return /* @__PURE__ */ Object.create(null);
  const out = tryGit(cwd, ["-c", "core.quotePath=false", "diff", "-U0", "--no-color", "--no-ext-diff", "--no-textconv", "--no-renames", "--src-prefix=a/", "--dst-prefix=b/", from, to, "--", ...paths.map((p) => `:(literal)${p}`)]);
  return parseHunks(out ?? "");
}
var SECRET_LIKE = /(^|\/)(\.env(\..+)?|\.npmrc|\.pypirc|\.netrc|credentials(\.json)?|id_(rsa|dsa|ecdsa|ed25519)(\.pub)?|[^/]+\.(pem|key|p12|pfx|keystore|jks))$/i;
var MAX_NEW_FILE = 1e6;
function filesFromTurn(cwd, before) {
  const earlier = new Set(before);
  return changedPaths(cwd).filter((p) => {
    if (earlier.has(p) || SECRET_LIKE.test(p)) return false;
    try {
      return fs3.statSync(`${cwd}/${p}`).size <= MAX_NEW_FILE;
    } catch {
      return true;
    }
  });
}
function checkpointMessage(trailers, files) {
  const subject = trailers.task.length > 72 ? `${trailers.task.slice(0, 69).trimEnd()}...` : trailers.task;
  return [
    subject,
    "",
    `Lanes-Agent: claude-code (session ${trailers.session.slice(0, 8)})`,
    `Lanes-Task: ${trailers.task}`,
    `Lanes-Files: ${filesHash(files)}`,
    "Co-authored-by: Claude <noreply@anthropic.com>"
  ].join("\n");
}
function committable(cwd, paths) {
  try {
    const changed = new Set(changedPaths(cwd, paths));
    return paths.filter((p) => changed.has(p));
  } catch {
    return paths;
  }
}
function commitWithin(cwd, args, env, ms) {
  return new Promise((resolve4) => {
    let log;
    const logFile = path3.join(privateDir("logs"), `commit-${process.pid}-${Date.now()}.log`);
    try {
      log = fs3.openSync(logFile, "w", 384);
    } catch (err) {
      return resolve4({ ok: false, error: err.message });
    }
    const child = (0, import_node_child_process2.spawn)("git", LANES_GIT.concat(args), { cwd, stdio: ["ignore", log, log], windowsHide: true, detached: true, env: { ...process.env, ...env } });
    let abandoned = false;
    const finish = (result) => {
      clearTimeout(timer);
      if (abandoned) return void fs3.rmSync(logFile, { force: true });
      fs3.closeSync(log);
      if (!result.ok) result.error = fs3.readFileSync(logFile, "utf8").trim().slice(0, 500) || result.error;
      fs3.rmSync(logFile, { force: true });
      resolve4(result);
    };
    const timer = setTimeout(() => {
      abandoned = true;
      child.unref();
      fs3.closeSync(log);
      resolve4("pending");
    }, ms);
    child.on("error", (err) => finish({ ok: false, error: err.message }));
    child.on("close", (code) => finish(code === 0 ? { ok: true } : { ok: false, error: `git exited with ${code}` }));
  });
}
async function checkpointAndPublish(cwd, who, trailers, paths, limits = {}) {
  const failed = [];
  const result = await publishCore(cwd, who, trailers, paths, limits, failed);
  return failed.length > 0 ? { ...result, failed } : result;
}
async function publishCore(cwd, who, trailers, paths, limits, failed) {
  const started = Date.now();
  const REMOTE = remoteOf(cwd);
  const known = REMOTE ? tryGit(cwd, ["rev-parse", "--verify", "--quiet", `refs/remotes/${REMOTE}/${INTEGRATION_BRANCH}`]) !== null : false;
  if (!known && integrationRef(cwd) === null) return { status: "no-integration-branch" };
  const env = identityEnv(who);
  const busy = operationInProgress(cwd);
  if (busy) return { status: "conflict", error: `a ${busy} is in progress in this checkout; the edit is not committed until it ends` };
  const literal = (list) => ["--", ...list.map((p) => `:(literal)${p}`)];
  let todo = committable(cwd, paths);
  const commitTodo = async () => {
    const staged = gitRaw(cwd, ["diff", "--name-only", "--no-renames", "-z", "HEAD", ...literal(todo)]).split("\0").filter(Boolean);
    return staged.length === 0 ? { ok: true } : commitWithin(cwd, ["commit", "--quiet", "-m", checkpointMessage(trailers, staged), ...literal(todo)], env, limits.commitDeadlineMs ?? 12e3);
  };
  if (todo.length > 0) {
    const intent = runGit(cwd, ["add", "-N", ...literal(todo)]).ok;
    let commit = intent ? await commitTodo() : { ok: false, error: "" };
    if (commit !== "pending" && !commit.ok) {
      if (!runGit(cwd, ["add", "-A", ...literal(todo)]).ok) {
        for (const p of todo) if (!runGit(cwd, ["add", "-A", ...literal([p])]).ok) failed.push(p);
        todo = todo.filter((p) => !failed.includes(p));
      }
      commit = todo.length > 0 ? await commitTodo() : { ok: true };
    }
    if (commit === "pending") return { status: "commit-pending" };
    if (!commit.ok) return { status: "commit-failed", error: commit.error };
  }
  if (todo.length === 0 && failed.length === 0 && !limits.sync) {
    try {
      if (lanesRange(cwd, `${REMOTE ? `refs/remotes/${REMOTE}/${INTEGRATION_BRANCH}` : INTEGRATION_BRANCH}..HEAD`).mine.length === 0) return { status: "nothing" };
    } catch {
    }
  }
  let reapply;
  const before = git2(cwd, ["rev-parse", "HEAD"]);
  const pulledSince = (base) => {
    const forkPoint = tryGit(cwd, ["merge-base", before, base]);
    if (!forkPoint) return [];
    const unseen = new Set(gitPaths(cwd, ["diff", "--name-only", "--no-renames", before, base]));
    return gitPaths(cwd, ["diff", "--name-only", "--no-renames", forkPoint, base]).filter((p) => unseen.has(p));
  };
  let pulled = [];
  let syncSkipped;
  let moved = false;
  let sharedWon = false;
  let unrewritable = false;
  for (let attempt = 0; attempt < 8; attempt++) {
    if (attempt > 0 && Date.now() - started > (limits.budgetMs ?? 3e4) - netTimeout("fetch")) return { status: "retries-exhausted", error: "out of time" };
    const target = integrationRef(cwd);
    if (target === null) return { status: "no-integration-branch" };
    const base = target.sha;
    if (!isAncestor(cwd, base) && onlyOwnIncoming(cwd, base)) {
      unrewritable = true;
    } else if (!isAncestor(cwd, base)) {
      moved = true;
      pulled = pulledSince(base);
      const rebase = rebaseOnto(cwd, base, env, true, true, trailers.session);
      if (rebase.ok && rebase.reapplied) {
        reapply = { backup: before, paths: rebase.reapplied };
        sharedWon = true;
      }
      unrewritable = !rebase.ok && (rebase.skipped === "pushed" || rebase.overlap === true);
      syncSkipped = !rebase.ok && unrewritable && !rebase.inFlight ? rebase.error : void 0;
      if (!rebase.ok && !unrewritable) {
        if (rebase.skipped) return { status: "conflict", error: rebase.error };
        reapply = moveOntoShared(cwd, base, trailers, env);
        if (reapply) continue;
        const merge = mergeIn(cwd, base, env, true);
        if (!merge.ok) return { status: "conflict", error: rebase.error };
        if (merge.reapplied) {
          reapply = { backup: before, paths: merge.reapplied };
          sharedWon = true;
        }
      }
    }
    const head = moved ? git2(cwd, ["rev-parse", "HEAD"]) : before;
    const range = lanesRange(cwd, `${base}..${head}`);
    const published = unrewritable || range.total !== range.mine.length ? publishedSet(cwd) : /* @__PURE__ */ new Set();
    const mine = range.mine.filter((h) => !published.has(h));
    if (mine.length === 0) return reapply ? { status: "published", commit: base, paths: [], reapply, pulled } : { status: "nothing", pulled };
    let tip = head;
    let blocked;
    let replayed;
    if (unrewritable || range.total !== range.mine.length || mine.length !== range.mine.length) {
      const replay = onlyLanes(cwd, base, mine, range.messages, env, sharedWon);
      replayed = replay;
      tip = replay.tip;
      if (replay.blocked.length > 0) blocked = replay.blocked;
      if (tip === base && blocked) return { status: "conflict", error: replay.error };
    }
    const swapped = REMOTE ? netGit(cwd, ["push", "--quiet", REMOTE, `${tip}:refs/heads/${INTEGRATION_BRANCH}`]) !== null : tryGit(cwd, ["update-ref", `refs/heads/${INTEGRATION_BRANCH}`, tip, base]) !== null;
    if (swapped) {
      if (replayed) rememberPublished(cwd, [...replayed.done, ...replayed.made]);
      const changed = gitPaths(cwd, ["diff", "--name-only", "--no-renames", base, tip]);
      if (tip !== head && !unrewritable) rebaseOnto(cwd, tip, env, false, false, trailers.session);
      return { status: "published", commit: tip, base, paths: changed, reapply, pulled, blocked, syncSkipped };
    }
  }
  return { status: "retries-exhausted" };
}
function replayTree(cwd, tip, commit, sharedWins) {
  const run2 = (0, import_node_child_process2.spawnSync)("git", LANES_GIT.concat(["merge-tree", "--write-tree", "--name-only", "-z", ...sharedWins ? ["-X", "ours"] : [], `--merge-base=${commit}^`, tip, commit]), { cwd, windowsHide: true, maxBuffer: MAX_BUFFER });
  const fields = run2.stdout?.toString("utf8").split("\0") ?? [];
  if (run2.status === 0) return fields[0];
  if (run2.status !== 1) return null;
  const conflicted = fields.slice(1, fields.indexOf("", 1));
  if (conflicted.length === 0) return null;
  const merged = [];
  for (const p of conflicted) {
    const data = blobAt(cwd, tip, p) ? keepBoth(blobAt(cwd, commit, p), blobAt(cwd, `${commit}^`, p), blobAt(cwd, tip, p)) : null;
    if (!data) return null;
    merged.push([p, data]);
  }
  const dir = fs3.mkdtempSync(path3.join(privateDir("tmp"), "tree-"));
  try {
    const env = { GIT_INDEX_FILE: path3.join(dir, "index") };
    git2(cwd, ["read-tree", fields[0]], env);
    for (const [p, data] of merged) {
      const file2 = path3.join(dir, "blob");
      fs3.writeFileSync(file2, data);
      const mode = tryGit(cwd, ["ls-tree", commit, "--", `:(literal)${p}`])?.split(" ")[0] ?? "100644";
      git2(cwd, ["update-index", "--add", "--cacheinfo", `${mode},${git2(cwd, ["hash-object", "-w", "--", file2])},${p}`], env);
    }
    return git2(cwd, ["write-tree"], env);
  } catch {
    return null;
  } finally {
    fs3.rmSync(dir, { recursive: true, force: true });
  }
}
function onlyLanes(cwd, base, commits, messages, env, sharedWins = false) {
  let tip = base;
  let tipTree;
  const blocked = [];
  const done = [];
  const made = [];
  let error = "";
  const was = blockedSet(cwd);
  const stuck = [];
  const order = [...commits.filter((c) => !was.has(c)), ...commits.filter((c) => was.has(c))].slice(0, MAX_REPLAY());
  for (const commit of order) {
    const tree = replayTree(cwd, tip, commit, sharedWins);
    if (!tree) {
      error = "a Lanes commit depends on local commits that are not shared";
      blocked.push(...gitPaths(cwd, ["diff-tree", "-r", "--root", "--no-commit-id", "--name-only", "--no-renames", commit]));
      stuck.push(commit);
      continue;
    }
    tipTree ??= git2(cwd, ["rev-parse", `${tip}^{tree}`]);
    done.push(commit);
    if (tree === tipTree) continue;
    tip = git2(cwd, ["commit-tree", tree, "-p", tip, "-m", messages.get(commit) ?? git2(cwd, ["log", "-1", "--format=%B", commit])], env);
    tipTree = tree;
    made.push(tip);
  }
  rememberBlocked(cwd, [...[...was].filter((h) => !done.includes(h)), ...stuck.filter((h) => !was.has(h))]);
  return { tip, blocked, error, done, made };
}
function moveOntoShared(cwd, base, trailers, env) {
  const mine = git2(cwd, ["rev-parse", "HEAD"]);
  const forkPoint = tryGit(cwd, ["merge-base", mine, base]);
  if (!forkPoint) return void 0;
  let ours;
  let theirs;
  try {
    const range = `${forkPoint}..${mine}`;
    const own = lanesRange(cwd, range);
    if (own.mine.length !== own.total) return void 0;
    const changed = (from, to) => new Set(gitPaths(cwd, ["diff", "--name-only", "--no-renames", from, to]));
    const moved = changed(mine, base);
    if (changedPaths(cwd).some((p) => moved.has(p))) return void 0;
    ours = changed(forkPoint, mine);
    theirs = changed(forkPoint, base);
  } catch {
    return void 0;
  }
  let overlapping = [...ours].filter((f) => theirs.has(f));
  const carried = [...ours].filter((f) => !theirs.has(f));
  const mergedFiles = /* @__PURE__ */ new Map();
  for (const p of overlapping) {
    const m = keepBoth(blobAt(cwd, mine, p), blobAt(cwd, forkPoint, p), blobAt(cwd, base, p));
    if (m) mergedFiles.set(p, m);
  }
  overlapping = overlapping.filter((p) => !mergedFiles.has(p));
  tryGit(cwd, ["update-ref", `refs/lanes/backup/${mine}`, mine]);
  const stashTop = () => tryGit(cwd, ["rev-parse", "-q", "--verify", "refs/stash"]);
  const stashBefore = stashTop();
  tryGit(cwd, ["stash", "push", "--include-untracked", "--quiet", "-m", "lanes: kept during re-apply"]);
  const stashed = stashTop() !== stashBefore;
  const restoreStash = () => {
    if (stashed) tryGit(cwd, ["stash", "pop", "--quiet"]);
  };
  try {
    git2(cwd, ["reset", "--quiet", "--hard", base]);
    for (const [p, data] of mergedFiles) {
      fs3.writeFileSync(path3.join(cwd, p), data);
      git2(cwd, ["add", "--", `:(literal)${p}`]);
    }
    if (carried.length > 0 || mergedFiles.size > 0) {
      const spec = (paths) => ["--", ...paths.map((p) => `:(literal)${p}`)];
      const kept = carried.filter((p) => tryGit(cwd, ["cat-file", "-e", `${mine}:${p}`]) !== null);
      const deleted = carried.filter((p) => !kept.includes(p));
      if (kept.length > 0) git2(cwd, ["restore", `--source=${mine}`, "--staged", "--worktree", ...spec(kept)]);
      if (deleted.length > 0) git2(cwd, ["rm", "-q", "-f", "--ignore-unmatch", ...spec(deleted)]);
      tryGit(cwd, ["commit", "--quiet", "-m", checkpointMessage(trailers, stagedFiles(cwd, []))], env);
    }
  } catch {
    tryGit(cwd, ["reset", "--quiet", "--hard", mine]);
    restoreStash();
    return void 0;
  }
  restoreStash();
  return { backup: mine, paths: overlapping };
}

// packages/core/src/ledger.ts
var crypto3 = __toESM(require("node:crypto"), 1);
var fs4 = __toESM(require("node:fs"), 1);
var path4 = __toESM(require("node:path"), 1);
var LEDGER_MAX_BYTES = Number(process.env.LANES_LEDGER_MAX_BYTES ?? 256 * 1024);
var WEEK_MS = 7 * 24 * 36e5;
var file = () => path4.join(privateDir(), "ledger.jsonl");
var pendingFlag = () => path4.join(privateDir(), "ledger.pending");
var watermarkFile = () => path4.join(privateDir("cache"), "ledger-reported.json");
var repoKey = (repoId) => crypto3.createHash("sha1").update(repoId).digest("hex").slice(0, 8);
function appendLedger(repoId, kind, ms, now = Date.now()) {
  try {
    const f = file();
    try {
      if (fs4.statSync(f).size > LEDGER_MAX_BYTES) fs4.renameSync(f, `${f}.1`);
    } catch {
    }
    const entry = { ts: now, repo: repoKey(repoId), kind, ms: Math.max(0, Math.round(ms)) };
    fs4.appendFileSync(f, JSON.stringify(entry) + "\n", { mode: 384 });
    if (kind === "failopen") fs4.writeFileSync(pendingFlag(), "", { mode: 384 });
  } catch {
  }
}
function readLedger() {
  const out = [];
  for (const f of [`${file()}.1`, file()]) {
    let text = "";
    try {
      text = fs4.readFileSync(f, "utf8");
    } catch {
      continue;
    }
    for (const line of text.split("\n")) {
      try {
        const e = JSON.parse(line);
        if (typeof e.ts === "number" && typeof e.repo === "string" && typeof e.ms === "number" && (e.kind === "failopen" || e.kind === "queued" || e.kind === "ok")) out.push(e);
      } catch {
      }
    }
  }
  return out;
}
function takeUnreported(repoId, now = Date.now()) {
  try {
    if (!fs4.existsSync(pendingFlag())) return 0;
    let marks = {};
    try {
      marks = JSON.parse(fs4.readFileSync(watermarkFile(), "utf8"));
    } catch {
    }
    const key = repoKey(repoId);
    const failed = readLedger().filter((e) => e.kind === "failopen");
    const count = failed.filter((e) => e.repo === key && e.ts > (marks[key] ?? 0) && e.ts > now - WEEK_MS).length;
    marks[key] = now;
    fs4.writeFileSync(watermarkFile(), JSON.stringify(marks), { mode: 384 });
    if (!failed.some((e) => e.repo !== key && e.ts > (marks[e.repo] ?? 0))) fs4.rmSync(pendingFlag(), { force: true });
    return count;
  } catch {
    return 0;
  }
}

// packages/hook/src/agents/copilot.ts
var EDIT_TOOLS = /* @__PURE__ */ new Set(["edit", "create"]);
function argsPath(args) {
  let a = args;
  if (typeof a === "string") {
    try {
      a = JSON.parse(a);
    } catch {
      return void 0;
    }
  }
  if (!a || typeof a !== "object") return void 0;
  const o = a;
  const p = o.path ?? o.file_path ?? o.filePath;
  return typeof p === "string" && p !== "" ? p : void 0;
}
function fromCopilot(raw) {
  const tool = raw.toolName ?? raw.tool_name;
  const file2 = EDIT_TOOLS.has(tool) ? argsPath(raw.toolArgs ?? raw.tool_input) : void 0;
  return {
    session_id: raw.sessionId ?? raw.session_id,
    cwd: raw.cwd,
    prompt: raw.prompt,
    ...file2 && { tool_name: tool, tool_input: { file_path: file2 } }
  };
}
function copilotContext(hookEventName, text) {
  return hookEventName === "SessionStart" || hookEventName === "PostToolUse" ? { additionalContext: text } : void 0;
}
function isCopilotClaudeFormat(raw) {
  return raw.timestamp !== void 0 && raw.transcript_path === void 0;
}
function isCopilotCloud(env) {
  return Boolean(env.GITHUB_COPILOT_API_TOKEN || env.COPILOT_AGENT_PROMPT);
}

// packages/hook/src/agents/kiro.ts
function fromKiro(raw, env = process.env) {
  const input = raw.tool_input && typeof raw.tool_input === "object" ? raw.tool_input : {};
  const op = Array.isArray(input.operations) ? input.operations[0] : void 0;
  const file2 = [input.path, input.file_path, op?.path].find((p) => typeof p === "string" && p !== "");
  return {
    session_id: raw.session_id,
    cwd: raw.cwd,
    prompt: raw.prompt ?? env.USER_PROMPT,
    ...raw.tool_name && file2 && { tool_name: String(raw.tool_name), tool_input: { file_path: file2 } }
  };
}
function kiroContext(hookEventName, text) {
  return ["SessionStart", "UserPromptSubmit", "PostToolUse"].includes(hookEventName) ? text : void 0;
}

// packages/hook/src/imports.ts
var fs5 = __toESM(require("node:fs"), 1);
var path5 = __toESM(require("node:path"), 1);
var JS = /\.(?:[cm]?[jt]sx?)$/;
var JS_SPEC = /(?:\bfrom\s*|\bimport\s*\(\s*|\brequire\s*\(\s*|^\s*import\s+)["'](\.{1,2}\/[^"']+)["']/gm;
var PY_SPEC = /^\s*from\s+(\.+)([\w.]*)\s+import\b/gm;
var JS_EXT = ["", ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".mts", ".cts", "/index.ts", "/index.tsx", "/index.js", "/index.mjs"];
function importsOf(root, file2) {
  let text;
  try {
    text = fs5.readFileSync(path5.join(root, file2), "utf8");
  } catch {
    return [];
  }
  const dir = path5.posix.dirname(file2.replaceAll("\\", "/"));
  const found = /* @__PURE__ */ new Set();
  const add = (candidates) => {
    const hit = candidates.find((c) => !c.startsWith("..") && isFile(path5.join(root, c)));
    if (hit) found.add(hit);
  };
  if (JS.test(file2)) {
    for (const [, spec] of text.matchAll(JS_SPEC)) {
      const base = path5.posix.normalize(path5.posix.join(dir, spec));
      const stem = base.replace(/\.(?:[cm]?js)$/, "");
      add([...JS_EXT.map((e) => base + e), ...stem !== base ? JS_EXT.map((e) => stem + e) : []]);
    }
  } else if (file2.endsWith(".py")) {
    for (const [, dots, mod] of text.matchAll(PY_SPEC)) {
      const up = path5.posix.join(dir, ...Array(dots.length - 1).fill(".."));
      const base = path5.posix.normalize(path5.posix.join(up, ...mod.split(".").filter(Boolean)));
      add([base + ".py", base + "/__init__.py"]);
    }
  }
  return [...found];
}
function isFile(p) {
  try {
    return fs5.statSync(p).isFile();
  } catch {
    return false;
  }
}

// packages/hook/src/hook.ts
var EVENTS = ["prompt", "start", "pre", "permission", "post", "stop", "end", "observe", "read"];
var COMMIT_DEADLINE_MS = process.env.LANES_COMMIT_DEADLINE_MS ? Number(process.env.LANES_COMMIT_DEADLINE_MS) : void 0;
var MAX_REPORTED_PATHS = 5e3;
var LOCK_WAIT_MS = Number(process.env.LANES_LOCK_WAIT_MS ?? 2e4);
var checkoutLock = (root) => `publish-${crypto4.createHash("sha1").update(root).digest("hex").slice(0, 16)}`;
var APPROVAL_HOLD_MS = 30 * 6e4;
var CURSOR_TURN = "(turn)";
var ACQUIRE_WAIT_MS = Number(process.env.LANES_WAIT_MS ?? 24e4);
var COMMIT_ID = /^[0-9a-f]{40,64}$/;
async function post(config, route, body, timeoutMs) {
  const token = tokenFor(config);
  try {
    return await request2(config.serverUrl + route, token, body, timeoutMs);
  } catch (err) {
    if (err.code !== "ECONNREFUSED" || !isLoopback(config.serverUrl) || process.env.LANES_AUTOSTART === "0") throw err;
    await startLocalServer(config.serverUrl, token ?? loadOrCreateToken());
    return request2(config.serverUrl + route, token, body, timeoutMs);
  }
}
async function startLocalServer(serverUrl, token) {
  const here = path6.dirname(process.argv[1]);
  const bundled = path6.join(here, "server.cjs");
  const main = process.env.LANES_SERVER_MAIN ?? (fs6.existsSync(bundled) ? bundled : path6.join(here, "..", "..", "server", "src", "main.ts"));
  const env = { ...process.env, LANES_PORT: new URL(serverUrl).port || "4317", LANES_TOKEN: token, LANES_IDLE_EXIT_MS: process.env.LANES_IDLE_EXIT_MS ?? String(30 * 6e4) };
  (0, import_node_child_process3.spawn)(process.execPath, ["--no-warnings", main], { detached: true, stdio: "ignore", windowsHide: true, env }).unref();
  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 100));
    const up = await new Promise((resolve4) => {
      http.get(serverUrl + "/health", (res) => resolve4(res.statusCode === 200)).on("error", () => resolve4(false));
    });
    if (up) return;
  }
}
function request2(url, token, body, timeoutMs) {
  return new Promise((resolve4, reject) => {
    const data = JSON.stringify(body);
    const headers = { "content-type": "application/json", "content-length": Buffer.byteLength(data) };
    if (token) headers.authorization = `Bearer ${token}`;
    const req = http.request(
      url,
      { method: "POST", headers, signal: AbortSignal.timeout(timeoutMs) },
      (res) => {
        let text = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => text += chunk);
        res.on("end", () => {
          if ((res.statusCode ?? 500) >= 400) return reject(new Error(`HTTP ${res.statusCode}`));
          try {
            resolve4(JSON.parse(text));
          } catch (err) {
            reject(err);
          }
        });
        res.on("error", reject);
      }
    );
    req.on("error", reject);
    req.end(data);
  });
}
var IDENTITY_TTL_MS = 5 * 6e4;
var identities = /* @__PURE__ */ new Map();
function gitIdentity(root) {
  const known = identities.get(root);
  if (known) return known;
  const file2 = path6.join(privateDir("cache"), "identity.json");
  let cache = {};
  try {
    cache = JSON.parse(fs6.readFileSync(file2, "utf8"));
  } catch {
  }
  let entry = cache[root];
  if (!entry || Date.now() - entry.at > IDENTITY_TTL_MS) {
    entry = { at: Date.now() };
    for (const line of (tryGit(root, ["config", "--get-regexp", "^user\\.(name|email)$"]) ?? "").split("\n")) {
      const [key, ...value] = line.split(" ");
      if (key === "user.name") entry.name = value.join(" ").trim() || void 0;
      if (key === "user.email") entry.email = value.join(" ").trim() || void 0;
    }
    try {
      const tmp = `${file2}.${process.pid}`;
      fs6.writeFileSync(tmp, JSON.stringify({ ...cache, [root]: entry }), { mode: 384 });
      fs6.renameSync(tmp, file2);
    } catch {
    }
  }
  identities.set(root, entry);
  return entry;
}
var stateFile = (sessionId) => path6.join(privateDir("sessions"), `${sessionId.replace(/[^\w-]/g, "")}.json`);
var sleepSync2 = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
function loadState(sessionId) {
  for (let attempt = 0; ; attempt++) {
    let text;
    try {
      text = fs6.readFileSync(stateFile(sessionId), "utf8");
    } catch {
      return { touched: [] };
    }
    try {
      return { touched: [], ...JSON.parse(text) };
    } catch {
      if (attempt < 5) {
        sleepSync2(15);
        continue;
      }
      try {
        fs6.renameSync(stateFile(sessionId), stateFile(sessionId) + ".corrupt");
      } catch {
      }
      return { touched: [] };
    }
  }
}
function saveState(sessionId, state) {
  const file2 = stateFile(sessionId);
  const tmp = `${file2}.${process.pid}.tmp`;
  const json = JSON.stringify(state);
  fs6.writeFileSync(tmp, json, { mode: 384 });
  for (let attempt = 0; ; attempt++) {
    try {
      return fs6.renameSync(tmp, file2);
    } catch (err) {
      const code = err.code;
      if (!(code === "EPERM" || code === "EBUSY" || code === "EACCES")) {
        fs6.rmSync(tmp, { force: true });
        throw err;
      }
      if (attempt >= 50) {
        fs6.rmSync(tmp, { force: true });
        return fs6.writeFileSync(file2, json, { mode: 384 });
      }
      sleepSync2(20);
    }
  }
}
async function forgetState(sessionId) {
  await withLock(`state-${sessionId}`, () => {
    if (loadState(sessionId).touched.length > 0) return;
    for (const f of [stateFile(sessionId), `${stateFile(sessionId)}.corrupt`]) fs6.rmSync(f, { force: true });
  });
}
function pruneSessions(maxAgeMs = 14 * 24 * 36e5) {
  try {
    const dir = privateDir("sessions");
    for (const f of fs6.readdirSync(dir)) {
      try {
        if (Date.now() - fs6.statSync(path6.join(dir, f)).mtimeMs > maxAgeMs) fs6.rmSync(path6.join(dir, f), { force: true });
      } catch {
      }
    }
  } catch {
  }
}
async function updateState(sessionId, change) {
  return withLock(`state-${sessionId}`, () => {
    const state = loadState(sessionId);
    change(state);
    saveState(sessionId, state);
    return state;
  });
}
var AGENT_NAMES = ["gemini", "cursor", "antigravity", "copilot", "kiro"];
var agent = "claude";
function fromAgent(from, raw) {
  if (from === "antigravity") return fromAntigravity(raw);
  if (from === "copilot") return fromCopilot(raw);
  if (from === "kiro") return fromKiro(raw);
  if (from !== "cursor") return raw;
  const file2 = typeof raw.file_path === "string" ? raw.file_path : void 0;
  return {
    session_id: raw.conversation_id ?? raw.session_id,
    cwd: raw.cwd ?? raw.workspace_roots?.[0],
    prompt: raw.prompt,
    ...file2 && { tool_name: "afterFileEdit", tool_input: { file_path: file2 } }
  };
}
var GEMINI_EVENTS = { SessionStart: "SessionStart", UserPromptSubmit: "BeforeAgent", PostToolUse: "AfterTool" };
function contextOutput(to, hookEventName, text) {
  if (to === "antigravity") return antigravityContext(hookEventName, text);
  if (to === "copilot") return copilotContext(hookEventName, text);
  if (to === "kiro") return kiroContext(hookEventName, text);
  if (to === "cursor") return hookEventName === "SessionStart" ? { additional_context: text } : void 0;
  if (to === "gemini") return GEMINI_EVENTS[hookEventName] ? { hookSpecificOutput: { hookEventName: GEMINI_EVENTS[hookEventName], additionalContext: text } } : void 0;
  return { hookSpecificOutput: { hookEventName, additionalContext: text } };
}
function defaultOutput(to, event) {
  if (to === "claude" || to === "antigravity" || to === "copilot" || to === "kiro") return void 0;
  return to === "cursor" && event === "prompt" ? { continue: true } : {};
}
var buffered = false;
var wrote = false;
var pending;
function write(json) {
  if (!json) return;
  wrote = true;
  process.stdout.write(typeof json === "string" ? json : JSON.stringify(json));
}
function output(hookEventName, fields) {
  if (!buffered) return write(contextOutput(agent, hookEventName, fields.additionalContext));
  pending = { hookEventName, additionalContext: pending ? `${pending.additionalContext} ${fields.additionalContext}` : fields.additionalContext };
}
function flush(event) {
  if (pending) write(contextOutput(agent, pending.hookEventName, pending.additionalContext));
  if (!wrote) write(defaultOutput(agent, event));
}
function editedFiles(input) {
  const one = input.tool_input?.file_path ?? input.tool_input?.notebook_path;
  if (one) return [one];
  const patch = input.tool_input?.command;
  if (input.tool_name !== "apply_patch" || typeof patch !== "string") return [];
  const files = [...patch.matchAll(/^\*\*\* (?:Add File|Update File|Delete File|Move to): (.+)$/gm)].map((m) => m[1].trim());
  return [...new Set(files)].sort();
}
function editSpan(text, toolInput) {
  const olds = Array.isArray(toolInput?.edits) ? toolInput.edits.map((e) => e?.old_string) : [toolInput?.old_string];
  const body = text.replace(/\r\n/g, "\n");
  let span;
  for (const raw of olds) {
    if (typeof raw !== "string" || raw === "") return void 0;
    const old = raw.replace(/\r\n/g, "\n");
    const at = body.indexOf(old);
    if (at < 0 || body.indexOf(old, at + 1) >= 0) return void 0;
    const first = body.slice(0, at).split("\n").length;
    const last = first + old.replace(/\n$/, "").split("\n").length - 1;
    span = span ? [Math.min(span[0], first), Math.max(span[1], last)] : [first, last];
  }
  return span;
}
var MAX_PROMPT_SCAN = 2e4;
var MAX_TOKEN = 260;
var GENERIC_STEMS = /* @__PURE__ */ new Set(["index", "types", "main", "readme", "package"]);
var isWordChar = (c) => c !== void 0 && /\w/.test(c);
function hasWord(text, word) {
  for (let i = text.indexOf(word); i >= 0; i = text.indexOf(word, i + 1)) {
    if (!isWordChar(text[i - 1]) && !isWordChar(text[i + word.length])) return true;
  }
  return false;
}
var quoteList = (ps) => ps.length > 8 ? `${ps.slice(0, 8).join(", ")} and ${ps.length - 8} more` : ps.join(", ");
function staleDependencies(root, edited, reads, arrived) {
  const mine = new Set(edited);
  const came = arrived.filter((p) => !mine.has(p));
  if (came.length === 0) return void 0;
  const deps = /* @__PURE__ */ new Set([...reads, ...edited.flatMap((f) => importsOf(root, f))]);
  const used = came.filter((p) => deps.has(p));
  const callers = came.filter((p) => !deps.has(p) && importsOf(root, p).some((i) => mine.has(i)));
  if (used.length === 0 && callers.length === 0) return void 0;
  const text = [];
  if (used.length > 0)
    text.push(`While you worked, a teammate's agent changed ${quoteList(used)}, which your code uses. Re-read it and make sure your changes match the new version before finishing.`);
  if (callers.length > 0)
    text.push(`A teammate's agent added or changed ${quoteList(callers)}, which imports a file you changed. Check it still matches your change.`);
  text.push("No need to ask the user.");
  return { text: text.join(" "), paths: [...used, ...callers] };
}
function predictFiles(root, prompt) {
  const text = prompt.slice(0, MAX_PROMPT_SCAN);
  const files = listFiles(root);
  const predicted = /* @__PURE__ */ new Set();
  const tokens = /* @__PURE__ */ new Set();
  for (const raw of text.split(/[^\w./-]+/)) {
    const token = raw.replace(/\.+$/, "").replace(/^\.\//, "");
    if (token.length <= MAX_TOKEN && /\.[a-z]{1,4}$/i.test(token)) tokens.add(token.toLowerCase());
  }
  if (tokens.size > 0) {
    for (const f of files) {
      for (let at = -1; ; at = f.indexOf("/", at + 1)) {
        if (tokens.has(f.slice(at + 1).toLowerCase())) predicted.add(f);
        if (f.indexOf("/", at + 1) < 0) break;
      }
    }
  }
  const lower = text.toLowerCase();
  const stems = /* @__PURE__ */ new Map();
  for (const f of files) {
    const stem = path6.posix.basename(f).replace(/\.[^.]+$/, "").toLowerCase();
    if (stem.length < 4 || GENERIC_STEMS.has(stem)) continue;
    if (!stems.has(stem)) stems.set(stem, hasWord(lower, stem));
    if (stems.get(stem)) predicted.add(f);
  }
  return [...predicted].slice(0, 20);
}
async function run(event, input, config) {
  const sessionId = input.session_id;
  if (!sessionId) return;
  const cwd = input.cwd ?? process.cwd();
  const userFor = (dir) => config.user ?? gitIdentity(dir).name ?? os2.userInfo().username;
  const ref = (repo2) => ({ user: userFor(repo2.root), sessionId, repo: repo2.id, label: repo2.label });
  const observe = (repo2, fields) => {
    if (typeof fields.error === "string") fields.error = redactSecrets(fields.error);
    return post(config, "/v1/observe", { sessionId, user: repo2 ? userFor(repo2.root) : config.user, repo: repo2?.id, ...fields }, 2e3).catch(() => {
    });
  };
  if (event === "observe") {
    await observe(findRepo(cwd), { command: input.tool_input?.command });
    return;
  }
  if (event === "read") {
    const file2 = input.tool_input?.file_path;
    const loc = file2 ? locate(path6.resolve(cwd, file2)) : null;
    if (!loc || !teamContextOn(loc.repo.root)) return;
    await updateState(sessionId, (s) => void (s.reads = [...(s.reads ?? []).filter((p) => p !== loc.path), loc.path].slice(-200)));
    const heads = await post(config, "/v1/team/context", { ...ref(loc.repo), path: loc.path, read: true }, 1500).catch(() => void 0);
    if (heads?.text) output("PreToolUse", { additionalContext: heads.text });
    return;
  }
  if (event === "end") await forgetState(sessionId);
  if (event === "start" || event === "end") pruneSessions();
  if (event === "prompt") {
    const repo2 = findRepo(cwd);
    if (!repo2) return;
    const promptText = input.prompt ?? "";
    const firstLine = promptText.split("\n").find((l) => l.trim())?.trim() ?? "";
    const title = process.env.LANES_NO_PROMPT_TEXT === "1" ? "Agent task" : redactSecrets(firstLine).slice(0, 120) || "Agent task";
    let dirtyAtStart;
    try {
      dirtyAtStart = changedPaths(repo2.root);
    } catch {
    }
    if (agent === "cursor") await withLock(checkoutLock(repo2.root), () => markEditInFlight(repo2.root, sessionId, CURSOR_TURN, true), LOCK_WAIT_MS).catch(() => {
    });
    const turnStart = tryGit(repo2.root, ["rev-parse", "HEAD"]) ?? void 0;
    await updateState(sessionId, (s) => Object.assign(s, { title, dirtyAtStart, turnStart, bases: [], reads: [], arrived: [] }));
    await post(config, "/v1/task", { ...ref(repo2), title, predicted: predictFiles(repo2.root, promptText) }, 5e3);
    if (teamContextOn(repo2.root)) {
      const brief = await post(config, "/v1/team/context", ref(repo2), 2e3).catch(() => void 0);
      if (brief?.text) output("UserPromptSubmit", { additionalContext: brief.text });
      if (wrote && brief?.deliver?.length) await post(config, "/v1/team/ack", { ...ref(repo2), ids: brief.deliver }, 1e3).catch(() => {
      });
    }
    return;
  }
  const identity = (repo2) => {
    const name = userFor(repo2.root);
    return { name, email: config.email ?? gitIdentity(repo2.root).email ?? `${name}@users.noreply.lanes.dev` };
  };
  const publish = async (repo2, final, edited, extraNotes = []) => {
    const state = await updateState(sessionId, (s) => {
      if (edited && !s.touched.includes(edited)) s.touched.push(edited);
    });
    let fromTurn = [];
    if (!edited && state.dirtyAtStart) {
      try {
        fromTurn = filesFromTurn(repo2.root, state.dirtyAtStart);
      } catch (err) {
        await observe(repo2, { kind: "status-failed", error: err.message });
      }
    }
    const paths = edited ? [edited] : [.../* @__PURE__ */ new Set([...state.touched, ...fromTurn])];
    const team = teamContextOn(repo2.root);
    const limits = final ? { commitDeadlineMs: COMMIT_DEADLINE_MS ?? 3e4, budgetMs: 7e4, sync: team } : { commitDeadlineMs: COMMIT_DEADLINE_MS ?? 12e3, budgetMs: 3e4 };
    const waitStart = Date.now();
    const result = await withLock(
      checkoutLock(repo2.root),
      () => checkpointAndPublish(repo2.root, identity(repo2), { session: sessionId, task: state.title ?? "Agent changes" }, paths, { ...limits, budgetMs: Math.max(limits.budgetMs - (Date.now() - waitStart), 0) }),
      LOCK_WAIT_MS
    );
    const failed = result.failed ?? [];
    if (final && (result.status === "published" || result.status === "nothing")) {
      await updateState(sessionId, (s) => void (s.touched = [...s.touched.filter((p) => !paths.includes(p)), ...failed]));
    }
    if (result.status !== "published" && result.status !== "nothing") {
      await observe(repo2, { kind: `publish-${result.status}`, error: "error" in result ? result.error : void 0 });
    }
    if (failed.length > 0) await observe(repo2, { kind: "publish-add-failed", paths: failed });
    if (result.status === "published" && result.blocked) await observe(repo2, { kind: "publish-blocked", paths: result.blocked });
    if (result.status === "published" && result.syncSkipped) await observe(repo2, { kind: "sync-skipped", error: result.syncSkipped });
    const published = result.status === "published";
    const notes = [...extraNotes];
    const reapply = published && result.reapply && result.reapply.paths.length > 0 ? result.reapply : void 0;
    if (reapply) {
      const short = reapply.backup.slice(0, 12);
      notes.push(
        `${reapply.paths.join(", ")} received newer content that overlaps your latest edit, so that edit is not in the file now. This is expected. Your version is saved in commit ${short} (git show ${short}). Re-read the file and apply your change again on top of the new content.`
      );
      await observe(repo2, { kind: "reapply", paths: reapply.paths, backup: reapply.backup });
    }
    const pulled = ("pulled" in result ? result.pulled ?? [] : []).filter((p) => p !== edited && !reapply?.paths.includes(p));
    if (!final && pulled.length > 0) {
      const list = quoteList(pulled);
      notes.push(
        `Newer content arrived for ${list} while you worked. If what you are doing depends on these files, re-read them and make your code match the new version. This is expected; no need to ask the user.`
      );
      await observe(repo2, { kind: "pulled", paths: pulled });
    }
    if (team && pulled.length > 0) await updateState(sessionId, (s) => void (s.arrived = [.../* @__PURE__ */ new Set([...s.arrived ?? [], ...pulled])].slice(-500)));
    const stale = team && final ? staleDependencies(repo2.root, paths, state.reads ?? [], [.../* @__PURE__ */ new Set([...state.arrived ?? [], ...pulled])]) : void 0;
    if (stale) {
      notes.push(stale.text);
      await observe(repo2, { kind: "stale-dependency", paths: stale.paths });
    }
    if (notes.length > 0 && !final) output("PostToolUse", { additionalContext: notes.join(" ") });
    else if ((reapply || stale) && final && !input.stop_hook_active && agent === "claude") write({ decision: "block", reason: notes.join(" ") });
    const ranges = published && result.base && teamContextOn(repo2.root) ? changedRanges(repo2.root, result.base, result.commit, result.paths.slice(0, 50)) : void 0;
    await post(config, "/v1/publish", { ...ref(repo2), commit: published ? result.commit : void 0, paths: published ? result.paths.slice(0, MAX_REPORTED_PATHS) : [], release: edited ? [edited] : void 0, final, ranges }, 5e3);
  };
  const acquireDeadline = Date.now() + ACQUIRE_WAIT_MS;
  const editOne = async (file2) => {
    const abs = path6.resolve(cwd, file2);
    const loc = locate(abs);
    if (!loc) return;
    if (event === "post") {
      markEditInFlight(loc.repo.root, sessionId, loc.path, false);
      const extra = [];
      const known2 = loadState(sessionId);
      const turnStart = ["Write", "write_file", "Create"].includes(input.tool_name ?? "") ? known2.bases?.find(([p]) => p === loc.path)?.[1] ?? known2.turnStart : void 0;
      if (turnStart) {
        const merged = await withLock(checkoutLock(loc.repo.root), () => mergeStaleWrite(loc.repo.root, loc.path, turnStart), LOCK_WAIT_MS);
        if (merged === "merged") {
          extra.push(`${loc.path} was replaced as a whole from an older copy: lines teammates added meanwhile were put back into it. This is expected. Re-read the file if your next step depends on it.`);
          await observe(loc.repo, { kind: "write-merged", path: loc.path });
        } else if (merged === "conflict") await observe(loc.repo, { kind: "write-conflict", path: loc.path });
      }
      await publish(loc.repo, false, loc.path, extra);
      if (agent === "cursor") markEditInFlight(loc.repo.root, sessionId, CURSOR_TURN, true);
      const head = tryGit(loc.repo.root, ["rev-parse", "HEAD"]);
      if (head) await updateState(sessionId, (s) => void (s.bases = [...(s.bases ?? []).filter(([p]) => p !== loc.path), [loc.path, head]].slice(-200)));
      return;
    }
    if (event === "permission") {
      markEditInFlight(loc.repo.root, sessionId, loc.path, true);
      await post(config, "/v1/hold", { ...ref(loc.repo), tool: input.tool_name ?? "unknown", paths: [loc.path], holdMs: APPROVAL_HOLD_MS }, 2e3);
      return;
    }
    const body = { ...ref(loc.repo), tool: input.tool_name ?? "unknown", paths: [loc.path], hookMs: +performance.now().toFixed(1), waitMs: Math.max(acquireDeadline - Date.now(), 0) };
    const started = Date.now();
    let res;
    try {
      res = await post(config, "/v1/acquire", body, ACQUIRE_WAIT_MS + 1e4);
    } catch (err) {
      appendLedger(loc.repo.id, "failopen", Date.now() - started);
      throw err;
    }
    if (res.decision === "timeout") {
      appendLedger(loc.repo.id, "failopen", res.waitedMs);
      await observe(loc.repo, { kind: "wait-overrun", path: loc.path, waitedMs: res.waitedMs });
      return;
    }
    appendLedger(loc.repo.id, res.waitedMs > 0 ? "queued" : "ok", res.waitedMs);
    const notes = [];
    if (teamContextOn(loc.repo.root)) {
      let range;
      try {
        range = editSpan(fs6.readFileSync(abs, "utf8"), input.tool_input);
      } catch {
      }
      const heads = await post(config, "/v1/team/context", { ...ref(loc.repo), path: loc.path, range }, 2e3).catch(() => void 0);
      if (heads?.text) notes.push(heads.text);
    }
    const say = () => notes.length > 0 && output("PreToolUse", { additionalContext: notes.join(" ") });
    const missed = takeUnreported(loc.repo.id);
    if (missed > 0) await observe(loc.repo, { kind: "uncoordinated", count: missed });
    const version = res.versions[loc.path];
    const known = loadState(sessionId);
    const restarted = res.epoch !== known.epoch;
    if (restarted) await updateState(sessionId, (s) => void (s.epoch = res.epoch));
    let behind = restarted;
    if (version) {
      behind = false;
      if (COMMIT_ID.test(version.commit) && !known.seen?.includes(version.commit)) {
        behind = !isAncestor(loc.repo.root, version.commit) && !isOwnPublished(loc.repo.root, version.commit);
        if (!behind) await updateState(sessionId, (s) => void (s.seen = [...(s.seen ?? []).slice(-99), version.commit]));
      }
    }
    if (behind) {
      const before = fileHash(abs);
      const head = tryGit(loc.repo.root, ["rev-parse", "HEAD"]);
      const synced = await withLock(checkoutLock(loc.repo.root), () => syncFromIntegration(loc.repo.root, identity(loc.repo), sessionId), LOCK_WAIT_MS);
      const came = synced.ok && head && teamContextOn(loc.repo.root) ? (tryGit(loc.repo.root, ["diff", "--name-only", "--no-renames", head, "HEAD"]) ?? "").split("\n").filter(Boolean) : [];
      if (came.length > 0) await updateState(sessionId, (s) => void (s.arrived = [.../* @__PURE__ */ new Set([...s.arrived ?? [], ...came])].slice(-500)));
      if (!synced.ok) {
        if (!synced.skipped) await observe(loc.repo, { kind: "sync-failed", path: loc.path, error: synced.error });
        say();
        return;
      }
      const changed = fileHash(abs) !== before;
      await observe(loc.repo, { kind: "synced", path: loc.path, from: version?.user, changed });
      if (changed) {
        notes.unshift(`${loc.path} has new content since you last read it. This is expected: edits to this file are coordinated automatically, so the new content should stay and needs no checking from you or the user. Re-read it with the Read tool, then make your edit.`);
      }
    }
    say();
    return;
  };
  if (event === "pre" || event === "permission" || event === "post") {
    let failed;
    const files = editedFiles(input);
    buffered = files.length > 1;
    for (const file2 of files) await editOne(file2).catch((err) => void (failed ??= err));
    if (event === "pre") {
      for (const file2 of files) {
        const loc = locate(path6.resolve(cwd, file2));
        if (loc) await withLock(checkoutLock(loc.repo.root), () => markEditInFlight(loc.repo.root, sessionId, loc.path, true), LOCK_WAIT_MS).catch(() => {
        });
      }
    }
    if (failed) throw failed;
    return;
  }
  const repo = findRepo(cwd);
  if (!repo) return;
  if (event === "stop" || event === "end") markEditInFlight(repo.root, sessionId, void 0, false);
  if (event === "stop") {
    await publish(repo, true);
    return;
  }
  if (event === "start") {
    output("SessionStart", {
      additionalContext: "In this repository, each file edit you make is saved automatically as a commit on your branch, named after the current request. You don't need to commit. Those commits are your own edits; this is expected."
    });
  }
  let registerError;
  await post(config, "/v1/session", { ...ref(repo), event }, 2e3).catch((err) => void (registerError = err));
  if (event === "start") {
    process.env.LANES_NET_TIMEOUT_MS ??= "2000";
    const synced = await withLock(checkoutLock(repo.root), () => syncFromIntegration(repo.root, identity(repo), sessionId), 2e3);
    if (!synced.ok && !synced.skipped) await observe(repo, { kind: "sync-failed", path: "(session start)", error: synced.error });
  }
  if (registerError) throw registerError;
}
if (true) {
  const event = process.argv[2] ?? "";
  if (process.env.LANES_OFF === "1") process.exit(0);
  if (!EVENTS.includes(event)) {
    process.stderr.write(`lanes hook: unknown event "${event}"
`);
    process.exit(0);
  }
  agent = AGENT_NAMES.includes(process.argv[4]) ? process.argv[4] : "claude";
  if (agent === "copilot" && isCopilotCloud(process.env)) process.exit(0);
  let raw = {};
  try {
    raw = JSON.parse(fs6.readFileSync(0, "utf8") || "{}");
  } catch {
  }
  if (agent === "claude" && raw.cursor_version) process.exit(0);
  if (agent === "claude" && isCopilotClaudeFormat(raw)) process.exit(0);
  const input = fromAgent(agent, raw);
  const config = loadConfig();
  run(event, input, config).then(
    () => {
      flush(event);
      process.exit(0);
    },
    // Fail open (ADR-0019): never refuse an edit. Exit 0 keeps stderr out of the agent's context; the views show "offline".
    (err) => {
      flush(event);
      process.stderr.write(`lanes hook: coordination paused, server ${config.serverUrl} unreachable (${err.message})
`);
      process.exit(0);
    }
  );
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  checkoutLock,
  contextOutput,
  defaultOutput,
  editSpan,
  editedFiles,
  fromAgent,
  loadConfig,
  loadState,
  predictFiles,
  pruneSessions,
  redactSecrets,
  staleDependencies,
  updateState
});
