#!/usr/bin/env node
'use strict';

const crypto = require('crypto');
const fs = require('fs');
const readline = require('readline');
const { types: utilTypes } = require('util');
const vm = require('vm');

const { webcrypto } = crypto;

const WORKER_NAME_PREFIX = process.env.SOURCE_A_WORKER_PREFIX || 'stream';
const DEFAULT_ORIGIN = (process.env.SOURCE_A_URL || 'https://example-source.com').replace(/\/+$/, '');
const DEFAULT_PATH_PREFIX = process.env.SOURCE_A_PATH_PREFIX || '/dist/engine';

const moduleScriptCache = new Map();
const pendingModuleFetches = new Map();
let isStreamingMode = false;

function requestModuleScript(url) {
  if (moduleScriptCache.has(url)) {
    return Promise.resolve(moduleScriptCache.get(url));
  }
  const existing = pendingModuleFetches.get(url);
  if (existing) {
    return existing.promise;
  }
  if (!isStreamingMode) {
    return Promise.reject(new Error(`Module script not available in --once mode: ${url}`));
  }

  let resolveFn;
  let rejectFn;
  const promise = new Promise((res, rej) => {
    resolveFn = res;
    rejectFn = rej;
  });
  const timer = setTimeout(() => {
    pendingModuleFetches.delete(url);
    rejectFn(new Error(`Timed out waiting for module script: ${url}`));
  }, 15000);

  pendingModuleFetches.set(url, { promise, resolve: resolveFn, reject: rejectFn, timer });
  process.stdout.write(JSON.stringify({ type: 'fetch_module', url }) + '\n');
  return promise;
}

function resolveModuleScript(msg) {
  const url = msg && msg.url;
  if (!url) return;
  const entry = pendingModuleFetches.get(url);
  if (entry) {
    clearTimeout(entry.timer);
    pendingModuleFetches.delete(url);
  }
  if (msg.error) {
    if (entry) entry.reject(new Error(String(msg.error)));
    return;
  }
  const code = typeof msg.code === 'string' ? msg.code : '';
  if (!code) {
    if (entry) entry.reject(new Error(`Empty module script for ${url}`));
    return;
  }
  moduleScriptCache.set(url, code);
  if (entry) entry.resolve(code);
}

function detachArrayBuffers(transfer) {
  const ports = [];
  const abMap = new Map();
  if (!Array.isArray(transfer)) {
    return { ports, abMap };
  }
  for (const item of transfer) {
    if (item instanceof VirtualMessagePort) {
      ports.push(item);
    } else if (utilTypes.isAnyArrayBuffer(item)) {
      const cloned = structuredClone(item, { transfer: [item] });
      abMap.set(item, new Uint8Array(cloned));
    }
  }
  return { ports, abMap };
}

class TransferredBytesToken {
  constructor(bytes) {
    this.bytes = bytes;
  }
}

function snapshotValueWithBuffers(val, abMap, seen = new WeakMap()) {
  if (val === null || typeof val !== 'object') {
    return val;
  }
  if (abMap.has(val)) {
    return new TransferredBytesToken(abMap.get(val));
  }
  if (val instanceof globalThis.CryptoKey || val instanceof VirtualMessagePort) {
    return val;
  }
  if (seen.has(val)) {
    return seen.get(val);
  }
  if (Array.isArray(val)) {
    const out = [];
    seen.set(val, out);
    for (let i = 0; i < val.length; i++) {
      out.push(snapshotValueWithBuffers(val[i], abMap, seen));
    }
    return out;
  }
  const out = {};
  seen.set(val, out);
  for (const key of Object.keys(val)) {
    out[key] = snapshotValueWithBuffers(val[key], abMap, seen);
  }
  return out;
}

function materializeValueInContext(val, ctx, seen = new WeakMap()) {
  if (val === null || typeof val !== 'object') {
    return val;
  }
  if (val instanceof TransferredBytesToken) {
    if (!ctx) return new Uint8Array(val.bytes).buffer;
    const ChildUint8Array = vm.runInContext('Uint8Array', ctx);
    return new ChildUint8Array(val.bytes).buffer;
  }
  if (val instanceof globalThis.CryptoKey || val instanceof VirtualMessagePort) {
    return val;
  }
  if (seen.has(val)) {
    return seen.get(val);
  }
  if (Array.isArray(val)) {
    const ChildArray = ctx ? vm.runInContext('Array', ctx) : Array;
    const out = new ChildArray();
    seen.set(val, out);
    for (let i = 0; i < val.length; i++) {
      out.push(materializeValueInContext(val[i], ctx, seen));
    }
    return out;
  }
  const ChildObject = ctx ? vm.runInContext('Object', ctx) : Object;
  const out = new ChildObject();
  seen.set(val, out);
  for (const key of Object.keys(val)) {
    out[key] = materializeValueInContext(val[key], ctx, seen);
  }
  return out;
}

function toHex(buf) {
  return Buffer.from(buf).toString('hex');
}

async function sha256Hex(uint8OrBuf) {
  const digest = await webcrypto.subtle.digest('SHA-256', uint8OrBuf);
  return toHex(digest);
}

async function hmacSha256Hex(key, text) {
  const bytes = new TextEncoder().encode(text);
  const sig = await webcrypto.subtle.sign('HMAC', key, bytes);
  return toHex(sig);
}

const listenerStore = new WeakMap();

class VirtualEventTarget {
  addEventListener(type, fn, opts) {
    if (typeof fn !== 'function') return;
    let map = listenerStore.get(this);
    if (!map) {
      map = new Map();
      listenerStore.set(this, map);
    }
    let list = map.get(type);
    if (!list) {
      list = [];
      map.set(type, list);
    }
    const once = Boolean(opts && typeof opts === 'object' && opts.once);
    list.push({ fn, once });
  }

  removeEventListener(type, fn) {
    const map = listenerStore.get(this);
    if (!map) return;
    const list = map.get(type);
    if (!list) return;
    map.set(type, list.filter((item) => item.fn !== fn));
  }

  _dispatch(ev) {
    const prop = this['on' + ev.type];
    if (typeof prop === 'function') {
      try {
        prop.call(this, ev);
      } catch (e) {
        if (process.env.BRIDGE_WORKER_DEBUG === '1') {
          console.error('[event on' + ev.type + ' error]', e);
        }
      }
    }
    const map = listenerStore.get(this);
    if (!map) return;
    const list = map.get(ev.type);
    if (!list || list.length === 0) return;
    for (const item of list.slice()) {
      if (item.once) {
        VirtualEventTarget.prototype.removeEventListener.call(this, ev.type, item.fn);
      }
      try {
        item.fn.call(this, ev);
      } catch (e) {
        if (process.env.BRIDGE_WORKER_DEBUG === '1') {
          console.error('[event listener error]', e);
        }
      }
    }
  }
}

class VirtualMessageEvent {
  constructor(type, init = {}) {
    this._type = String(type);
    this._data = init.data;
    this._ports = Array.isArray(init.ports) ? init.ports : [];
    this._origin = init.origin || '';
    this._source = init.source ?? null;
    this._lastEventId = init.lastEventId || '';
    this._isTrusted = Boolean(init._trusted);
  }

  get type() {
    return this._type;
  }

  get data() {
    return this._data;
  }

  get ports() {
    return this._ports;
  }

  get origin() {
    return this._origin;
  }

  get source() {
    return this._source;
  }

  get lastEventId() {
    return this._lastEventId;
  }

  get isTrusted() {
    return this._isTrusted;
  }
}

class VirtualMessagePort extends VirtualEventTarget {
  constructor() {
    super();
    this._peer = null;
    this._onmessage = null;
    this._started = false;
    this._queue = [];
    this._closed = false;
  }

  get onmessage() {
    return this._onmessage;
  }

  set onmessage(fn) {
    this._onmessage = typeof fn === 'function' ? fn : null;
    if (this._onmessage) {
      this.start();
    }
  }

  start() {
    if (this._started) return;
    this._started = true;
    while (this._queue.length > 0) {
      const ev = this._queue.shift();
      queueMicrotask(() => this._deliver(ev));
    }
  }

  close() {
    this._closed = true;
    this._queue.length = 0;
    if (typeof this.onclose === 'function') {
      try {
        this.onclose();
      } catch (_) {}
    }
  }

  postMessage(data, transfer) {
    if (this._closed || !this._peer || this._peer._closed) return;
    const { ports, abMap } = detachArrayBuffers(transfer);
    const dataPayload =
      abMap.size > 0
        ? materializeValueInContext(
            snapshotValueWithBuffers(data, abMap),
            this._peer._targetCtx || null
          )
        : data;
    const peer = this._peer;
    const payload = { data: dataPayload, ports };
    if (!peer._started) {
      peer._queue.push(payload);
    } else {
      queueMicrotask(() => peer._deliver(payload));
    }
  }

  _deliver(payload) {
    if (this._closed) return;
    const ev = new VirtualMessageEvent('message', {
      data: payload.data,
      ports: payload.ports,
      origin: '',
      _trusted: true,
    });
    this._dispatch(ev);
  }
}

class VirtualMessageChannel {
  constructor() {
    this.port1 = new VirtualMessagePort();
    this.port2 = new VirtualMessagePort();
    this.port1._peer = this.port2;
    this.port2._peer = this.port1;
  }
}

class WorkerGlobalScope extends VirtualEventTarget {}
class DedicatedWorkerGlobalScope extends WorkerGlobalScope {}

class VirtualWorker extends VirtualEventTarget {
  constructor(url, opts = {}, parentSession = null) {
    super();
    this._parentSession = parentSession;
    const baseUrl = (parentSession && parentSession.workerUrl) || `${DEFAULT_ORIGIN}/`;
    this._url = new URL(String(url), baseUrl).href;
    this._opts = opts && typeof opts === 'object' ? opts : {};
    this._name =
      typeof this._opts.name === 'string' && this._opts.name
        ? this._opts.name
        : `${WORKER_NAME_PREFIX}-yggdrasil-module`;
    this._terminated = false;
    this._childSession = null;
    this._queuedMsgs = [];
    this._flushing = false;
    this._bootError = null;
    if (parentSession && parentSession.childWorkers) {
      parentSession.childWorkers.add(this);
    }
    this._bootPromise = this._boot();
  }

  async _boot() {
    await Promise.resolve();
    if (this._terminated || (this._parentSession && this._parentSession.closed)) {
      return;
    }
    try {
      const code = await requestModuleScript(this._url);
      if (this._terminated || (this._parentSession && this._parentSession.closed)) {
        return;
      }
      const ua = this._parentSession ? this._parentSession.userAgent : 'Mozilla/5.0';
      const protoVer = this._parentSession ? this._parentSession.protocolVersion : 5;
      const childSession = new WorkerSession(this._url, this._name, code, ua, protoVer);
      await childSession.prepareVm();
      if (this._terminated || (this._parentSession && this._parentSession.closed)) {
        childSession.destroy();
        return;
      }
      this._childSession = childSession;
    } catch (err) {
      this._bootError = err;
      if (!this._terminated) {
        const errEv = new VirtualMessageEvent('error', {
          data: String(err && err.message ? err.message : err),
          _trusted: true,
        });
        VirtualEventTarget.prototype._dispatch.call(this, errEv);
      }
    }
  }

  postMessage(data, transfer) {
    if (this._terminated) return;
    const { ports, abMap } = detachArrayBuffers(transfer);
    const dataSnap = snapshotValueWithBuffers(data, abMap);
    this._queuedMsgs.push({ dataSnap, ports });
    this._flush();
  }

  async _flush() {
    if (this._flushing) return;
    this._flushing = true;
    try {
      await this._bootPromise;
      if (this._terminated || !this._childSession) return;
      const childOrigin = makeLocationObject(this._url).origin;
      while (!this._terminated && this._childSession && this._queuedMsgs.length > 0) {
        const item = this._queuedMsgs.shift();
        const materialized = materializeValueInContext(
          item.dataSnap,
          this._childSession.ctx
        );
        const ev = new VirtualMessageEvent('message', {
          origin: childOrigin,
          source: null,
          lastEventId: '',
          ports: item.ports,
          data: materialized,
          _trusted: true,
        });
        VirtualEventTarget.prototype._dispatch.call(this._childSession.vmSelf, ev);
      }
    } catch (err) {
      if (!this._terminated) {
        const errEv = new VirtualMessageEvent('error', {
          data: String(err && err.message ? err.message : err),
          _trusted: true,
        });
        VirtualEventTarget.prototype._dispatch.call(this, errEv);
      }
    } finally {
      this._flushing = false;
    }
  }

  terminate() {
    this._terminated = true;
    this._queuedMsgs.length = 0;
    if (this._childSession) {
      this._childSession.destroy();
      this._childSession = null;
    }
    if (this._parentSession && this._parentSession.childWorkers) {
      this._parentSession.childWorkers.delete(this);
    }
  }
}

function makeLocationObject(urlStr) {
  const u = new URL(urlStr);
  const loc = {
    href: u.href,
    origin: u.origin,
    protocol: u.protocol,
    host: u.host,
    hostname: u.hostname,
    port: u.port,
    pathname: u.pathname,
    search: u.search,
    hash: u.hash,
    toString() {
      return u.href;
    },
    valueOf() {
      return u.href;
    },
  };
  return loc;
}

class WorkerSession {
  constructor(workerUrl, workerName, workerCode, userAgent, protocolVersion) {
    this.workerUrl = workerUrl;
    this.workerName = workerName;
    this.workerCode = workerCode;
    this.userAgent = userAgent || 'Mozilla/5.0';
    this.protocolVersion =
      protocolVersion !== undefined
        ? Number(protocolVersion)
        : workerName === `${WORKER_NAME_PREFIX}-yggdrasil`
          ? 5
          : 1;
    this.codeHash = crypto.createHash('sha256').update(workerCode).digest('hex');
    this.sessionKey = null;
    this.sessionRequestId = '';
    this.expiresAt = 0;
    this.seq = 0;
    this.closed = false;
    this.port = null;
    this.pendingQueue = [];
    this.readyPromise = null;
    this.ctx = null;
    this.vmSelf = null;
    this.workerListeners = [];
    this.workerOnmessage = null;
    this.timers = new Set();
    this.childWorkers = new Set();
  }

  isAlive() {
    return (
      !this.closed &&
      this.port !== null &&
      !this.port._closed &&
      this.port._peer !== null &&
      !this.port._peer._closed &&
      this.sessionKey !== null &&
      Date.now() < this.expiresAt - 2000
    );
  }

  destroy() {
    this.closed = true;
    for (const id of this.timers) {
      clearTimeout(id);
      clearInterval(id);
    }
    this.timers.clear();
    for (const child of Array.from(this.childWorkers)) {
      try {
        child.terminate();
      } catch (_) {}
    }
    this.childWorkers.clear();
    if (this.port) {
      try {
        this.port.close();
      } catch (_) {}
      this.port = null;
    }
    while (this.pendingQueue.length > 0) {
      const p = this.pendingQueue.shift();
      clearTimeout(p.timer);
      p.reject(new Error('Worker session destroyed'));
    }
  }

  async prepareVm() {
    const locObj = makeLocationObject(this.workerUrl);
    const timers = this.timers;
    const outboundMessages = [];
    const workerSession = this;

    class SandboxWorker extends VirtualWorker {
      constructor(url, opts) {
        super(url, opts, workerSession);
      }
    }

    const sandbox = {
      name: this.workerName,
      location: locObj,
      origin: locObj.origin,
      isSecureContext: true,
      EventTarget: VirtualEventTarget,
      WorkerGlobalScope,
      DedicatedWorkerGlobalScope,
      Worker: SandboxWorker,
      navigator: {
        userAgent: this.userAgent,
        language: 'pt-BR',
        languages: ['pt-BR', 'en-US', 'en'],
        platform: 'MacIntel',
        hardwareConcurrency: 8,
      },
      crypto: {
        getRandomValues(arr) {
          return webcrypto.getRandomValues(arr);
        },
        randomUUID() {
          return webcrypto.randomUUID();
        },
        subtle: webcrypto.subtle,
      },
      CryptoKey: globalThis.CryptoKey,
      Crypto: globalThis.Crypto,
      SubtleCrypto: globalThis.SubtleCrypto,
      TextEncoder,
      TextDecoder,
      URL,
      URLSearchParams,
      atob: globalThis.atob.bind(globalThis),
      btoa: globalThis.btoa.bind(globalThis),
      structuredClone: globalThis.structuredClone.bind(globalThis),
      MessageChannel: VirtualMessageChannel,
      MessagePort: VirtualMessagePort,
      MessageEvent: VirtualMessageEvent,
      performance: {
        now: () => performance.now(),
        timeOrigin: performance.timeOrigin,
      },
      console: {
        log: () => {},
        info: () => {},
        warn: () => {},
        debug: () => {},
        error: (...args) => {
          if (process.env.BRIDGE_WORKER_DEBUG === '1') {
            console.error('[worker console.error]', ...args);
          }
        },
      },
      importScripts() {
        throw new Error('importScripts is disabled');
      },
      setTimeout(fn, ms, ...args) {
        const id = setTimeout(() => {
          timers.delete(id);
          fn(...args);
        }, ms);
        timers.add(id);
        return id;
      },
      clearTimeout(id) {
        timers.delete(id);
        clearTimeout(id);
      },
      setInterval(fn, ms, ...args) {
        const id = setInterval(() => fn(...args), ms);
        timers.add(id);
        return id;
      },
      clearInterval(id) {
        timers.delete(id);
        clearInterval(id);
      },
      queueMicrotask: globalThis.queueMicrotask.bind(globalThis),
      postMessage(msg) {
        outboundMessages.push(msg);
      },
      close() {
        workerSession.closed = true;
      },
    };

    Object.defineProperty(sandbox, 'onmessage', {
      configurable: true,
      enumerable: true,
      get() {
        return workerSession.workerOnmessage;
      },
      set(fn) {
        workerSession.workerOnmessage = typeof fn === 'function' ? fn : null;
      },
    });

    const ctx = vm.createContext(sandbox);
    const vmSelf = vm.runInContext(
      'this.self = this; this.globalThis = this; Object.setPrototypeOf(this, DedicatedWorkerGlobalScope.prototype); this',
      ctx
    );
    this.ctx = ctx;
    this.vmSelf = vmSelf;

    vm.runInContext(this.workerCode, ctx, { filename: this.workerUrl });
    return this;
  }

  async init() {
    await this.prepareVm();
    const locObj = makeLocationObject(this.workerUrl);
    const ctx = this.ctx;
    const vmSelf = this.vmSelf;

    this.sessionKey = await webcrypto.subtle.generateKey(
      { name: 'HMAC', hash: 'SHA-256', length: 256 },
      false,
      ['sign', 'verify']
    );

    const reqId = crypto.randomBytes(16).toString('hex');
    this.sessionRequestId = reqId;
    this.expiresAt = Date.now() + 12000;
    const probeBytes = webcrypto.getRandomValues(new Uint8Array(32));
    const expectedReadyHash = await sha256Hex(probeBytes);
    const VmUint8Array = vm.runInContext('Uint8Array', ctx);
    const vmProbeArr = new VmUint8Array(probeBytes);

    const mc = new VirtualMessageChannel();
    this.port = mc.port1;
    mc.port2.onclose = () => {
      this.closed = true;
    };

    let resolveReady;
    let rejectReady;
    this.readyPromise = new Promise((res, rej) => {
      resolveReady = res;
      rejectReady = rej;
    });

    const timer = setTimeout(() => {
      rejectReady(new Error(`Worker ${this.workerName} handshake timed out`));
    }, 5000);

    mc.port1.onmessage = (ev) => {
      const msg = ev && ev.data;
      if (!msg || typeof msg !== 'object') return;
      if (msg.operation === 'ready') {
        clearTimeout(timer);
        if (msg.body !== expectedReadyHash) {
          rejectReady(
            new Error(
              `Ready probe mismatch for ${this.workerName}: got ${msg.body}, expected ${expectedReadyHash}`
            )
          );
          return;
        }
        resolveReady(msg);
        return;
      }
      if (this.pendingQueue.length > 0) {
        const entry = this.pendingQueue.shift();
        clearTimeout(entry.timer);
        entry.resolve(msg);
      }
    };

    const connectEv = new VirtualMessageEvent('message', {
      origin: locObj.origin,
      source: null,
      lastEventId: '',
      ports: [mc.port2],
      data: {
        version: this.protocolVersion,
        operation: 'connect',
        requestId: reqId,
        expiresAt: this.expiresAt,
        sessionKey: this.sessionKey,
        environmentProbe: vmProbeArr.buffer,
      },
      _trusted: true,
    });

    VirtualEventTarget.prototype._dispatch.call(vmSelf, connectEv);

    await this.readyPromise;
    return this;
  }

  async call(operation, payloadObj, timeoutMs = 8000) {
    if (!this.port || !this.sessionKey) {
      throw new Error('Worker session not initialized');
    }
    this.seq += 1;
    const seq = this.seq;
    const reqId = this.sessionRequestId;
    const exp = this.expiresAt;
    const version = this.protocolVersion;
    const bodyStr = JSON.stringify(payloadObj);
    const macInput = JSON.stringify([
      this.workerName,
      version,
      operation,
      reqId,
      seq,
      exp,
      bodyStr,
    ]);
    const mac = await hmacSha256Hex(this.sessionKey, macInput);

    const response = await new Promise((resolve, reject) => {
      const entry = { resolve, reject, timer: null };
      entry.timer = setTimeout(() => {
        const idx = this.pendingQueue.indexOf(entry);
        if (idx >= 0) this.pendingQueue.splice(idx, 1);
        reject(new Error(`Worker RPC timeout (${this.workerName} ${operation})`));
      }, timeoutMs);
      this.pendingQueue.push(entry);
      this.port.postMessage({
        version,
        operation,
        requestId: reqId,
        sequence: seq,
        expiresAt: exp,
        body: bodyStr,
        mac,
      });
    });

    if (response.ok === false) {
      throw new Error(
        `Worker RPC failed (${this.workerName} ${operation}): ${response.error || response.body || JSON.stringify(response)}`
      );
    }

    if (typeof response.body === 'string') {
      let parsed;
      try {
        parsed = JSON.parse(response.body);
      } catch (_) {
        return response.body;
      }
      if (parsed && typeof parsed === 'object' && 'ok' in parsed) {
        if (!parsed.ok) {
          throw new Error(
            `Worker operation error (${this.workerName} ${operation}): ${parsed.error || JSON.stringify(parsed)}`
          );
        }
        return parsed.result;
      }
      return parsed;
    }
    return response;
  }
}

const sessionCache = new Map();

function loadWorkerCode(req) {
  if (typeof req.workerCode === 'string' && req.workerCode.length > 0) {
    return req.workerCode;
  }
  if (typeof req.workerPath === 'string' && req.workerPath.length > 0) {
    return fs.readFileSync(req.workerPath, 'utf8');
  }
  throw new Error('Missing workerCode or workerPath');
}

async function getWorkerSession(workerUrl, workerName, workerCode, userAgent) {
  const codeHash = crypto.createHash('sha256').update(workerCode).digest('hex');
  const cacheKey = `${workerName}|${workerUrl}|${userAgent || ''}|${codeHash}`;
  const existing = sessionCache.get(cacheKey);
  if (existing && existing.isAlive()) {
    return existing;
  }
  if (existing) {
    existing.destroy();
    sessionCache.delete(cacheKey);
  }
  const session = new WorkerSession(workerUrl, workerName, workerCode, userAgent);
  await session.init();
  sessionCache.set(cacheKey, session);
  return session;
}

function detectImageMime(buf) {
  if (buf.length >= 4 && buf[0] === 0xff && buf[1] === 0xd8 && buf[2] === 0xff) {
    return 'image/jpeg';
  }
  if (
    buf.length >= 8 &&
    buf[0] === 0x89 &&
    buf[1] === 0x50 &&
    buf[2] === 0x4e &&
    buf[3] === 0x47
  ) {
    return 'image/png';
  }
  if (
    buf.length >= 12 &&
    buf.subarray(0, 4).toString('ascii') === 'RIFF' &&
    buf.subarray(8, 12).toString('ascii') === 'WEBP'
  ) {
    return 'image/webp';
  }
  if (buf.length >= 4 && buf.subarray(0, 4).toString('ascii') === 'GIF8') {
    return 'image/gif';
  }
  if (buf.length >= 12 && buf.subarray(4, 8).toString('ascii') === 'ftyp') {
    return 'image/avif';
  }
  return 'application/octet-stream';
}

function decryptSkpgEnvelope(req) {
  const raw = Buffer.from(req.binBase64, 'base64');
  if (raw.length < 34) {
    throw new Error(`SKPG buffer too short: ${raw.length}`);
  }
  const magic = raw.subarray(0, 6);
  const v1 = Buffer.from([0x53, 0x4b, 0x50, 0x47, 0x01, 0x01]);
  const v2 = Buffer.from([0x53, 0x4b, 0x50, 0x47, 0x01, 0x02]);
  if (!magic.equals(v1) && !magic.equals(v2)) {
    throw new Error(`Invalid SKPG header: ${magic.toString('hex')}`);
  }

  const key = Buffer.from(req.aesKeyBase64, 'base64');
  if (key.length !== 32) {
    throw new Error(`Invalid AES-256 key length: ${key.length}`);
  }

  const iv = raw.subarray(6, 18);
  const ciphertext = raw.subarray(18, raw.length - 16);
  const tag = raw.subarray(raw.length - 16);

  const aad = Buffer.alloc(18);
  magic.copy(aad, 0);
  aad.writeBigUInt64BE(BigInt(req.chapterId), 6);
  aad.writeUInt32BE(Number(req.pageNumber) >>> 0, 14);

  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAAD(aad);
  decipher.setAuthTag(tag);
  const plain = Buffer.concat([decipher.update(ciphertext), decipher.final()]);

  return {
    dataBase64: plain.toString('base64'),
    mimeType: detectImageMime(plain),
    size: plain.length,
  };
}

async function handleRequest(req) {
  const op = req.op;
  if (op === 'hydra_prove') {
    const surface = req.surface || 'manga';
    const workerUrl =
      req.workerUrl ||
      `${DEFAULT_ORIGIN}${DEFAULT_PATH_PREFIX}/__hydra/${surface}/${req.workerFile}.js?transport=3`;
    const workerCode = loadWorkerCode(req);
    const session = await getWorkerSession(
      workerUrl,
      `${WORKER_NAME_PREFIX}-hydra`,
      workerCode,
      req.userAgent
    );
    return session.call('prove', {
      challenge: req.challenge,
      userAgent: req.userAgent,
    });
  }

  if (op === 'kaguya_transform') {
    const workerUrl =
      req.workerUrl ||
      `${DEFAULT_ORIGIN}${DEFAULT_PATH_PREFIX}/__kaguya/${req.workerFile}.js?transport=3`;
    const workerCode = loadWorkerCode(req);
    const session = await getWorkerSession(
      workerUrl,
      `${WORKER_NAME_PREFIX}-kaguya`,
      workerCode,
      req.userAgent
    );
    return session.call('transform', {
      action: req.action,
      data: req.data,
      key: req.key,
      mode: req.mode !== undefined ? req.mode : 0,
      challenge: req.challenge !== undefined ? req.challenge : '',
    });
  }

  if (req.moduleScripts && typeof req.moduleScripts === 'object') {
    for (const [mUrl, mCode] of Object.entries(req.moduleScripts)) {
      if (typeof mCode === 'string' && mCode.length > 0) {
        moduleScriptCache.set(mUrl, mCode);
      }
    }
  }

  if (op === 'yggdrasil_decipher') {
    const workerUrl =
      req.workerUrl ||
      `${DEFAULT_ORIGIN}${DEFAULT_PATH_PREFIX}/__yggdrasil/yggdrasil-nw.js?transport=5`;
    const workerCode = loadWorkerCode(req);
    const session = new WorkerSession(
      workerUrl,
      `${WORKER_NAME_PREFIX}-yggdrasil`,
      workerCode,
      req.userAgent,
      5
    );
    try {
      await session.init();
      return await session.call(
        req.operation || 'decipher',
        {
          data: req.data,
          key: req.key,
        },
        20000
      );
    } finally {
      session.destroy();
    }
  }

  if (op === 'skpg_decrypt') {
    return decryptSkpgEnvelope(req);
  }

  if (op === 'ping') {
    return { pong: true, cachedSessions: sessionCache.size };
  }

  throw new Error(`Unknown op: ${op}`);
}

async function main() {
  if (process.argv.includes('--once')) {
    const input = fs.readFileSync(0, 'utf8').trim();
    const req = JSON.parse(input);
    try {
      const result = await handleRequest(req);
      process.stdout.write(JSON.stringify({ ok: true, result }) + '\n');
    } catch (err) {
      process.stdout.write(
        JSON.stringify({ ok: false, error: String(err && err.stack ? err.stack : err) }) + '\n'
      );
      process.exitCode = 1;
    }
    return;
  }

  isStreamingMode = true;
  const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  rl.on('line', (line) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    let req;
    try {
      req = JSON.parse(trimmed);
    } catch (err) {
      process.stdout.write(
        JSON.stringify({
          id: null,
          ok: false,
          error: String(err && err.message ? err.message : err),
        }) + '\n'
      );
      return;
    }
    if (req && req.type === 'module_script') {
      resolveModuleScript(req);
      return;
    }
    const id = req.id ?? null;
    handleRequest(req)
      .then((result) => {
        process.stdout.write(JSON.stringify({ id, ok: true, result }) + '\n');
      })
      .catch((err) => {
        process.stdout.write(
          JSON.stringify({
            id,
            ok: false,
            error: String(err && err.message ? err.message : err),
          }) + '\n'
        );
      });
  });
  rl.on('close', () => {
    for (const session of sessionCache.values()) {
      session.destroy();
    }
    process.exit(0);
  });
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { handleRequest };
