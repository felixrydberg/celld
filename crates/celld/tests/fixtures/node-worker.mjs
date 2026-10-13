import { DurableObject, WorkflowEntrypoint, env } from 'cloudflare:workers';
import { NonRetryableError } from 'cloudflare:workflows';
// A dynamically loaded facet class, as the actual-VM facets check loads one:
// its table is a database and replication stream of its own below the object.
// `pad` is an incompressible payload, so a write's size shows in a follower's log.
const facetCode = `
  import { DurableObject } from 'cloudflare:workers';
  export class F extends DurableObject {
    add(k, pad) {
      const sql = this.ctx.storage.sql;
      sql.exec('CREATE TABLE IF NOT EXISTS t(v INTEGER, pad BLOB)');
      sql.exec('INSERT INTO t VALUES(?, randomblob(?))', k, pad);
      return k;
    }
    rows() {
      const sql = this.ctx.storage.sql;
      sql.exec('CREATE TABLE IF NOT EXISTS t(v INTEGER, pad BLOB)');
      return sql.exec('SELECT v FROM t ORDER BY v').toArray().map(row => row.v);
    }
  }
  export default { fetch() { return new Response('f'); } };
`;
export class Counter extends DurableObject {
  facet() {
    const cls = this.env.LOADER
      .get('code', () => ({ compatibilityDate: '2026-03-01', mainModule: 'main.js', modules: { 'main.js': facetCode } }))
      .getDurableObjectClass('F');
    return this.ctx.facets.get('child', () => ({ class: cls }));
  }
  // The root's write (when asked) and the facet's are separate commits.
  async facetWrite(n, withRoot, pad) {
    if (withRoot) await this.ctx.storage.put('froot:' + n, n);
    return await this.facet().add(n, pad);
  }
  async facetState() {
    return { root: [...(await this.ctx.storage.list({ prefix: 'froot:' })).values()], facet: await this.facet().rows() };
  }
  async fetch() {
    const count = (await this.ctx.storage.get('count') ?? 0) + 1;
    await this.ctx.storage.put('count', count);
    return new Response(`${this.ctx.id.name}:${count}`);
  }
  async add(a, b) { return { sum: a + b, name: this.ctx.id.name }; }
  async count() { return (await this.ctx.storage.get('count')) ?? 0; }
  async fail() { const error = new RangeError('rpc failed'); error.code = 7; throw error; }
  async armAndWait() {
    const fired = new Promise(resolve => { this.alarmDone = resolve; });
    await this.ctx.storage.setAlarm(Date.now() + 150);
    await fired;
    return this.alarmState();
  }
  async arm(delay, failOnce = false) {
    await this.ctx.storage.put('alarmAttempts', 0);
    await this.ctx.storage.put('alarmFailOnce', failOnce);
    await this.ctx.storage.setAlarm(Date.now() + delay);
    return 'armed';
  }
  async rollbackAlarm(delay = 100) {
    try { await this.ctx.storage.transaction(async txn => {
      await txn.setAlarm(Date.now() + delay);
      throw new Error('rollback alarm');
    }); } catch (error) { if (error?.message !== 'rollback alarm') throw error; }
    return this.alarmState();
  }
  async cancel() { await this.ctx.storage.deleteAlarm(); return this.alarmState(); }
  async alarm(event) {
    const attempt = (await this.ctx.storage.get('alarmAttempts') ?? 0) + 1;
    const hidden = (await this.ctx.storage.getAlarm()) === null;
    await this.ctx.storage.put({alarmAttempts: attempt, alarmHidden: hidden,
      alarmRetryCount: event.retryCount, alarmIsRetry: event.isRetry});
    if (await this.ctx.storage.get('alarmFailOnce') && attempt === 1) throw new Error('alarm retry');
    this.alarmDone?.();
  }
  // Commits `acked`, then holds an open transaction that wrote `uncommitted`
  // until the test kills the VM.
  async holdOpen() {
    await this.ctx.storage.put('acked', 1);
    await this.ctx.storage.transaction(async txn => {
      await txn.put('uncommitted', 1);
      await new Promise(resolve => setTimeout(resolve, 120000));
    });
  }
  async crashState() {
    return {acked: await this.ctx.storage.get('acked') ?? 0, uncommitted: await this.ctx.storage.get('uncommitted') ?? 0};
  }
  async alarmState() {
    return {attempts: await this.ctx.storage.get('alarmAttempts') ?? 0,
      hidden: await this.ctx.storage.get('alarmHidden') ?? false,
      retryCount: await this.ctx.storage.get('alarmRetryCount') ?? 0,
      isRetry: await this.ctx.storage.get('alarmIsRetry') ?? false,
      alarm: await this.ctx.storage.getAlarm()};
  }
}
export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
  }
  async fetch(request) {
    const url = new URL(request.url);
    if (url.searchParams.has('peek')) {
      return new Response(`${(await this.ctx.storage.get('messages')) ?? 0}:${(await this.ctx.storage.get('closed')) ?? ''}:${this.ctx.getWebSockets().length}`);
    }
    if (request.headers.get('upgrade') !== 'websocket') return new Response('no upgrade', { status: 426 });
    const [client, server] = Object.values(new WebSocketPair());
    const tag = url.searchParams.get('tag') ?? 'room';
    this.ctx.acceptWebSocket(server, [tag]);
    server.serializeAttachment({ joined: tag, n: 0 });
    server.send('welcome:' + this.ctx.getWebSockets().length);
    return new Response(null, { status: 101, webSocket: client });
  }
  async webSocketMessage(ws, message) {
    const attachment = ws.deserializeAttachment();
    attachment.n += 1;
    ws.serializeAttachment(attachment);
    const count = ((await this.ctx.storage.get('messages')) ?? 0) + 1;
    await this.ctx.storage.put('messages', count);
    if (message === 'all') {
      for (const peer of this.ctx.getWebSockets(attachment.joined)) peer.send('broadcast:' + attachment.joined);
    } else if (message === 'close') {
      ws.close(1000, 'bye');
    } else {
      ws.send('echo:' + message + ':' + attachment.n + ':' + count + ':' + this.ctx.getTags(ws).join(','));
    }
  }
  async webSocketClose(ws, code, reason, wasClean) {
    await this.ctx.storage.put('closed', code + ':' + reason + ':' + wasClean);
  }
}
export class Flow extends WorkflowEntrypoint {
  async run(event, step) {
    const prefix = 'wf-' + event.instanceId;
    const first = await step.do('once', async () => {
      await this.env.COUNTER.getByName(prefix + '-once').fetch('http://counter/');
      return await this.env.COUNTER.getByName(prefix + '-once').count();
    });
    if (event.payload.mode === 'fail')
      return await step.do('permanent', () => { throw new NonRetryableError('workflow permanent failure'); });
    if (event.payload.mode === 'cancel') {
      await step.sleep('cancel-wait', '1 day');
      await this.env.COUNTER.getByName(prefix + '-after').fetch('http://counter/');
    }
    await step.sleep('replay', 100);
    const attempts = await step.do('retry', {retries: {limit: 1, delay: 100, backoff: 'constant'}}, async () => {
      const counter = this.env.COUNTER.getByName(prefix + '-retry');
      await counter.fetch('http://counter/');
      const attempt = await counter.count();
      if (attempt === 1) throw new Error('retry this durable step');
      return attempt;
    });
    if (event.payload.mode === 'event' || event.payload.mode === 'long') {
      const incoming = await step.waitForEvent('event', {type: 'go', timeout: event.payload.mode === 'long' ? '10 minutes' : '1 minute'});
      return {first, attempts, label: this.env.LABEL, payload: incoming.payload};
    }
    return {first, attempts, label: this.env.LABEL};
  }
}
export default {
  async fetch(request, _env, ctx) {
    const url = new URL(request.url);
    if (url.pathname.startsWith('/wf-')) {
      const id = url.searchParams.get('id');
      try {
        if (url.pathname === '/wf-create') {
          const instance = await env.FLOW.create({id, params: {mode: url.searchParams.get('mode')}});
          return Response.json({id: instance.id});
        }
        if (url.pathname === '/wf-counts') return Response.json({
          first: await env.COUNTER.getByName('wf-' + id + '-once').count(),
          retry: await env.COUNTER.getByName('wf-' + id + '-retry').count(),
          after: await env.COUNTER.getByName('wf-' + id + '-after').count(),
        });
        const instance = await env.FLOW.get(id);
        if (url.pathname === '/wf-terminate') await instance.terminate();
        if (url.pathname === '/wf-event') await instance.sendEvent({type: 'go', payload: env.LABEL});
        if (url.pathname === '/wf-pause') await instance.pause();
        if (url.pathname === '/wf-resume') await instance.resume();
        return Response.json(await instance.status());
      } catch (error) { return Response.json({error: String(error.message)}); }
    }
    const stub = env.COUNTER.getByName(url.searchParams.get('name'));
    switch (url.pathname) {
      case '/dial': {
        // An outbound socket the node dials for the guest: text, then a binary
        // frame above one credit's chunk bound, echoed back, then a close.
        const ws = new WebSocket(url.searchParams.get('url'));
        const seen = [];
        const code = await new Promise((resolve, reject) => {
          ws.addEventListener('open', () => ws.send('hello'));
          ws.addEventListener('message', (event) => {
            seen.push(typeof event.data === 'string' ? event.data : event.data.byteLength);
            if (seen.length === 1) ws.send(new Uint8Array(100000)); else ws.close(1000, 'done');
          });
          ws.addEventListener('close', (event) => resolve(event.code));
          ws.addEventListener('error', () => reject(new Error('socket error')));
        });
        return Response.json({ protocol: ws.protocol, seen, code });
      }
      case '/ws': return env.ROOM.getByName(url.searchParams.get('name')).fetch(request);
      case '/ws-peek': return env.ROOM.getByName(url.searchParams.get('name')).fetch('http://room/?peek=1');
      case '/alarm-wait': return Response.json(await stub.armAndWait());
      case '/alarm-arm': return new Response(await stub.arm(Number(url.searchParams.get('delay') ?? 100), url.searchParams.has('fail')));
      case '/alarm-state': return Response.json(await stub.alarmState());
      case '/alarm-rollback': return Response.json(await stub.rollbackAlarm(Number(url.searchParams.get('delay') ?? 100)));
      case '/alarm-cancel': return Response.json(await stub.cancel());
      case '/hello': return new Response('hello ' + env.LABEL);
      case '/slow':
        await new Promise(resolve => setTimeout(resolve, +url.searchParams.get('ms')));
        return new Response('slow ' + env.LABEL);
      case '/slow-bump':
        await new Promise(resolve => setTimeout(resolve, +url.searchParams.get('ms')));
        return stub.fetch('http://object/bump');
      // Replies at once; the row is written by retained background work.
      case '/bg':
        ctx.waitUntil((async () => {
          await new Promise(resolve => setTimeout(resolve, +url.searchParams.get('ms')));
          await env.DB.exec('CREATE TABLE IF NOT EXISTS bg (label TEXT)');
          await env.DB.prepare('INSERT INTO bg VALUES (?)').bind(env.LABEL).run();
        })());
        return new Response('bg ' + env.LABEL);
      case '/bg-read':
        await env.DB.exec('CREATE TABLE IF NOT EXISTS bg (label TEXT)');
        return Response.json(await env.DB.prepare('SELECT label FROM bg ORDER BY rowid').raw());
      case '/r2-peek': {
        const object = await env.BLOBS.get(url.searchParams.get('key') ?? 'record');
        return Response.json(object === null ? null : await object.text());
      }
      case '/r2': {
        const bucket = env.BLOBS;
        const options = {httpMetadata: {contentType: 'text/plain'}, customMetadata: {label: env.LABEL}};
        await bucket.put('record', 'hello ' + env.LABEL, options);
        const head = await bucket.head('record');
        const ranged = await bucket.get('record', {range: {suffix: env.LABEL.length}});
        const unmet = await bucket.get('record', {onlyIf: {etagDoesNotMatch: head.etag}});
        const refused = await bucket.put('record', 'wrong', {onlyIf: {etagMatches: 'wrong'}});
        let checksumFailed = false;
        try { await bucket.put('record', 'wrong', {md5: new Uint8Array(16)}); }
        catch { checksumFailed = true; }
        const size = 8 * 1024 * 1024 + 1;
        let produced = 0;
        await bucket.put('stream', new ReadableStream({pull(controller) {
          const count = Math.min(65536, size - produced);
          if (count === 0) { controller.close(); return; }
          controller.enqueue(new Uint8Array(count).fill(7)); produced += count;
        }}), options);
        const streamed = new Uint8Array(await (await bucket.get('stream')).arrayBuffer());
        let cancelled = false;
        try { await bucket.put('cancelled', new ReadableStream({pull(controller) {
          controller.error(new Error('source cancelled'));
        }})); } catch { cancelled = true; }
        const upload = await bucket.createMultipartUpload('multipart', options);
        const second = await upload.uploadPart(2, 'tail');
        const resumed = bucket.resumeMultipartUpload('multipart', upload.uploadId);
        const first = await resumed.uploadPart(1, new Uint8Array(5 * 1024 * 1024).fill(3));
        await resumed.complete([first, second]);
        const multipart = await bucket.get('multipart', {range: {suffix: 4}});
        const aborted = await bucket.createMultipartUpload('aborted');
        await aborted.abort(); await aborted.abort();
        const page = await bucket.list({limit: 1, include: ['customMetadata', 'httpMetadata']});
        const next = await bucket.list({cursor: page.cursor});
        const out = {text: await (await bucket.get('record')).text(), range: await ranged.text(),
          custom: head.customMetadata, type: head.httpMetadata.contentType,
          unmet: !('body' in unmet), refused: refused === null, checksumFailed,
          stream: [streamed.length, streamed[0], streamed[streamed.length - 1]], produced,
          cancelled, absent: await bucket.head('cancelled') === null,
          multipart: await multipart.text(), aborted: await bucket.head('aborted') === null,
          pages: [page.objects.length, page.truncated, next.objects.length, next.truncated]};
        await bucket.delete(['stream', 'multipart', 'missing']);
        out.deleted = await bucket.head('stream') === null;
        return Response.json(out);
      }
      case '/d1-setup':
        await env.DB.exec('CREATE TABLE c (n INTEGER); INSERT INTO c VALUES (0); CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)');
        return Response.json('ready');
      case '/d1-next':
        return Response.json(await env.DB.prepare('UPDATE c SET n = n + 1 RETURNING n').first('n'));
      case '/d1-read':
        return Response.json(await env.DB.prepare('SELECT n FROM c').first('n'));
      case '/d1-large-input': {
        const results = await env.DB.batch(Array.from({ length: 5 }, (_, index) =>
          env.DB.prepare('SELECT length(?) AS n').bind(String(index).repeat(1800000))));
        const larger = await env.DB.batch(Array.from({ length: 10 }, (_, index) =>
          env.DB.prepare('SELECT length(?) AS n').bind(String(index).repeat(1800000))));
        return Response.json({ values: results.map(result => result.results[0].n), larger: larger.map(result => result.results[0].n) });
      }
      case '/d1-large': {
        const { results } = await env.DB.prepare("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<20) SELECT i, replace(hex(zeroblob(900000)), '00', 'x') AS value FROM n ORDER BY i").all();
        return Response.json(results.map(({ i, value }) => [i, value.length, value.slice(0, 4), value.slice(-4)]));
      }
      case '/d1-batch': {
        const inserted = await env.DB.batch([
          env.DB.prepare('INSERT INTO t VALUES (?, ?)').bind(1, 'one'),
          env.DB.prepare('INSERT INTO t VALUES (?, ?)').bind(2, 'two'),
        ]);
        let failed;
        try {
          await env.DB.batch([
            env.DB.prepare('INSERT INTO t VALUES (?, ?)').bind(3, 'three'),
            env.DB.prepare('INSERT INTO t VALUES (?, ?)').bind(1, 'duplicate'),
          ]);
        } catch (error) { failed = { name: error.name, message: error.message, cause: error.cause.message }; }
        const session = env.DB.withSession('first-primary');
        const rows = await session.prepare('SELECT id, label FROM t ORDER BY id').raw();
        return Response.json({ changes: inserted.map(result => result.meta.changes), failed, rows, bookmark: session.getBookmark() });
      }
      case '/t23-d1-setup':
        await env.DB.exec('CREATE TABLE IF NOT EXISTS t23 (n INTEGER); INSERT INTO t23 SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM t23)');
        return Response.json('ready');
      case '/t23-d1-next':
        return Response.json(await env.DB.prepare('UPDATE t23 SET n = n + 1 RETURNING n').first('n'));
      case '/t23-d1-read':
        return Response.json(await env.DB.prepare('SELECT n FROM t23').first('n'));
      // The object holds an open transaction; a second object, written a second
      // later, tells the test when the first one is in flight.
      case '/crash-hold': {
        const held = stub.holdOpen();
        held.catch(() => {});
        await new Promise(resolve => setTimeout(resolve, 1000));
        await env.COUNTER.getByName(url.searchParams.get('name') + '-sig').fetch('http://counter/');
        return Response.json(await held);
      }
      case '/crash-state': return Response.json(await stub.crashState());
      case '/kv-seed': {
        await env.CACHE.put('seed', 'one');
        return Response.json(await env.CACHE.get('seed'));
      }
      case '/kv': {
        const { CACHE } = env;
        await CACHE.put('text', 'hello');
        await CACHE.put('json', JSON.stringify({ a: [1, 2] }), { metadata: { tag: 'm' } });
        for (const name of ['one', 'two', 'three']) await CACHE.put('p:' + name, name);
        await CACHE.put('ttl', 'soon', { expirationTtl: 60 });
        const first = await CACHE.list({ prefix: 'p:', limit: 2 });
        const second = await CACHE.list({ prefix: 'p:', cursor: first.cursor });
        const ttl = await CACHE.list({ prefix: 'ttl' });
        const out = {
          text: await CACHE.get('text'),
          json: await CACHE.get('json', 'json'),
          meta: await CACHE.getWithMetadata('json', 'json'),
          missing: await CACHE.get('absent'),
          pages: [first.keys.map(key => key.name), first.list_complete, second.keys.map(key => key.name), second.list_complete],
          ttl: [await CACHE.get('ttl'), ttl.keys[0].expiration - Math.floor(Date.now() / 1000)],
        };
        await CACHE.delete('text');
        out.deleted = await CACHE.get('text');
        return Response.json(out);
      }
      case '/kv-large': {
        const size = 25 * 1024 * 1024;
        const big = new Uint8Array(size).fill(7);
        big.set([1, 2, 3, 4]);
        big.set([5, 6, 7, 8], size - 4);
        await env.CACHE.put('big', big);
        const back = new Uint8Array(await env.CACHE.get('big', 'arrayBuffer'));
        return Response.json({ length: back.length, head: [...back.slice(0, 4)], tail: [...back.slice(-4)], middle: back[size >> 1] });
      }
      case '/kv-put': {
        const size = +url.searchParams.get('size');
        const bytes = new Uint8Array(size).fill(+url.searchParams.get('fill'));
        bytes.set([1, 2, 3, 4]);
        bytes.set([5, 6, 7, 8], size - 4);
        try { await env.CACHE.put(url.searchParams.get('key'), bytes); return Response.json({ ok: true }); }
        catch (error) { return Response.json({ failed: String(error.message) }); }
      }
      case '/kv-peek': {
        const value = await env.CACHE.get(url.searchParams.get('key'), 'arrayBuffer');
        if (value === null) return Response.json(null);
        const back = new Uint8Array(value);
        return Response.json({ length: back.length, head: [...back.slice(0, 4)], tail: [...back.slice(-4)], middle: back[back.length >> 1] });
      }
      case '/kv-keys':
        return Response.json((await env.CACHE.list({ prefix: url.searchParams.get('prefix') })).keys.map(key => key.name));
      case '/q-setup':
        await env.DB.exec('CREATE TABLE IF NOT EXISTS q (queue TEXT, n INTEGER, attempts INTEGER, size INTEGER, at INTEGER, sent INTEGER, label TEXT)');
        return Response.json('ready');
      case '/q-backlog': {
        // Messages `ns` of one kind, each delayed by `delay` seconds.
        const label = url.searchParams.get('label'), kind = url.searchParams.get('kind');
        for (const n of url.searchParams.get('ns').split(',').map(Number))
          await env.JOBS.send({ kind, n, label, sent: Date.now() }, { delaySeconds: +url.searchParams.get('delay') });
        return Response.json('sent');
      }
      case '/q-produce': {
        const label = url.searchParams.get('label');
        // One batch: two messages the consumer acknowledges before it throws for the third.
        await env.JOBS.sendBatch([{ body: { kind: 'ok', n: 1, label } }, { body: { kind: 'ok', n: 2, label } }, { body: { kind: 'poison', n: 3, label } }]);
        await env.JOBS.send({ kind: 'flaky', n: 4, label });
        await env.JOBS.send({ kind: 'ok', n: 5, label, sent: Date.now() }, { delaySeconds: 2 });
        return Response.json('sent');
      }
      case '/q-log':
        return Response.json(await env.DB.prepare('SELECT queue, n, attempts, size, at, sent, label FROM q ORDER BY rowid').raw());
      case '/t22': {
        // One request writes the object, D1, KV and the queue. The reply
        // exists as soon as the last write returns; only native proof of each
        // cell (a follower's disk or the bucket) may let it out.
        const n = +url.searchParams.get('n');
        await env.DB.exec('CREATE TABLE IF NOT EXISTS t22 (n INTEGER)');
        await env.DB.prepare('INSERT INTO t22 VALUES (?)').bind(n).run();
        await env.CACHE.put('t22:' + n, 'v' + n);
        await stub.fetch('http://object/bump');
        await env.JOBS.send({ kind: 'ok', n, label: 't22' });
        return Response.json(n);
      }
      // The facet phase: a root write and a facet write (`f-both`), or a facet
      // write alone (`f-only`). The reply exists as soon as the facet call returns;
      // only the facet stream's proof may let it out.
      case '/f-both':
      case '/f-only':
        return Response.json(await stub.facetWrite(+url.searchParams.get('n'), url.pathname === '/f-both', +(url.searchParams.get('pad') ?? 0)));
      case '/f-read': return Response.json(await stub.facetState());
      case '/t22-read':
        return Response.json({
          rows: (await env.DB.prepare('SELECT n FROM t22 ORDER BY n').raw()).flat(),
          keys: (await env.CACHE.list({ prefix: 't22:' })).keys.map(key => key.name),
          count: await stub.count(),
        });
      case '/cron-log':
        await env.DB.exec('CREATE TABLE IF NOT EXISTS cron (cron TEXT, at INTEGER, attempt INTEGER, label TEXT)');
        return Response.json(await env.DB.prepare('SELECT cron, at, attempt, label FROM cron ORDER BY rowid').raw());
      case '/asset-fetch': return env.ASSETS.fetch('https://assets.local/' + (url.searchParams.get('path') ?? 'a.txt'));
      case '/bump': return stub.fetch('http://object/bump');
      case '/rpc': return Response.json({ add: await stub.add(2, 3), count: await stub.count() });
      case '/fail':
        try { await stub.fail(); return new Response('returned'); }
        catch (error) { return Response.json({ name: error.name, code: error.code, remote: error.remote }); }
    }
    return new Response('objects');
  },
  // Every delivery is logged. `ok` is acknowledged, `flaky` retried once, and
  // `poison` fails the whole handler after the others settled, so it
  // reaches the dead-letter queue (which only logs) once its retries are spent.
  async queue(batch) {
    for (const message of batch.messages) {
      const { n, sent, label } = message.body;
      await env.DB.prepare('INSERT INTO q VALUES (?, ?, ?, ?, ?, ?, ?)')
        .bind(batch.queue, n, message.attempts, batch.messages.length, Date.now(), sent ?? null, label).run();
    }
    if (batch.queue === 'dead') return;
    let poison = false;
    for (const message of batch.messages) {
      if (message.body.kind === 'poison') poison = true;
      else if (message.body.kind === 'flaky' && message.attempts === 1) message.retry({ delaySeconds: 1 });
      // The first attempt of `hang` blocks until the test kills the VM.
      else if (message.body.kind === 'hang' && message.attempts === 1) await new Promise(resolve => setTimeout(resolve, 120000));
      else message.ack();
    }
    if (poison) throw new Error('poison');
  },
  // Every run is logged. The first attempt of each occurrence throws, so the
  // original retry runs it again with the same scheduledTime.
  async scheduled(controller) {
    await env.DB.exec('CREATE TABLE IF NOT EXISTS cron (cron TEXT, at INTEGER, attempt INTEGER, label TEXT)');
    const [[done]] = await env.DB.prepare('SELECT COUNT(*) FROM cron WHERE at = ?').bind(controller.scheduledTime).raw();
    await env.DB.prepare('INSERT INTO cron VALUES (?, ?, ?, ?)').bind(controller.cron, controller.scheduledTime, done + 1, env.LABEL).run();
    if (done === 0) throw new Error('first attempt');
  }
};
