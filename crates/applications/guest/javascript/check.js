// Real-VM fixture for the original runtime. The host owns every database,
// grant, ownership decision and output position; this file has no credentials.
globalThis.constructed = 0;
globalThis.releaseDone = false;
globalThis.releaseLateDone = false;
globalThis.releaseDenied = false;
globalThis.releaseMode = '';

export class Counter {
  constructor(ctx) {
    this.ctx = ctx;
    this.count = 0;
    constructed++;
    ctx.facets._release = async () => {
      if (releaseMode === '/retain-never') {
        console.log('CELLD_JS_RELEASE_PENDING_WITHOUT_IO');
        await new Promise(() => {});
      }
      if (releaseMode === '/retain-throw') throw new Error('release failed');
      Promise.allSettled = () => Promise.resolve([]);
      ctx.waitUntil((async () => {
        await new Promise(resolve => setTimeout(resolve, 15));
        ctx.waitUntil((async () => {
          await new Promise(resolve => setTimeout(resolve, 15));
          releaseLateDone = true;
          console.log('CELLD_JS_RELEASE_LATE_DONE');
        })());
      })());
      await new Promise(resolve => setTimeout(resolve, 15));
      try { ctx.storage.sql.exec('CREATE TABLE forbidden(value)'); }
      catch(e) { releaseDenied = e.message.includes('lifecycle channel refuses'); }
      releaseDone = true;
      console.log('CELLD_JS_RELEASE_DONE ' + releaseDenied);
    };
    ctx.blockConcurrencyWhile(async () => {
      await new Promise(resolve => setTimeout(resolve, 2));
      ctx.storage.sql.exec('CREATE TABLE IF NOT EXISTS fixture(value INTEGER)');
      if (ctx.storage.sql.exec('SELECT count(*) AS count FROM fixture').one().count === 0)
        ctx.storage.sql.exec('INSERT INTO fixture VALUES(11),(22),(33)');
      this.saved = ctx.storage.sql.exec('SELECT value FROM fixture ORDER BY value').raw();
    });
  }

  async fetch(request) {
    const mode = new URL(request.url).pathname;
    if (['/cancel-native', '/cancel-async', '/cancel-background', '/cancel-never', '/cancel-throw', '/cancel-late'].includes(mode)) {
      await this.ctx.storage.put('acknowledged', 9);
      await this.ctx.storage.put('acknowledged:' + mode, 9);
      const run = () => this.ctx.storage.transaction(async tx => {
        await tx.put('uncommitted', 99);
        let aborted = false, runs = 0, denied = false;
        try {
          console.log('CELLD_JS_CANCEL_WAITING ' + mode);
          await new Promise((resolve, reject) => {
            const abort = () => { aborted = request.signal.aborted; reject(new Error('cancelled by signal')); };
            if (request.signal.aborted) abort();
            else request.signal.addEventListener('abort', abort, { once: true });
          });
        } finally {
          runs++;
          if (mode === '/cancel-never') {
            console.log('CELLD_JS_CANCEL_PENDING_WITHOUT_IO');
            await new Promise(() => {});
          }
          if (mode === '/cancel-async' || mode === '/cancel-background') {
            // Cleanup completion must not trust a handler-overridden aggregate.
            Promise.allSettled = () => Promise.resolve([]);
            let late = 0;
            this.ctx.waitUntil((async () => {
              await new Promise(resolve => setTimeout(resolve, 15));
              late++;
              this.ctx.waitUntil((async () => {
                await new Promise(resolve => setTimeout(resolve, 15));
                late++;
                console.log('CELLD_JS_CANCEL_LATE_DONE ' + late);
              })());
            })());
            await new Promise(resolve => setTimeout(resolve, 15));
          }
          try { this.ctx.storage.sql.exec('CREATE TABLE forbidden(value)'); }
          catch(e) { denied = e.message.includes('lifecycle channel refuses') || e.message.includes("this event's storage is closed"); }
          console.log('CELLD_JS_CANCEL_READY_CLEANUP ' + JSON.stringify({ aborted, runs, denied }));
          if (mode === '/cancel-throw') throw new Error('cancel cleanup failure');
        }
      });
      if (mode === '/cancel-late') {
        // The reply escapes first; the transaction opens only afterwards.
        this.ctx.waitUntil((async () => {
          await new Promise(resolve => setTimeout(resolve, 50));
          await run();
        })());
        return new Response('late');
      }
      if (mode === '/cancel-background') {
        this.ctx.waitUntil(run());
        return new Response('background');
      }
      await run();
      return new Response('must not reply');
    }
    if (mode.startsWith('/retain')) {
      releaseMode = mode;
      this.saved = this.ctx.storage.sql.exec('SELECT value FROM fixture ORDER BY value').raw();
      return new Response(JSON.stringify({ phase: 'retained', row: this.saved.next().value[0] }));
    }
    if (mode === '/inspect') {
      return new Response(JSON.stringify({ phase: 'inspected', count: this.count, constructed,
        persisted: String(await this.ctx.storage.get('count')), rows: [...this.saved].map(row => row[0]),
        name: this.ctx.id.name, releaseDone, releaseLateDone, releaseDenied }));
    }
    if (mode === '/abort') {
      await this.ctx.storage.put('abort-prefix', true);
      console.log('CELLD_JS_ABORTING');
      this.ctx.abort('fixture abort');
      return new Response('must not reply');
    }
    if (mode === '/hold') {
      this.ctx.blockConcurrencyWhile(async () => {
        await this.ctx.storage.put('hold-entered', true);
        console.log('CELLD_JS_HOLD_ENTERED');
        await new Promise(resolve => setTimeout(resolve, 150));
        await this.ctx.storage.put('hold-released', true);
      });
      return new Response(JSON.stringify({ phase: 'holding' }));
    }
    if (mode === '/slow') {
      await this.ctx.storage.put('slow-entered', true);
      await new Promise(resolve => setTimeout(resolve, 200));
      return new Response(JSON.stringify({ phase: 'slow' }));
    }
    const count = ++this.count;
    await this.ctx.storage.transaction(async tx => {
      await tx.put('count', BigInt(count));
      await new Promise(resolve => setTimeout(resolve, 2));
      if (await tx.get('count') !== BigInt(count)) throw new Error('clone mismatch');
    });
    const row = this.saved.next();
    if (row.done) throw new Error('saved cursor retired too early');
    this.ctx.waitUntil((async () => {
      await new Promise(resolve => setTimeout(resolve, 2));
      await this.ctx.storage.put('background', count);
    })());
    const response = new Response(JSON.stringify({ count, row: row.value[0], constructed,
      name: this.ctx.id.name, language: 'original-v8' }));
    const headers = response.headers;
    let decoded = false;
    Object.defineProperty(response, 'headers', { get: () => {
      if (!decoded) {
        decoded = true;
        this.ctx.storage.sql.exec('CREATE TABLE IF NOT EXISTS response_owner(value INTEGER)');
        this.ctx.storage.sql.exec('INSERT INTO response_owner VALUES(?)', count);
        this.ctx.waitUntil((async () => {
          await new Promise(resolve => setTimeout(resolve, 15));
          await this.ctx.storage.put('response-background', count);
          console.log('CELLD_JS_RESPONSE_BACKGROUND ' + count);
        })());
      }
      return headers;
    }});
    return response;
  }
}

export class QuiescenceCounter {
  constructor(ctx) { this.ctx = ctx; }
  async fetch(request) {
    const mode = new URL(request.url).pathname;
    if (mode === '/quiescence-wait' || mode === '/quiescence-fail') {
      // An application-controlled aggregate cannot certify event retirement.
      Promise.allSettled = () => Promise.resolve([]);
      this.ctx.waitUntil((async () => {
        await new Promise(resolve => { this.quiescenceResume = resolve; });
        this.ctx.waitUntil((async () => {
          await new Promise(resolve => setTimeout(resolve, 30));
          await this.ctx.storage.put('quiescence-nested', mode);
          console.log('CELLD_JS_QUIESCENCE_NESTED ' + mode);
        })());
        await this.ctx.storage.put('quiescence-background', mode);
        console.log('CELLD_JS_QUIESCENCE_BACKGROUND ' + mode);
      })());
      await this.ctx.storage.put('quiescence-prefix', mode);
      console.log('CELLD_JS_QUIESCENCE_WAITING ' + mode);
      if (mode === '/quiescence-fail') throw new Error('quiescence handler failure');
      return new Response('early');
    }
    if (mode === '/quiescence-resolve') {
      this.quiescenceResume();
      return new Response('peer');
    }
    if (mode === '/quiescence-inspect') {
      return new Response(JSON.stringify({
        prefix: await this.ctx.storage.get('quiescence-prefix'),
        background: await this.ctx.storage.get('quiescence-background'),
        nested: await this.ctx.storage.get('quiescence-nested'),
      }));
    }
    throw new Error('unknown quiescence fixture mode');
  }
}

// Interleaved events on one object. A's block runs in B's checkpoint, so B
// owns its native gate while A's grant performs its effects. E's foreign
// continuations run inside D's transaction and must be refused, not parked.
export class CallbackCounter {
  constructor(ctx) { this.ctx = ctx; }
  async fetch(request) {
    const mode = new URL(request.url).pathname;
    const ctx = this.ctx;
    if (mode === '/callback-wait') {
      const resumed = new Promise(resolve => { this.resumeBlock = resolve; });
      console.log('CELLD_JS_CALLBACK_WAITING');
      await resumed;
      // B's checkpoint also holds A's savepoints for A's grant.
      ctx.storage.transactionSync(() => {
        ctx.storage.sql.exec('CREATE TABLE callback_rows(value INTEGER)');
        ctx.storage.sql.exec('INSERT INTO callback_rows VALUES(1)');
      });
      await ctx.blockConcurrencyWhile(async () => {
        await ctx.storage.put('callback-critical-entered', true);
        console.log('CELLD_JS_CALLBACK_CRITICAL_ENTERED');
        await new Promise(resolve => setTimeout(resolve, 40));
        await ctx.storage.transaction(async tx => {
          await tx.put('callback-transaction', 'A');
          await new Promise(resolve => setTimeout(resolve, 5));
          if (await tx.get('callback-transaction') !== 'A') throw new Error('delegated transaction mismatch');
        });
        await ctx.storage.put('callback-critical', 'A');
        console.log('CELLD_JS_CALLBACK_CRITICAL_DONE');
      });
      return new Response('A:' + await ctx.storage.get('callback-critical'));
    }
    if (mode === '/callback-drive') {
      this.resumeBlock();
      await new Promise(resolve => setTimeout(resolve, 5));
      await ctx.storage.put('callback-driver', 1);
      return new Response('B');
    }
    if (mode === '/callback-ping') {
      return new Response(String(await ctx.storage.get('callback-critical')));
    }
    if (mode === '/callback-tx-wait') {
      const second = new Promise(resolve => { this.resumeSecond = resolve; });
      const first = new Promise(resolve => { this.resumeFirst = resolve; });
      console.log('CELLD_JS_CALLBACK_TX_WAITING');
      await first;
      const read = async () => {
        try { return String(await ctx.storage.get('callback-tx-uncommitted')); }
        catch (error) { return error.message; }
      };
      const foreign = await read();
      const late = second.then(read);
      await new Promise(resolve => setTimeout(resolve, 1));
      const own = await read();
      await ctx.storage.put('callback-tx-foreign', 'own-turn');
      return new Response(JSON.stringify({ foreign, late: await late, own }));
    }
    if (mode === '/callback-tx-hold') {
      try {
        await ctx.storage.transaction(async tx => {
          await tx.put('callback-tx-uncommitted', 'D');
          this.resumeFirst();
          await new Promise(resolve => setTimeout(resolve, 30));
          this.resumeSecond();
          await new Promise(resolve => setTimeout(resolve, 5));
          throw new Error('callback rollback');
        });
      } catch (error) {
        return new Response('rolled-back:' + error.message);
      }
      return new Response('committed');
    }
    if (mode === '/callback-inspect') {
      return new Response(JSON.stringify({
        critical: await ctx.storage.get('callback-critical'),
        transaction: await ctx.storage.get('callback-transaction'),
        rows: ctx.storage.sql.exec('SELECT count(*) AS n FROM callback_rows').one().n,
        driver: await ctx.storage.get('callback-driver'),
        foreign: await ctx.storage.get('callback-tx-foreign'),
        uncommitted: String(await ctx.storage.get('callback-tx-uncommitted')),
      }));
    }
    throw new Error('unknown callback fixture mode');
  }
}

// A failed constructor fails only its event; the next event constructs again.
globalThis.alternateFailed = false;
export class Alternate {
  constructor(ctx) {
    if (!alternateFailed) {
      alternateFailed = true;
      throw new Error('fixture constructor failure');
    }
    this.ctx = ctx;
  }
  async fetch() {
    await this.ctx.storage.put('alternate-constructed', true);
    return new Response('constructed');
  }
}

export class StreamCounter {
  constructor(ctx) { this.ctx = ctx; }
  async fetch(request) {
    const mode = new URL(request.url).pathname;
    const ctx = this.ctx;
    if (mode === '/ping') {
      await ctx.storage.put('stream-peer', true);
      return new Response('peer');
    }
    if (mode === '/inspect') {
      return new Response(JSON.stringify({produced: await ctx.storage.get('stream-produced'),
        background: await ctx.storage.get('stream-background'),
        rows: ctx.storage.sql.exec('SELECT count(*) AS n FROM stream_rows').one().n}));
    }
    ctx.storage.sql.exec('CREATE TABLE IF NOT EXISTS stream_rows(value INTEGER)');
    let count = 0;
    ctx.waitUntil((async () => {
      await new Promise(resolve => setTimeout(resolve, 10));
      await ctx.storage.put('stream-background', true);
    })());
    return new Response(new ReadableStream({
      async pull(controller) {
        if (mode === '/error' && count === 3) throw new Error('private producer diagnostic');
        if (count === 257) { controller.close(); return; }
        ++count;
        await ctx.storage.put('stream-produced', count);
        ctx.storage.sql.exec('INSERT INTO stream_rows VALUES(?)', count);
        await new Promise(resolve => setTimeout(resolve, 1));
        controller.enqueue(new Uint8Array(65536).fill(count & 255));
      }
    }), {status: 201, headers: [['set-cookie', 'one'], ['set-cookie', 'two']]});
  }
}

export default { fetch() { return new Response('guest'); } };
