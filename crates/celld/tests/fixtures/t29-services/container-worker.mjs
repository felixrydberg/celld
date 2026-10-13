
import { DurableObject } from 'cloudflare:workers';
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const text = (bytes) => new TextDecoder().decode(bytes);
export class Counter extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    const c = this.ctx.container;
    switch (url.pathname) {
      case '/running': return new Response(String(c.running));
      case '/start': {
        c.start({ env: { WHO: this.env.LABEL } });
        this.exited = c.monitor().then(() => 'exited', (error) => 'error:' + error.message);
        for (let i = 0; i < 100; i++) {
          try {
            const reply = await c.getTcpPort(8080).fetch('http://app.test/index.html');
            return new Response(await reply.text() + ':' + c.running);
          } catch { await sleep(100); }
        }
        return new Response('never listened', { status: 500 });
      }
      case '/mark': {
        const process = await c.exec(['sh', '-c', 'cat > /www/who'], { stdin: 'pipe' });
        const writer = process.stdin.getWriter();
        await writer.write(new TextEncoder().encode(url.searchParams.get('v')));
        await writer.close();
        return new Response(String(await process.exitCode));
      }
      case '/who': return new Response(await (await c.getTcpPort(8080).fetch('http://app.test/who')).text());
      case '/output': {
        const process = await c.exec(['sh', '-c', 'echo out; echo err >&2; exit 4']);
        const output = await process.output();
        return Response.json({ stdout: text(output.stdout), stderr: text(output.stderr), exit: output.exitCode });
      }
      case '/redirect': {
        const manual = await c.getTcpPort(8080).fetch('http://app.test/dir', { redirect: 'manual' });
        const followed = await c.getTcpPort(8080).fetch('http://app.test/dir');
        return Response.json({ status: manual.status, location: manual.headers.get('location'), followed: await followed.text() });
      }
      case '/echo': {
        const socket = c.getTcpPort(9000).connect('ignored:9000');
        const writer = socket.writable.getWriter();
        await writer.write(new TextEncoder().encode('ping'));
        const reader = socket.readable.getReader();
        let got = '';
        while (got.length < 4) {
          const { value, done } = await reader.read();
          if (done) break;
          got += text(value);
        }
        await writer.close();
        let rest = '';
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          rest += text(value);
        }
        return new Response(got + '|' + rest);
      }
      case '/tls':
        try { c.getTcpPort(9000).connect('ignored:9000', { secureTransport: 'on' }); return new Response('connected'); }
        catch (error) { return new Response(error.message); }
      case '/destroy': await c.destroy(); return new Response(String(c.running));
      case '/slow-start':
        c.start({ entrypoint: ['sleep', '3600'] });
        await new Promise(() => {});
        return new Response('unreachable');
    }
    return new Response('unknown', { status: 404 });
  }
}
export default { fetch(request, env) { return env.BOX.getByName('same').fetch(request); } };
