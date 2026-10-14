// Plain freeze observations; this fixture never requests GC or changes clocks.
let calls = 0;
let timer = null;
const identity = `${Date.now()}:${Math.random()}`;
const retained = new Uint8Array(8 * 1024 * 1024);
for (let i = 0; i < retained.length; i += 4096) retained[i] = 77;
function clocks() { return {wallMs: Date.now(), performanceMs: performance.now()}; }
function status() {
  let checksum = 0;
  for (let i = 0; i < retained.length; i += 4096) checksum += retained[i];
  return {identity, calls, checksum, clocks: clocks(), timer};
}
export default {
  async fetch(request, _env, ctx) {
    calls++;
    const path = new URL(request.url).pathname;
    if (path === '/arm') {
      const observation = {armed: clocks(), completed: null};
      timer = observation;
      ctx.waitUntil(new Promise(resolve => setTimeout(() => {
        observation.completed = clocks();
        resolve();
      }, 500)));
    } else if (path === '/slow') {
      await new Promise(resolve => setTimeout(resolve, 2000));
    } else if (path !== '/status') {
      return new Response('unknown operation', {status: 404});
    }
    return Response.json(status());
  }
};