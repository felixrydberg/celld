// Bounded allocations for the trusted T33 memory benchmark; no GC requests.
let retained = null;
function allocate() {
  const objects = Array.from({length: 32768}, (_, i) => ({i, value: i ^ 0x5a5a}));
  const buffer = new Uint8Array(8 * 1024 * 1024);
  const wasm = new WebAssembly.Memory({initial: 64, maximum: 64});
  const wasmBytes = new Uint8Array(wasm.buffer);
  for (let i = 0; i < buffer.length; i += 4096) buffer[i] = 77;
  for (let i = 0; i < wasmBytes.length; i += 4096) wasmBytes[i] = 91;
  return {objects, buffer, wasm};
}
function inspect(value) {
  if (!value) return {retained: false};
  let checksum = 0;
  for (const object of value.objects) checksum += object.value;
  for (let i = 0; i < value.buffer.length; i += 4096) checksum += value.buffer[i];
  const wasmBytes = new Uint8Array(value.wasm.buffer);
  for (let i = 0; i < wasmBytes.length; i += 4096) checksum += wasmBytes[i];
  return {retained: true, objects: value.objects.length, bufferBytes: value.buffer.length,
          wasmBytes: wasmBytes.length, checksum};
}
export default {
  async fetch(request) {
    const operation = new URL(request.url).pathname;
    if (operation === "/temporary") return Response.json(inspect(allocate()));
    if (operation === "/retain") retained = allocate();
    else if (operation === "/release") retained = null;
    else if (operation === "/slow") await new Promise(resolve => setTimeout(resolve, 200));
    else if (operation !== "/status") return new Response("unknown operation", {status: 404});
    return Response.json(inspect(retained));
  }
};
