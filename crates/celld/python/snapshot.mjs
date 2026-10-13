// Builds the base Pyodide memory snapshot that every Python VM restores
// instead of booting its own interpreter (`application_pyodide.rs`). It runs
// in a VM of the node's own from the pinned Pyodide files on the shared drive,
// with no handler code, and answers with the snapshot.
//
// The loader and the interpreter's script are the pinned files, which the
// node's drive wraps in the same names a bundled Python Worker gets from
// esbuild, so this boots the interpreter in the environment a handler does.
import { RUNTIME_URL } from "./shims.js";
import { LOCK } from "./config.js";
import "./pyodide.asm.js";
import { loadPyodide } from "./pyodide.mjs";

export default {
  async fetch() {
    const pyodide = await loadPyodide({
      indexURL: RUNTIME_URL,
      lockFileContents: LOCK,
      jsglobals: globalThis,
      // The interpreter environment of `python/worker.js`, which is baked
      // into the snapshot: keep the two the same.
      env: { HOME: "/session", PYTHONHASHSEED: "111" },
      _makeSnapshot: true,
    });
    // Streamed: a buffered response is bounded far below the snapshot's size.
    const snapshot = pyodide.makeMemorySnapshot();
    let offset = 0;
    return new Response(
      new ReadableStream({
        pull(controller) {
          if (offset >= snapshot.length) return controller.close();
          controller.enqueue(snapshot.subarray(offset, (offset += 1 << 20)));
        },
      }),
    );
  },
};
