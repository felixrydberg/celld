// `cloudflare:sockets` -- outbound TCP over the __tcp_* host ops,
// following Workerd (src/workerd/api/sockets.c++): the same address
// parsing (a URL parse behind a fake scheme, so IPv6 works), the same
// validation messages, EOF closing the write side unless allowHalfOpen,
// and startTls() consuming the plaintext socket.
(function () {
  const validHost = (address) => {
    if (address.length === 0 || address.length > 255) return false;
    return /^[a-zA-Z0-9.\-_[\]:]+$/.test(address);
  };

  const parseAddress = (address) => {
    let hostname, port;
    if (typeof address === "string") {
      let url;
      try {
        url = new URL("fake://" + address);
      } catch {
        throw new TypeError("Specified address could not be parsed.");
      }
      if (!url.hostname) {
        throw new TypeError("Specified address is missing hostname.");
      }
      if (!url.port) {
        throw new TypeError("Specified address is missing port.");
      }
      hostname = url.hostname;
      port = Number(url.port);
    } else {
      hostname = String(address?.hostname ?? "");
      if (!hostname) {
        throw new TypeError("Specified address is missing hostname.");
      }
      if (address?.port === undefined || address?.port === null) {
        throw new TypeError("Specified address is missing port.");
      }
      port = Number(address.port);
    }
    const joined = typeof address === "string"
      ? address
      : hostname + ":" + port;
    if (!validHost(joined)) {
      throw new TypeError(
        "Specified address is empty string, contains unsupported " +
          "characters or is too long.");
    }
    return { hostname, port };
  };

  const parseSecureTransport = (options) => {
    const value = options?.secureTransport ?? "off";
    if (value !== "off" && value !== "on" && value !== "starttls") {
      throw new TypeError(
        "Unsupported value in secureTransport socket option: " + value);
    }
    return value;
  };

  // The socket ops: the node's own, or a container's brokered through the
  // host (an application guest has no network, and no address of its own).
  const hostOps = {
    read: (id) => __tcp_read(id),
    write: (id, bytes) => __tcp_write(id, bytes),
    shutdown: (id) => __tcp_shutdown(id),
    close: (id) => __tcp_close(id),
    starttls: (request) => __tcp_starttls(request),
  };
  const containerOps = {
    read: (id) => __container_tcp_read(id),
    write: (id, bytes) => __container_tcp_write(id, bytes),
    shutdown: (id) => __container_tcp_shutdown(id),
    close: (id) => __container_tcp_close(id),
    starttls: (request) => __container_tcp_starttls(request),
  };

  class Socket {
    constructor(opening, secureTransport, allowHalfOpen, startTlsInfo, ops = hostOps) {
      this._ops = ops;
      this._secureTransport = secureTransport;
      this._allowHalfOpen = allowHalfOpen;
      this._startTlsInfo = startTlsInfo; // null once used or unusable
      this._id = null;
      this._closing = false;
      this._detached = false;
      let resolveClosed, rejectClosed;
      this._closed = new Promise((resolve, reject) => {
        resolveClosed = resolve;
        rejectClosed = reject;
      });
      this._resolveClosed = resolveClosed;
      this._rejectClosed = rejectClosed;
      this._closed.catch(() => {});

      this._opened = opening.then((info) => {
        this._id = info.id;
        return {
          remoteAddress: info.remoteAddress ?? undefined,
          localAddress: info.localAddress ?? undefined,
        };
      });
      this._opened.catch(() => {});

      const socket = this;
      // highWaterMark 0: the stream must never read ahead of the
      // consumer. A speculative host read parked on the connection
      // would deadlock startTls(), whose handshake cannot begin while
      // a plaintext read holds the socket -- the server speaks second.
      this._readable = new ReadableStream({
        async pull(controller) {
          await socket._opened;
          if (socket._closing || socket._detached) {
            controller.close();
            return;
          }
          const bytes = await socket._ops.read(socket._id);
          if (socket._closing || socket._detached) {
            controller.close();
            return;
          }
          if (bytes.byteLength === 0) {
            controller.close();
            // EOF closes the write side too unless the application
            // asked for a half-open socket.
            if (!socket._allowHalfOpen) await socket._eofClose();
            return;
          }
          controller.enqueue(bytes);
        },
        cancel() {},
      }, { highWaterMark: 0 });
      this._writable = new WritableStream({
        async write(chunk) {
          await socket._opened;
          if (socket._detached) throw new TypeError("This socket was upgraded.");
          const bytes = chunk instanceof Uint8Array
            ? chunk
            : ArrayBuffer.isView(chunk)
              ? new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength)
              : new Uint8Array(chunk);
          await socket._ops.write(socket._id, bytes);
        },
        async close() {
          await socket._opened;
          if (!socket._detached) await socket._ops.shutdown(socket._id);
        },
        abort() {},
      });

      // A failed connection fails everything the socket exposes.
      opening.catch((error) => {
        this._closing = true;
        this._rejectClosed(error);
      });
    }

    get readable() { return this._readable; }
    get writable() { return this._writable; }
    get opened() { return this._opened; }
    get closed() { return this._closed; }
    get secureTransport() { return this._secureTransport; }

    async _eofClose() {
      try {
        const writer = this._writable.getWriter();
        await writer.close();
        writer.releaseLock();
      } catch { /* already closed or locked; closing wins either way */ }
      this._finishClose();
    }

    _finishClose() {
      if (this._closing) return;
      this._closing = true;
      if (this._id !== null) this._ops.close(this._id);
      this._resolveClosed(undefined);
    }

    async close() {
      if (this._closing) return this._closed;
      this._closing = true;
      try {
        await this._opened;
      } catch {
        return this._closed;
      }
      try {
        const writer = this._writable.getWriter();
        await writer.close();
      } catch { /* stream already closed, errored, or locked */ }
      try {
        await this._readable.cancel();
      } catch { /* reader may hold the lock; the close below still wins */ }
      this._ops.close(this._id);
      this._resolveClosed(undefined);
      return this._closed;
    }

    startTls(options = undefined) {
      if (this._secureTransport === "on") {
        throw new TypeError("Cannot startTls on a TLS socket.");
      }
      if (this._secureTransport !== "starttls") {
        throw new TypeError(
          "The `secureTransport` socket option must be set to 'starttls' " +
            "for startTls to be used.");
      }
      if (this._closing) {
        throw new TypeError(
          "The connection was closed before startTls could be started.");
      }
      if (this._startTlsInfo === null) {
        throw new TypeError("startTls can only be called once.");
      }
      const info = this._startTlsInfo;
      this._startTlsInfo = null;
      this._detached = true;
      const upgrading = this._opened.then(async () => {
        const raw = await this._ops.starttls(JSON.stringify({
          id: this._id,
          host: String(options?.expectedServerHostname ?? info.hostname),
        }));
        // The plaintext socket is consumed; its id now fails every op.
        this._resolveClosed(undefined);
        const parsed = JSON.parse(raw);
        return { id: parsed.id };
      });
      return new Socket(upgrading, "on", this._allowHalfOpen, null, this._ops);
    }
  }

  // `container` is `{scope, port}` for a guest's container port: the host dials.
  const connect = (address, options = undefined, container = undefined) => {
    const { hostname, port } = parseAddress(address);
    const secureTransport = parseSecureTransport(options);
    const allowHalfOpen = !!(options && options.allowHalfOpen);
    const opening = (container
      ? __container_connect(container.scope, container.port)
      : __tcp_connect(JSON.stringify({
        hostname,
        port,
        secure: secureTransport === "on",
      }))).then((raw) => JSON.parse(raw));
    return new Socket(
      opening,
      secureTransport,
      allowHalfOpen,
      secureTransport === "starttls" ? { hostname } : null,
      container ? containerOps : hostOps,
    );
  };

  __celld.__cfSockets = { connect };
})();
