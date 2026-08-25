import { connect } from 'cloudflare:sockets';

const userID = 'bdeb28a4-ca3f-4665-9da2-6d92b718e4eb';
const proxyIP = '';

function uuidToBytes(uuid) {
  const hex = uuid.replace(/-/g, '');
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) bytes[i] = parseInt(hex.substr(i * 2, 2), 16);
  return bytes;
}

const userIDBytes = uuidToBytes(userID);

function bytesEqual(a, b) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) { if (a[i] !== b[i]) return false; }
  return true;
}

function readU16BE(view, offset) {
  return (view.getUint8(offset) << 8) | view.getUint8(offset + 1);
}

function ipv4String(bytes) {
  return bytes[0] + '.' + bytes[1] + '.' + bytes[2] + '.' + bytes[3];
}

function ipv6String(bytes) {
  const parts = [];
  for (let i = 0; i < 16; i += 2) parts.push(((bytes[i] << 8) | bytes[i + 1]).toString(16));
  return parts.join(':');
}

async function toUint8Array(data) {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (data instanceof Uint8Array) return data;
  if (typeof data === 'string') return new TextEncoder().encode(data);
  return new Uint8Array(await data.arrayBuffer());
}

function parseVlessHeader(chunk) {
  const view = new DataView(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  if (chunk.byteLength < 24) throw new Error('Header too short');
  const version = view.getUint8(0);
  const clientUUID = chunk.slice(1, 17);
  if (!bytesEqual(clientUUID, userIDBytes)) throw new Error('UUID mismatch');
  const addonsLen = view.getUint8(17);
  const addonsEnd = 18 + addonsLen;
  if (chunk.byteLength < addonsEnd + 4) throw new Error('Header truncated after addons');
  const command = view.getUint8(addonsEnd);
  if (command !== 0x01) throw new Error('Unsupported command: ' + command);
  const port = readU16BE(view, addonsEnd + 1);
  const addrType = view.getUint8(addonsEnd + 3);
  let address = '';
  let addrStart = addonsEnd + 4;
  if (addrType === 0x01) {
    if (chunk.byteLength < addrStart + 4) throw new Error('IPv4 truncated');
    address = ipv4String(chunk.slice(addrStart, addrStart + 4));
    addrStart += 4;
  } else if (addrType === 0x02) {
    const domainLen = view.getUint8(addrStart);
    addrStart += 1;
    if (chunk.byteLength < addrStart + domainLen) throw new Error('Domain truncated');
    address = new TextDecoder().decode(chunk.slice(addrStart, addrStart + domainLen));
    addrStart += domainLen;
  } else if (addrType === 0x03) {
    if (chunk.byteLength < addrStart + 16) throw new Error('IPv6 truncated');
    address = ipv6String(chunk.slice(addrStart, addrStart + 16));
    addrStart += 16;
  } else {
    throw new Error('Unknown address type: ' + addrType);
  }
  const rawData = chunk.slice(addrStart);
  return { address, port, rawData, version };
}

function buildVlessResponse(version) {
  return new Uint8Array([version, 0x00]);
}

async function handleVlessSession(webSocket, headerChunk) {
  const { address, port, rawData, version } = parseVlessHeader(headerChunk);
  const targetHost = proxyIP || address;

  let tcpSocket;
  try {
    tcpSocket = connect({ hostname: targetHost, port: port });
    await tcpSocket.opened;
  } catch (err) {
    try { webSocket.close(1011, 'TCP connect failed: ' + err.message); } catch (_) {}
    return;
  }

  const tcpWriter = tcpSocket.writable.getWriter();

  const respHeader = buildVlessResponse(version);
  const respBuf = respHeader.buffer.slice(respHeader.byteOffset, respHeader.byteOffset + respHeader.byteLength);
  webSocket.send(respBuf);

  if (rawData && rawData.byteLength > 0) {
    try {
      await tcpWriter.write(rawData);
    } catch (err) {
      try { webSocket.close(1011, 'Initial write failed'); } catch (_) {}
      return;
    }
  }

  let wsClosed = false;
  let tcpClosed = false;

  const cleanup = () => {
    if (wsClosed && tcpClosed) return;
    wsClosed = true;
    tcpClosed = true;
    try { tcpWriter.close(); } catch (_) {}
    try { tcpSocket.close(); } catch (_) {}
    try { webSocket.close(1000, 'cleanup'); } catch (_) {}
  };

  (async () => {
    const reader = tcpSocket.readable.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (wsClosed) break;
        const buf = value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength);
        webSocket.send(buf);
      }
    } catch (err) {
    } finally {
      tcpClosed = true;
      cleanup();
    }
  })();

  webSocket.addEventListener('message', async (event) => {
    if (tcpClosed) return;
    try {
      const data = await toUint8Array(event.data);
      await tcpWriter.write(data);
    } catch (err) {
      cleanup();
    }
  });

  webSocket.addEventListener('close', () => {
    wsClosed = true;
    cleanup();
  });

  webSocket.addEventListener('error', () => {
    wsClosed = true;
    cleanup();
  });
}

export default {
  async fetch(request) {
    const upgradeHeader = request.headers.get('Upgrade');
    if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      const firstMessageListener = async (event) => {
        server.removeEventListener('message', firstMessageListener);
        const chunkBytes = await toUint8Array(event.data);
        try {
          await handleVlessSession(server, chunkBytes);
        } catch (err) {
          try { server.close(1011, 'vless error: ' + err.message); } catch (_) {}
        }
      };
      server.addEventListener('message', firstMessageListener);
      return new Response(null, { status: 101, webSocket: server });
    }
    return new Response('VLESS-WS Worker is running', {
      status: 200,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    });
  },
};
