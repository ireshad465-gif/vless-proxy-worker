// =========================================================================
// Cloudflare Worker VLESS Proxy with 0-RTT EarlyData & DNS-over-HTTPS
// =========================================================================
import { connect } from 'cloudflare:sockets';

// Default User UUID (can also be overridden by environment variable UUID)
let userID = 'bdeb28a4-ca3f-4665-9da2-6d92b718e4eb';

// Default ProxyIP fallbacks for sites behind Cloudflare CDN
let proxyIPs = [
  'cdn-all.xn--b6gac.eu.org',
  'edgetunnel.anycast.eu.org',
  'cdn.anycast.eu.org',
  'proxyip.fxxk.dedyn.io',
  'workers.cloudflare.cyou'
];
let proxyIP = proxyIPs[0];

// DoH (DNS-over-HTTPS) providers for UDP DNS resolution (port 53)
const dohURLs = [
  'https://1.1.1.1/dns-query',
  'https://cloudflare-dns.com/dns-query',
  'https://dns.google/dns-query'
];

const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

export default {
  /**
   * @param {Request} request
   * @param {{UUID?: string, PROXYIP?: string}} env
   * @param {ExecutionContext} ctx
   * @returns {Promise<Response>}
   */
  async fetch(request, env, ctx) {
    try {
      if (env.UUID) userID = env.UUID;
      if (env.PROXYIP) proxyIP = env.PROXYIP;

      const upgradeHeader = request.headers.get('Upgrade');
      const url = new URL(request.url);

      // 1. WebSocket VLESS Connection
      if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
        return await vlessOverWSHandler(request);
      }

      // 2. HTTP Requests (Web Dashboard & Config Generator)
      switch (url.pathname) {
        case '/': {
          return new Response(generateHomePage(request, userID), {
            status: 200,
            headers: { 'Content-Type': 'text/html; charset=utf-8' }
          });
        }
        case `/${userID}`:
        case `/sub`: {
          const host = request.headers.get('Host') || url.hostname;
          const config = getVLESSConfig(userID, host);
          return new Response(config, {
            status: 200,
            headers: { 'Content-Type': 'text/plain; charset=utf-8' }
          });
        }
        case '/raw': {
          const host = request.headers.get('Host') || url.hostname;
          const vlessLink = `vless://${userID}@${host}:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fed%3D2048#${host}`;
          return new Response(vlessLink, {
            status: 200,
            headers: { 'Content-Type': 'text/plain; charset=utf-8' }
          });
        }
        default:
          return new Response('404 Not Found', { status: 404 });
      }
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  }
};

/**
 * Handles incoming WebSocket VLESS connection
 * @param {Request} request
 */
async function vlessOverWSHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);
  webSocket.accept();

  let address = '';
  let portWithRandomLog = '';
  const log = (info, event) => {
    // console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
  };

  // Support 0-RTT early data from Sec-WebSocket-Protocol header
  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);

  let remoteSocketWrapper = { value: null };
  let udpStreamWrite = null;
  let isDns = false;

  readableWebSocketStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (isDns && udpStreamWrite) {
        return udpStreamWrite(chunk);
      }
      if (remoteSocketWrapper.value) {
        const writer = remoteSocketWrapper.value.writable.getWriter();
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }

      const {
        hasError,
        message,
        portRemote = 443,
        addressRemote = '',
        rawDataIndex,
        vlessVersion = new Uint8Array([0, 0]),
        isUDP,
      } = processVlessHeader(chunk, userID);

      address = addressRemote;
      portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp' : 'tcp'}`;

      if (hasError) {
        throw new Error(message);
      }

      // VLESS Response Header: [version, addonsLength=0]
      const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawClientData = chunk.slice(rawDataIndex);

      if (isUDP) {
        if (portRemote === 53) {
          isDns = true;
          const { write } = await handleUDPOutBound(webSocket, vlessResponseHeader, log);
          udpStreamWrite = write;
          udpStreamWrite(rawClientData);
          return;
        } else {
          throw new Error('UDP proxy only enabled for DNS (port 53)');
        }
      }

      // Handle TCP outbound
      handleTCPOutBound(remoteSocketWrapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log);
    },
    close() {
      log('readableWebSocketStream is closed');
    },
    abort(reason) {
      log('readableWebSocketStream aborted', JSON.stringify(reason));
    }
  })).catch((err) => {
    log('readableWebSocketStream pipeTo error', err);
  });

  return new Response(null, {
    status: 101,
    webSocket: client
  });
}

/**
 * Outbound TCP Connection Manager with ProxyIP retry
 */
async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, log) {
  async function connectAndWrite(address, port) {
    const tcpSocket = connect({
      hostname: address,
      port: port
    });
    remoteSocket.value = tcpSocket;
    log(`Connected to ${address}:${port}`);
    const writer = tcpSocket.writable.getWriter();
    await writer.write(rawClientData);
    writer.releaseLock();
    return tcpSocket;
  }

  async function retry() {
    try {
      const selectedProxy = proxyIP || proxyIPs[Math.floor(Math.random() * proxyIPs.length)];
      log(`Retrying with ProxyIP: ${selectedProxy}`);
      const tcpSocket = await connectAndWrite(selectedProxy, portRemote);
      tcpSocket.closed.catch((err) => {
        log('retry tcpSocket closed error', err);
      }).finally(() => {
        safeCloseWebSocket(webSocket);
      });
      remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null, log);
    } catch (e) {
      safeCloseWebSocket(webSocket);
    }
  }

  try {
    const tcpSocket = await connectAndWrite(addressRemote, portRemote);
    remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, retry, log);
  } catch (err) {
    log(`Direct connection to ${addressRemote} failed, falling back to ProxyIP`, err);
    await retry();
  }
}

/**
 * Pipe TCP Remote Socket to WebSocket
 */
async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry, log) {
  let vlessHeader = vlessResponseHeader;
  let hasIncomingData = false;

  await remoteSocket.readable.pipeTo(new WritableStream({
    async write(chunk, controller) {
      hasIncomingData = true;
      if (webSocket.readyState !== WS_READY_STATE_OPEN) {
        controller.error('webSocket is not open');
      }
      if (vlessHeader) {
        webSocket.send(await new Blob([vlessHeader, chunk]).arrayBuffer());
        vlessHeader = null;
      } else {
        webSocket.send(chunk);
      }
    },
    close() {
      log(`remoteSocket.readable closed, hasIncomingData: ${hasIncomingData}`);
    },
    abort(reason) {
      log('remoteSocket.readable abort', reason);
    }
  })).catch((error) => {
    safeCloseWebSocket(webSocket);
  });

  if (!hasIncomingData && retry) {
    retry();
  }
}

/**
 * WebSocket to ReadableStream with 0-RTT support
 */
function makeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
  let readableStreamCancel = false;
  return new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => {
        if (readableStreamCancel) return;
        controller.enqueue(event.data);
      });

      webSocketServer.addEventListener('close', () => {
        safeCloseWebSocket(webSocketServer);
        if (readableStreamCancel) return;
        controller.close();
      });

      webSocketServer.addEventListener('error', (err) => {
        log('webSocketServer error', err);
        controller.error(err);
      });

      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) {
        controller.error(error);
      } else if (earlyData) {
        controller.enqueue(earlyData);
      }
    },
    cancel(reason) {
      if (readableStreamCancel) return;
      readableStreamCancel = true;
      safeCloseWebSocket(webSocketServer);
    }
  });
}

/**
 * Handles DNS UDP packets using DNS-over-HTTPS (DoH)
 */
async function handleUDPOutBound(webSocket, vlessResponseHeader, log) {
  let isVlessHeaderSent = false;
  const transformStream = new TransformStream({
    transform(chunk, controller) {
      for (let index = 0; index < chunk.byteLength;) {
        const lengthBuffer = chunk.slice(index, index + 2);
        const udpPacketLength = new DataView(lengthBuffer).getUint16(0);
        const udpData = new Uint8Array(chunk.slice(index + 2, index + 2 + udpPacketLength));
        index = index + 2 + udpPacketLength;
        controller.enqueue(udpData);
      }
    }
  });

  transformStream.readable.pipeTo(new WritableStream({
    async write(chunk) {
      let resp = null;
      for (const doh of dohURLs) {
        try {
          resp = await fetch(doh, {
            method: 'POST',
            headers: { 'content-type': 'application/dns-message' },
            body: chunk
          });
          if (resp && resp.ok) break;
        } catch (e) {}
      }

      if (!resp || !resp.ok) return;

      const dnsQueryResult = await resp.arrayBuffer();
      const udpSize = dnsQueryResult.byteLength;
      const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);

      if (webSocket.readyState === WS_READY_STATE_OPEN) {
        if (isVlessHeaderSent) {
          webSocket.send(await new Blob([udpSizeBuffer, dnsQueryResult]).arrayBuffer());
        } else {
          webSocket.send(await new Blob([vlessResponseHeader, udpSizeBuffer, dnsQueryResult]).arrayBuffer());
          isVlessHeaderSent = true;
        }
      }
    }
  })).catch((error) => {
    log('DNS DoH error', error);
  });

  const writer = transformStream.writable.getWriter();
  return {
    write(chunk) {
      writer.write(chunk);
    }
  };
}

/**
 * Parse and validate VLESS header
 */
function processVlessHeader(vlessBuffer, expectedUserID) {
  if (vlessBuffer.byteLength < 24) {
    return { hasError: true, message: 'invalid data length' };
  }

  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  const userUUID = stringifyUUID(new Uint8Array(vlessBuffer.slice(1, 17)));

  // If strict UUID matching is needed:
  if (expectedUserID && userUUID.toLowerCase() !== expectedUserID.toLowerCase()) {
    return { hasError: true, message: 'invalid user UUID' };
  }

  const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
  const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];

  let isUDP = false;
  if (command === 1) {
    // TCP
  } else if (command === 2) {
    isUDP = true;
  } else {
    return { hasError: true, message: `command ${command} not supported (01-tcp, 02-udp)` };
  }

  const portIndex = 18 + optLength + 1;
  const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
  const portRemote = new DataView(portBuffer).getUint16(0);

  const addressIndex = portIndex + 2;
  const addressBuffer = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1));
  const addressType = addressBuffer[0];

  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressValue = '';

  switch (addressType) {
    case 1: // IPv4
      addressLength = 4;
      addressValue = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
      break;
    case 2: // Domain
      addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
      addressValueIndex += 1;
      addressValue = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      break;
    case 3: // IPv6
      addressLength = 16;
      const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      const ipv6 = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(dataView.getUint16(i * 2).toString(16));
      }
      addressValue = ipv6.join(':');
      break;
    default:
      return { hasError: true, message: `invalid addressType: ${addressType}` };
  }

  return {
    hasError: false,
    addressRemote: addressValue,
    addressType,
    portRemote,
    rawDataIndex: addressValueIndex + addressLength,
    vlessVersion: version,
    isUDP
  };
}

/**
 * Base64 helper for 0-RTT
 */
function base64ToArrayBuffer(base64Str) {
  if (!base64Str) return { error: null };
  try {
    base64Str = base64Str.replace(/-/g, '+').replace(/_/g, '/');
    const decode = atob(base64Str);
    const arrayBuffer = Uint8Array.from(decode, (c) => c.charCodeAt(0));
    return { earlyData: arrayBuffer.buffer, error: null };
  } catch (error) {
    return { error };
  }
}

/**
 * Convert byte array to UUID string
 */
function stringifyUUID(arr, offset = 0) {
  const byteToHex = [];
  for (let i = 0; i < 256; ++i) {
    byteToHex.push((i + 256).toString(16).slice(1));
  }
  return (
    byteToHex[arr[offset + 0]] + byteToHex[arr[offset + 1]] +
    byteToHex[arr[offset + 2]] + byteToHex[arr[offset + 3]] + '-' +
    byteToHex[arr[offset + 4]] + byteToHex[arr[offset + 5]] + '-' +
    byteToHex[arr[offset + 6]] + byteToHex[arr[offset + 7]] + '-' +
    byteToHex[arr[offset + 8]] + byteToHex[arr[offset + 9]] + '-' +
    byteToHex[arr[offset + 10]] + byteToHex[arr[offset + 11]] +
    byteToHex[arr[offset + 12]] + byteToHex[arr[offset + 13]] +
    byteToHex[arr[offset + 14]] + byteToHex[arr[offset + 15]]
  ).toLowerCase();
}

function safeCloseWebSocket(socket) {
  try {
    if (socket.readyState === WS_READY_STATE_OPEN || socket.readyState === WS_READY_STATE_CLOSING) {
      socket.close();
    }
  } catch (e) {}
}

/**
 * Returns plain text VLESS configurations
 */
function getVLESSConfig(userID, hostName) {
  const vlessMain = `vless://${userID}@${hostName}:443?encryption=none&security=tls&sni=${hostName}&fp=chrome&type=ws&host=${hostName}&path=%2F%3Fed%3D2048#${hostName}`;
  const cleanIPLink = `vless://${userID}@104.16.1.1:443?encryption=none&security=tls&sni=${hostName}&fp=chrome&type=ws&host=${hostName}&path=%2F%3Fed%3D2048#CF-CleanIP-${hostName}`;

  return `================================================================
⚡ VLESS Links (Copy and import into v2rayNG / Shadowrocket / NekoBox)
================================================================

1. Standard Domain Node:
${vlessMain}

2. Cloudflare Clean IP Node (Recommended for Speed & Stability):
${cleanIPLink}

================================================================
📱 Clash Meta Configuration:
================================================================
- name: ${hostName}
  type: vless
  server: ${hostName}
  port: 443
  uuid: ${userID}
  network: ws
  tls: true
  udp: true
  sni: ${hostName}
  client-fingerprint: chrome
  ws-opts:
    path: "/?ed=2048"
    headers:
      Host: ${hostName}
`;
}

/**
 * HTML Web Page with clean UI, nodes, QR code & setup guide
 */
function generateHomePage(request, userID) {
  const host = request.headers.get('Host') || 'vless-proxy';
  const vlessLink = `vless://${userID}@${host}:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fed%3D2048#VLESS-${host}`;
  const cleanLink = `vless://${userID}@104.16.1.1:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fed%3D2048#CF-CleanIP-${host}`;

  return `<!DOCTYPE html>
<html lang="si">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>VLESS VPN Node - ${host}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #f8fafc; padding: 20px; display: flex; justify-content: center; }
    .container { max-width: 800px; width: 100%; background: #1e293b; border-radius: 16px; padding: 30px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); border: 1px solid #334155; }
    .badge { display: inline-block; padding: 6px 14px; background: #10b981; color: #fff; border-radius: 20px; font-weight: bold; font-size: 14px; margin-bottom: 15px; }
    h1 { font-size: 26px; margin-bottom: 10px; color: #38bdf8; }
    p { color: #94a3b8; font-size: 15px; line-height: 1.6; margin-bottom: 20px; }
    .card { background: #0f172a; border-radius: 12px; padding: 18px; margin-bottom: 20px; border: 1px solid #334155; }
    .card h3 { font-size: 17px; margin-bottom: 10px; color: #f1f5f9; display: flex; align-items: center; justify-content: space-between; }
    .code-box { background: #020617; padding: 12px; border-radius: 8px; font-family: monospace; font-size: 13px; color: #38bdf8; word-break: break-all; user-select: all; }
    .btn { background: #3b82f6; color: #fff; border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-weight: 600; font-size: 13px; transition: 0.2s; }
    .btn:hover { background: #2563eb; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-top: 15px; }
    @media(max-width: 600px) { .grid { grid-template-columns: 1fr; } }
    .stat { background: #1e293b; padding: 12px; border-radius: 8px; border: 1px solid #334155; }
    .stat-label { font-size: 12px; color: #64748b; text-transform: uppercase; }
    .stat-val { font-size: 15px; font-weight: 600; color: #e2e8f0; margin-top: 4px; word-break: break-all; }
  </style>
</head>
<body>
  <div class="container">
    <span class="badge">● Server Active & Running</span>
    <h1>🌐 Cloudflare VLESS Edge Proxy</h1>
    <p>Zero-configuration high-speed serverless proxy node with 0-RTT EarlyData and DoH DNS support.</p>

    <div class="card">
      <h3>🚀 Node 1: Standard Domain Link <button class="btn" onclick="navigator.clipboard.writeText('${vlessLink}');alert('Copied!')">Copy Link</button></h3>
      <div class="code-box">${vlessLink}</div>
    </div>

    <div class="card">
      <h3>⚡ Node 2: Cloudflare Clean IP Link (Super Fast) <button class="btn" onclick="navigator.clipboard.writeText('${cleanLink}');alert('Copied!')">Copy Link</button></h3>
      <div class="code-box">${cleanLink}</div>
    </div>

    <div class="grid">
      <div class="stat"><div class="stat-label">Host / SNI</div><div class="stat-val">${host}</div></div>
      <div class="stat"><div class="stat-label">Port</div><div class="stat-val">443 (TLS)</div></div>
      <div class="stat"><div class="stat-label">Transport / Path</div><div class="stat-val">ws / ?ed=2048</div></div>
      <div class="stat"><div class="stat-label">UUID</div><div class="stat-val">${userID}</div></div>
    </div>
  </div>
</body>
</html>`;
}
