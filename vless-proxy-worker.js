// =========================================================================
// Cloudflare Worker VLESS Edge Proxy (v3.0 - Multi-Country & Social Media SNI)
// =========================================================================
import { connect } from 'cloudflare:sockets';

let userID = 'bdeb28a4-ca3f-4665-9da2-6d92b718e4eb';

// Country-Specific Verified ProxyIPs
const countryProxyMap = {
  'us': 'proxyip.us.fxxk.dedyn.io',       // 🇺🇸 United States (Arena AI, ChatGPT)
  'sg': 'proxyip.aliyun.fxxk.dedyn.io',   // 🇸🇬 Singapore (Ultra Low Latency)
  'jp': 'proxyip.jp.fxxk.dedyn.io',       // 🇯🇵 Japan
  'hk': 'proxyip.hk.fxxk.dedyn.io',       // 🇭🇰 Hong Kong
  'de': 'proxyip.oracle.fxxk.dedyn.io',   // 🇩🇪 Germany / Europe
  'uk': 'proxyip.vultr.fxxk.dedyn.io',    // 🇬🇧 United Kingdom
  'global': 'proxyip.cmliussss.net',      // 🌐 Global Anycast
  'cf': 'cdn-all.xn--b6gac.eu.org'        // ⚡ Cloudflare Clean
};

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

      const upgradeHeader = request.headers.get('Upgrade');
      const url = new URL(request.url);

      // Extract custom proxyIP or country code
      let customProxyIP = url.searchParams.get('proxyip') || '';
      const countryCode = url.searchParams.get('cc') || '';
      
      if (!customProxyIP && countryCode && countryProxyMap[countryCode.toLowerCase()]) {
        customProxyIP = countryProxyMap[countryCode.toLowerCase()];
      }

      if (!customProxyIP) {
        const match = url.pathname.match(/\/proxyip=([^/&]+)/);
        if (match) customProxyIP = match[1];
      }
      if (!customProxyIP && env.PROXYIP) {
        customProxyIP = env.PROXYIP;
      }

      // 1. WebSocket VLESS Proxy
      if (upgradeHeader && upgradeHeader.toLowerCase() === 'websocket') {
        return await vlessOverWSHandler(request, customProxyIP);
      }

      // 2. HTTP Web Interface and Configuration Delivery
      const host = request.headers.get('Host') || url.hostname;

      switch (url.pathname) {
        case '/': {
          return new Response(generateHomePage(host, userID), {
            status: 200,
            headers: { 'Content-Type': 'text/html; charset=utf-8' }
          });
        }
        case `/${userID}`:
        case `/sub`: {
          return new Response(generateAllConfigs(host, userID), {
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
 */
async function vlessOverWSHandler(request, customProxyIP) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);
  webSocket.accept();

  const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';
  const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader);

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

      if (hasError) {
        throw new Error(message);
      }

      const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawClientData = chunk.slice(rawDataIndex);

      if (isUDP) {
        if (portRemote === 53) {
          isDns = true;
          const { write } = await handleUDPOutBound(webSocket, vlessResponseHeader);
          udpStreamWrite = write;
          udpStreamWrite(rawClientData);
          return;
        } else {
          throw new Error('UDP proxy only enabled for DNS (port 53)');
        }
      }

      handleTCPOutBound(remoteSocketWrapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, customProxyIP);
    },
    close() {},
    abort() {}
  })).catch(() => {});

  return new Response(null, {
    status: 101,
    webSocket: client
  });
}

/**
 * Outbound TCP Connection Manager with Smart Country ProxyIP Routing
 */
async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader, customProxyIP) {
  async function connectAndWrite(targetHost, targetPort) {
    const tcpSocket = connect({
      hostname: targetHost,
      port: targetPort
    });
    remoteSocket.value = tcpSocket;
    const writer = tcpSocket.writable.getWriter();
    await writer.write(rawClientData);
    writer.releaseLock();
    return tcpSocket;
  }

  const isDirect = customProxyIP === 'direct';
  const targetProxy = (customProxyIP && !isDirect) 
    ? customProxyIP 
    : (isDirect ? '' : countryProxyMap['us']);

  async function retry(fallbackProxy) {
    try {
      const tcpSocket = await connectAndWrite(fallbackProxy, portRemote);
      tcpSocket.closed.catch(() => {}).finally(() => {
        safeCloseWebSocket(webSocket);
      });
      remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, null);
    } catch (e) {
      safeCloseWebSocket(webSocket);
    }
  }

  if (targetProxy) {
    try {
      const tcpSocket = await connectAndWrite(targetProxy, portRemote);
      remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, async () => {
        await retry(countryProxyMap['sg']);
      });
    } catch (err) {
      await retry(countryProxyMap['sg']);
    }
  } else {
    try {
      const tcpSocket = await connectAndWrite(addressRemote, portRemote);
      remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, async () => {
        await retry(countryProxyMap['us']);
      });
    } catch (err) {
      await retry(countryProxyMap['us']);
    }
  }
}

/**
 * Pipe TCP Remote Socket to WebSocket
 */
async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry) {
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
    close() {},
    abort() {}
  })).catch(() => {
    safeCloseWebSocket(webSocket);
  });

  if (!hasIncomingData && retry) {
    retry();
  }
}

function makeReadableWebSocketStream(webSocketServer, earlyDataHeader) {
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
        controller.error(err);
      });

      const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
      if (error) {
        controller.error(error);
      } else if (earlyData) {
        controller.enqueue(earlyData);
      }
    },
    cancel() {
      if (readableStreamCancel) return;
      readableStreamCancel = true;
      safeCloseWebSocket(webSocketServer);
    }
  });
}

async function handleUDPOutBound(webSocket, vlessResponseHeader) {
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
  })).catch(() => {});

  const writer = transformStream.writable.getWriter();
  return {
    write(chunk) {
      writer.write(chunk);
    }
  };
}

function processVlessHeader(vlessBuffer, expectedUserID) {
  if (vlessBuffer.byteLength < 24) {
    return { hasError: true, message: 'invalid data length' };
  }

  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  const userUUID = stringifyUUID(new Uint8Array(vlessBuffer.slice(1, 17)));

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
    return { hasError: true, message: `command ${command} not supported` };
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
    case 1:
      addressLength = 4;
      addressValue = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
      break;
    case 2:
      addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
      addressValueIndex += 1;
      addressValue = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
      break;
    case 3:
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
 * Generates all 10+ Multi-Country & Social Media Configs (Raw Text)
 */
function generateAllConfigs(host, userID) {
  const c = [];
  
  // YouTube Package Configs
  c.push(`vless://${userID}@www.youtube.com:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.us.fxxk.dedyn.io%26ed%3D2048#🔴 YouTube Pack 🇺🇸 US - Arena AI & All Sites`);
  c.push(`vless://${userID}@www.youtube.com:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.aliyun.fxxk.dedyn.io%26ed%3D2048#🔴 YouTube Pack 🇸🇬 SG - Ultra Fast`);
  c.push(`vless://${userID}@www.youtube.com:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.jp.fxxk.dedyn.io%26ed%3D2048#🔴 YouTube Pack 🇯🇵 JP - Japan High Speed`);
  c.push(`vless://${userID}@www.youtube.com:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.oracle.fxxk.dedyn.io%26ed%3D2048#🔴 YouTube Pack 🇩🇪 DE - Germany Oracle`);
  
  // WhatsApp Package Configs
  c.push(`vless://${userID}@web.whatsapp.com:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.us.fxxk.dedyn.io%26ed%3D2048#🟢 WhatsApp Pack 🇺🇸 US - Arena AI & All Sites`);
  c.push(`vless://${userID}@web.whatsapp.com:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.aliyun.fxxk.dedyn.io%26ed%3D2048#🟢 WhatsApp Pack 🇸🇬 SG - Ultra Fast`);

  // Facebook & Instagram Package Configs
  c.push(`vless://${userID}@m.facebook.com:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.us.fxxk.dedyn.io%26ed%3D2048#🔵 Facebook/Insta 🇺🇸 US - Arena AI & All Sites`);
  c.push(`vless://${userID}@m.facebook.com:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.aliyun.fxxk.dedyn.io%26ed%3D2048#🔵 Facebook/Insta 🇸🇬 SG - Ultra Fast`);

  // Super Clean IP Multi-Country Configs
  c.push(`vless://${userID}@104.16.1.1:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.us.fxxk.dedyn.io%26ed%3D2048#⚡ CF Clean IP 🇺🇸 US - High Speed`);
  c.push(`vless://${userID}@104.16.1.1:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.aliyun.fxxk.dedyn.io%26ed%3D2048#⚡ CF Clean IP 🇸🇬 SG - Low Latency`);
  c.push(`vless://${userID}@104.16.1.1:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.hk.fxxk.dedyn.io%26ed%3D2048#⚡ CF Clean IP 🇭🇰 HK - Hong Kong`);
  c.push(`vless://${userID}@104.16.1.1:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.vultr.fxxk.dedyn.io%26ed%3D2048#⚡ CF Clean IP 🇬🇧 UK - United Kingdom`);

  return c.join('\n');
}

/**
 * Web Dashboard Page with All 12 Configs & Copy Buttons
 */
function generateHomePage(host, userID) {
  const subLink = `https://${host}/sub`;
  const rawConfigs = generateAllConfigs(host, userID).split('\n');

  const cardsHtml = rawConfigs.map((cfg, idx) => {
    const name = decodeURIComponent(cfg.split('#')[1] || `Node ${idx + 1}`);
    return `
    <div class="card">
      <div class="card-title">
        <h3>${name}</h3>
        <button class="btn" onclick="navigator.clipboard.writeText('${cfg}');alert('Copied: ${name}')">Copy Link</button>
      </div>
      <div class="code-box">${cfg}</div>
    </div>`;
  }).join('');

  return `<!DOCTYPE html>
<html lang="si">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>VLESS Social Media & Multi-Country VPN Nodes - ${host}</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #080c14; color: #f1f5f9; padding: 20px; display: flex; justify-content: center; }
    .container { max-width: 880px; width: 100%; background: #0f172a; border-radius: 16px; padding: 25px; box-shadow: 0 10px 30px rgba(0,0,0,0.6); border: 1px solid #1e293b; }
    .badge { display: inline-block; padding: 6px 14px; background: #10b981; color: #fff; border-radius: 20px; font-weight: bold; font-size: 13px; margin-bottom: 12px; }
    h1 { font-size: 24px; color: #38bdf8; margin-bottom: 8px; }
    p { color: #94a3b8; font-size: 14px; line-height: 1.5; margin-bottom: 15px; }
    .sub-box { background: #1e293b; border-radius: 10px; padding: 15px; margin-bottom: 25px; border: 1px solid #3b82f6; display: flex; justify-content: space-between; align-items: center; }
    .sub-title { font-weight: 600; color: #38bdf8; font-size: 14px; }
    .card { background: #080c14; border-radius: 12px; padding: 14px; margin-bottom: 14px; border: 1px solid #1e293b; }
    .card-title { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
    .card-title h3 { font-size: 14px; color: #f8fafc; font-weight: 600; }
    .code-box { background: #020617; padding: 10px; border-radius: 6px; font-family: monospace; font-size: 12px; color: #38bdf8; word-break: break-all; margin-top: 6px; user-select: all; }
    .btn { background: #2563eb; color: #fff; border: none; padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600; transition: 0.2s; }
    .btn:hover { background: #1d4ed8; }
    .btn-sub { background: #10b981; }
    .btn-sub:hover { background: #059669; }
  </style>
</head>
<body>
  <div class="container">
    <span class="badge">● Server Active & Multi-Country Ready</span>
    <h1>🌐 Social Media Pack & Multi-Country VLESS Nodes</h1>
    <p>Use these nodes with Dialog, Mobitel, Hutch, and SLT Social Media packages (YouTube, WhatsApp, Facebook, Instagram) to unlock full internet access across multiple countries.</p>

    <div class="sub-box">
      <div>
        <div class="sub-title">📥 All-in-One Subscription Link (Import all 12 nodes at once)</div>
        <div style="font-family: monospace; font-size: 12px; color: #cbd5e1; margin-top: 4px;">${subLink}</div>
      </div>
      <button class="btn btn-sub" onclick="navigator.clipboard.writeText('${subLink}');alert('Subscription Link Copied! Paste into v2rayNG Subscription')">Copy Subscription Link</button>
    </div>

    ${cardsHtml}
  </div>
</body>
</html>`;
}
