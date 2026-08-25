// =========================================================================
// Cloudflare Worker VLESS Edge Proxy (v6.0 - Dialog Non-TLS 80 & TLS 443)
// =========================================================================
import { connect } from 'cloudflare:sockets';

let userID = 'bdeb28a4-ca3f-4665-9da2-6d92b718e4eb';

const countryProxyMap = {
  'us': 'proxyip.us.fxxk.dedyn.io',       // 🇺🇸 US (Arena AI / ChatGPT / Sites)
  'sg': 'proxyip.aliyun.fxxk.dedyn.io',   // 🇸🇬 Singapore (Ultra Low Latency)
  'jp': 'proxyip.jp.fxxk.dedyn.io',       // 🇯🇵 Japan
  'hk': 'proxyip.hk.fxxk.dedyn.io',       // 🇭🇰 Hong Kong
  'de': 'proxyip.oracle.fxxk.dedyn.io',   // 🇩🇪 Germany
  'uk': 'proxyip.vultr.fxxk.dedyn.io'     // 🇬🇧 UK
};

const dohURLs = [
  'https://1.1.1.1/dns-query',
  'https://cloudflare-dns.com/dns-query',
  'https://dns.google/dns-query'
];

const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

export default {
  async fetch(request, env, ctx) {
    try {
      if (env.UUID) userID = env.UUID;

      const upgradeHeader = request.headers.get('Upgrade');
      const url = new URL(request.url);

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

      // 1. WebSocket VLESS Proxy (Supports both TLS 443 & Non-TLS 80/8080/8880/2052/2082/2086/2095)
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
 * Generates all Dialog Special Configs (Both Port 80 Non-TLS & Port 443 TLS)
 */
function generateAllConfigs(host, userID) {
  const c = [];
  
  // 1. HTTP Non-TLS Port 80 (DPI Bypass - Works 100% when Normal Data is 0 MB)
  c.push(`vless://${userID}@104.16.1.1:80?encryption=none&security=none&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.us.fxxk.dedyn.io%26ed%3D2048#⚡ [Port 80] Dialog Zero-Data 🇺🇸 US`);
  c.push(`vless://${userID}@104.16.1.1:80?encryption=none&security=none&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.aliyun.fxxk.dedyn.io%26ed%3D2048#⚡ [Port 80] Dialog Zero-Data 🇸🇬 SG`);
  c.push(`vless://${userID}@104.17.1.1:80?encryption=none&security=none&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.jp.fxxk.dedyn.io%26ed%3D2048#⚡ [Port 80] Dialog Zero-Data 🇯🇵 JP`);
  c.push(`vless://${userID}@172.67.1.1:80?encryption=none&security=none&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.oracle.fxxk.dedyn.io%26ed%3D2048#⚡ [Port 80] Dialog Zero-Data 🇩🇪 DE`);

  // 2. HTTP Port 8080 Non-TLS
  c.push(`vless://${userID}@104.16.1.1:8080?encryption=none&security=none&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.us.fxxk.dedyn.io%26ed%3D2048#⚡ [Port 8080] Dialog Alt 🇺🇸 US`);
  c.push(`vless://${userID}@104.16.1.1:8080?encryption=none&security=none&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.aliyun.fxxk.dedyn.io%26ed%3D2048#⚡ [Port 8080] Dialog Alt 🇸🇬 SG`);

  // 3. TLS Port 443 Direct Clean IPs
  c.push(`vless://${userID}@104.16.1.1:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.us.fxxk.dedyn.io%26ed%3D2048#🔒 [Port 443] Dialog TLS 🇺🇸 US`);
  c.push(`vless://${userID}@104.16.1.1:443?encryption=none&security=tls&sni=${host}&fp=chrome&type=ws&host=${host}&path=%2F%3Fproxyip%3Dproxyip.aliyun.fxxk.dedyn.io%26ed%3D2048#🔒 [Port 443] Dialog TLS 🇸🇬 SG`);

  return c.join('\n');
}

function generateHomePage(host, userID) {
  const subLink = `https://${host}/sub`;
  const rawConfigs = generateAllConfigs(host, userID).split('\n');

  const cardsHtml = rawConfigs.map((cfg, idx) => {
    const name = decodeURIComponent(cfg.split('#')[1] || `Node ${idx + 1}`);
    const isPort80 = name.includes('Port 80');
    const badgeText = isPort80 ? 'DPI Zero-Data Bypass (Port 80 No-TLS)' : 'Standard TLS (Port 443)';
    const badgeClass = isPort80 ? 'tag-wa' : 'tag-cf';

    return `
    <div class="card">
      <div class="card-top">
        <div class="card-title-group">
          <span class="node-title">${name}</span>
          <div class="tags"><span class="tag ${badgeClass}">${badgeText}</span></div>
        </div>
      </div>
      <div class="code-wrapper" id="cfg-${idx}">${cfg}</div>
      <div class="card-footer">
        <span class="host-info">Server: 104.16.1.1 | ${isPort80 ? 'Port: 80 (No-TLS)' : 'Port: 443 (TLS)'}</span>
        <button class="btn" onclick="copyConfig('cfg-${idx}', this)">📋 Copy Link</button>
      </div>
    </div>`;
  }).join('');

  return `<!DOCTYPE html>
<html lang="si">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dialog Zero-Data VLESS Hub - ${host}</title>
  <style>
    :root {
      --bg: #0b0f19;
      --card-bg: #131b2e;
      --card-inner: #080c14;
      --border: #1e293b;
      --primary: #38bdf8;
      --text: #f8fafc;
      --text-muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background-color: var(--bg); color: var(--text); padding: 24px 16px; display: flex; justify-content: center; line-height: 1.5; }
    .container { max-width: 960px; width: 100%; }
    .header { background: linear-gradient(135deg, #1e293b, #0f172a); border: 1px solid var(--border); border-radius: 16px; padding: 24px; margin-bottom: 24px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
    .badge { display: inline-flex; align-items: center; gap: 6px; padding: 5px 12px; background: rgba(16, 185, 129, 0.15); border: 1px solid #10b981; color: #34d399; border-radius: 9999px; font-size: 13px; font-weight: 600; margin-bottom: 12px; }
    .badge-dot { width: 8px; height: 8px; background: #10b981; border-radius: 50%; box-shadow: 0 0 8px #10b981; }
    h1 { font-size: 24px; font-weight: 700; color: var(--primary); margin-bottom: 8px; }
    .subtitle { color: var(--text-muted); font-size: 14px; }
    .alert-box { background: rgba(245, 158, 11, 0.1); border: 1px solid #f59e0b; border-radius: 10px; padding: 14px; margin-top: 14px; font-size: 13px; color: #fcd34d; }
    .sub-banner { background: #1e293b; border: 1px solid #3b82f6; border-radius: 12px; padding: 16px; margin-top: 16px; display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; }
    .sub-info { flex: 1; min-width: 250px; }
    .sub-title { font-size: 14px; font-weight: 700; color: #60a5fa; }
    .sub-url { font-family: monospace; font-size: 12px; color: #cbd5e1; word-break: break-all; margin-top: 4px; background: #090d16; padding: 6px 10px; border-radius: 6px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(420px, 1fr)); gap: 16px; }
    @media (max-width: 640px) { .grid { grid-template-columns: 1fr; } }
    .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 16px; display: flex; flex-direction: column; justify-content: space-between; transition: transform 0.15s, border-color 0.15s; }
    .card:hover { border-color: #3b82f6; transform: translateY(-2px); }
    .card-top { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 10px; }
    .card-title-group { display: flex; flex-direction: column; gap: 4px; }
    .node-title { font-size: 15px; font-weight: 700; color: #f8fafc; }
    .tags { display: flex; gap: 6px; flex-wrap: wrap; }
    .tag { font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 4px; }
    .tag-wa { background: rgba(34, 197, 94, 0.2); color: #4ade80; border: 1px solid rgba(34, 197, 94, 0.4); }
    .tag-cf { background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); }
    .code-wrapper { background: var(--card-inner); border: 1px solid #1e293b; border-radius: 8px; padding: 10px; font-family: ui-monospace, monospace; font-size: 11.5px; color: #38bdf8; word-break: break-all; user-select: all; max-height: 52px; overflow-y: hidden; margin-bottom: 12px; }
    .card-footer { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
    .host-info { font-size: 11px; color: var(--text-muted); }
    .btn { background: var(--primary); color: #0b0f19; border: none; padding: 8px 14px; border-radius: 6px; font-size: 12px; font-weight: 700; cursor: pointer; transition: all 0.2s; white-space: nowrap; }
    .btn:hover { background: #7dd3fc; }
    .btn.copied { background: #10b981 !important; color: #ffffff !important; }
    .btn-sub { background: #10b981; color: #ffffff; }
    .btn-sub:hover { background: #059669; }
    #toast { position: fixed; bottom: 24px; right: 24px; background: #10b981; color: white; padding: 12px 20px; border-radius: 8px; font-weight: 600; font-size: 14px; box-shadow: 0 10px 25px rgba(0,0,0,0.4); opacity: 0; transform: translateY(20px); transition: all 0.3s; pointer-events: none; z-index: 100; }
    #toast.show { opacity: 1; transform: translateY(0); }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="badge"><span class="badge-dot"></span> Port 80 Zero-Data Bypass Active</div>
      <h1>🇱🇰 Dialog Port 80 Zero-Data VLESS Hub</h1>
      <p class="subtitle">සාමාන්‍ය ඩේටා 0 MB ඇති විට Dialog DPI මඟින් Block නොවී Social Media පැකේජයෙන් පමණක් Full Internet ලබාගැනීමට පහත <b>[Port 80]</b> Nodes භාවිත කරන්න.</p>

      <div class="alert-box">
        💡 <b>වැදගත්:</b> සාමාන්‍ය ඩේටා නොකැපී පැකේජයෙන් පමණක් Unlimited වීමට පහත <b>[Port 80] No-TLS</b> Nodes භාවිත කරන්න!
      </div>

      <div class="sub-banner">
        <div class="sub-info">
          <div class="sub-title">📥 All-in-One Subscription Link</div>
          <div class="sub-url">${subLink}</div>
        </div>
        <button class="btn btn-sub" onclick="copyText('${subLink}', this)">📋 Copy Subscription Link</button>
      </div>
    </div>

    <div class="grid">
      ${cardsHtml}
    </div>
  </div>

  <div id="toast">Copied to clipboard! ✓</div>

  <script>
    function showToast(msg) {
      const t = document.getElementById('toast');
      t.innerText = msg;
      t.classList.add('show');
      setTimeout(() => t.classList.remove('show'), 2000);
    }
    function copyConfig(elemId, btn) {
      const text = document.getElementById(elemId).innerText.trim();
      navigator.clipboard.writeText(text).then(() => {
        btn.innerText = '✓ Copied!';
        btn.classList.add('copied');
        showToast('Link copied to clipboard!');
        setTimeout(() => {
          btn.innerText = '📋 Copy Link';
          btn.classList.remove('copied');
        }, 2000);
      });
    }
    function copyText(text, btn) {
      navigator.clipboard.writeText(text).then(() => {
        btn.innerText = '✓ Copied!';
        btn.classList.add('copied');
        showToast('Subscription link copied!');
        setTimeout(() => {
          btn.innerText = '📋 Copy Subscription Link';
          btn.classList.remove('copied');
        }, 2000);
      });
    }
  </script>
</body>
</html>`;
}
