# 🌐 Cloudflare Worker VLESS VPN Proxy

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-F38020?logo=cloudflare&logoColor=white)](https://workers.cloudflare.com/)

A lightweight, serverless **VLESS over WebSocket proxy** running entirely on **Cloudflare Workers** using native `cloudflare:sockets` TCP connectivity.

---

## ⚡ Features / විශේෂාංග
- **VLESS Protocol over WebSocket (WS)** with TLS support.
- Powered by Cloudflare's global edge network.
- Uses native `cloudflare:sockets` TCP connection handling.
- Compatible with **v2rayNG (Android)**, **v2rayN (Windows)**, **Shadowrocket (iOS)**, **Clash Meta**, **Sing-Box**, and **NekoBox**.
- Zero maintenance and easy one-click deployment.

---

## 🚀 Deployment Guide / ස්ථාපනය කරන ආකාරය

### ක්‍රමය 1: Cloudflare Dashboard හරහා (පහසුම ක්‍රමය)

1. **[Cloudflare Dashboard](https://dash.cloudflare.com/)** වෙත Log වන්න.
2. **Workers & Pages** -> **Create Application** -> **Create Worker** තෝරන්න.
3. Worker එකට නමක් දී **Deploy** ඔබන්න.
4. **Edit Code** ඔබා එහි ඇති කේතය ඉවත් කර `_worker.js` (හෝ `vless-proxy-worker.js`) හි ඇති කේතය Paste කරන්න.
5. **Save and Deploy** ඔබන්න.

---

### ක්‍රමය 2: Wrangler CLI මඟින් (Local Machine)

```bash
# 1. Clone repository
git clone https://github.com/ireshad465-gif/vless-proxy-worker.git
cd vless-proxy-worker

# 2. Install dependencies
npm install

# 3. Deploy to Cloudflare
npx wrangler deploy
```

---

## ⚙️ Configuration / සැකසුම්

`_worker.js` ගොනුවේ පහත අගයන් අවශ්‍ය පරිදි වෙනස් කරගත හැක:

```javascript
const userID = 'bdeb28a4-ca3f-4665-9da2-6d92b718e4eb'; // ඔබගේ කැමති UUID එක
const proxyIP = ''; // අවශ්‍ය නම් Proxy IP එකක් ඇතුළත් කරන්න (හිස්ව තැබිය හැක)
```

---

## 📱 Client Configuration / V2Ray App සැකසුම්

ඔබගේ Worker එක Deploy කළ පසු ලැබෙන Domain එක (උදා: `your-worker-name.workers.dev`) භාවිතයෙන් V2Ray Link එක සාදාගත හැක:

### VLESS Link Structure:
```text
vless://bdeb28a4-ca3f-4665-9da2-6d92b718e4eb@<YOUR_WORKER_DOMAIN>:443?encryption=none&security=tls&sni=<YOUR_WORKER_DOMAIN>&type=ws&host=<YOUR_WORKER_DOMAIN>&path=%2F#VLESS-Cloudflare
```

### Manual Configuration Settings:
| Setting / Parameter | Value |
| :--- | :--- |
| **Protocol** | `VLESS` |
| **Address (Server)** | `<YOUR_WORKER_DOMAIN>` (හෝ Clean IP) |
| **Port** | `443` |
| **UUID / User ID** | `bdeb28a4-ca3f-4665-9da2-6d92b718e4eb` |
| **Encryption** | `none` |
| **Transport** | `WebSocket (ws)` |
| **Path** | `/` |
| **Host / SNI** | `<YOUR_WORKER_DOMAIN>` |
| **TLS** | `Enabled` |

---

## 🔒 Security Notice
* ඔබේම අභිමතය පරිදි ආරක්ෂාව සඳහා `userID` (UUID) එක අලුත් එකකින් වෙනස් කරගන්න (Generate UUID: `uuidgen` හෝ [uuidgenerator.net](https://www.uuidgenerator.net/)).

---

## 📜 License
This project is licensed under the [MIT License](LICENSE).
