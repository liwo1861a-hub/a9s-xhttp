const http = require('http');
const net = require('net');

// 运行时配置（支持环境变量动态注入，零外部依赖）
const PORT = process.env.PORT || 8080;
const RAW_UUID = process.env.UUID || '81fedb63-36c2-4c88-857d-4692c1fca5f5';
const XHTTP_PATH = process.env.XHTTP_PATH || '/telemetry/v1/stream';

// 将 UUID 转为标准 16 字节 Buffer 进行内存快速比对
function uuidToBuffer(str) {
  const hex = str.replace(/-/g, '');
  return Buffer.from(hex, 'hex');
}
const TARGET_UUID_BUF = uuidToBuffer(RAW_UUID);

// 会话管理：管理 XHTTP 的上行(Upload)与下行(Download)会话配对
const sessions = new Map();

function getOrCreateSession(sessionId) {
  let session = sessions.get(sessionId);
  if (!session) {
    session = {
      id: sessionId,
      targetSocket: null,
      downloadRes: null,
      pendingUploads: [],
      vlessParsed: false,
      createdAt: Date.now()
    };
    sessions.set(sessionId, session);
  }
  return session;
}

function cleanSession(sessionId) {
  const session = sessions.get(sessionId);
  if (session) {
    if (session.targetSocket) {
      session.targetSocket.destroy();
    }
    if (session.downloadRes && !session.downloadRes.writableEnded) {
      session.downloadRes.end();
    }
    sessions.delete(sessionId);
  }
}

// 定时清理超时死会话（5分钟无活动自动释放内存）
setInterval(() => {
  const now = Date.now();
  for (const [id, s] of sessions.entries()) {
    if (now - s.createdAt > 300000 && !s.targetSocket) {
      cleanSession(id);
    }
  }
}, 60000);

// VLESS 头部解析与转发连接器
function handleVlessData(session, chunk) {
  if (session.vlessParsed && session.targetSocket) {
    if (session.targetSocket.writable) {
      session.targetSocket.write(chunk);
    }
    return;
  }

  // 第一次数据包，解析 VLESS 头部
  if (chunk.length < 18) {
    return;
  }

  // 1. 版本号 (1 byte)
  const version = chunk[0];
  // 2. UUID 校验 (16 bytes)
  const clientUuid = chunk.slice(1, 17);
  if (!clientUuid.equals(TARGET_UUID_BUF)) {
    cleanSession(session.id);
    return;
  }

  // 3. 附加信息长度 (1 byte)
  const optLen = chunk[17];
  let offset = 18 + optLen;

  // 4. 指令 (1 byte: 1=TCP, 2=UDP)
  const cmd = chunk[offset++];
  // 5. 目标端口 (2 bytes, big-endian)
  const targetPort = chunk.readUInt16BE(offset);
  offset += 2;

  // 6. 地址类型 (1 byte: 1=IPv4, 2=Domain, 3=IPv6)
  const addrType = chunk[offset++];
  let targetHost = '';

  if (addrType === 1) { // IPv4
    targetHost = `${chunk[offset++]}.${chunk[offset++]}.${chunk[offset++]}.${chunk[offset++]}`;
  } else if (addrType === 2) { // 域名
    const domainLen = chunk[offset++];
    targetHost = chunk.slice(offset, offset + domainLen).toString('utf8');
    offset += domainLen;
  } else if (addrType === 3) { // IPv6
    const ipv6Buf = chunk.slice(offset, offset + 16);
    const parts = [];
    for (let i = 0; i < 16; i += 2) {
      parts.push(ipv6Buf.readUInt16BE(i).toString(16));
    }
    targetHost = parts.join(':');
    offset += 16;
  } else {
    cleanSession(session.id);
    return;
  }

  session.vlessParsed = true;
  const payload = chunk.slice(offset);

  // 建立与目标地址的 TCP 连接
  const targetSocket = net.connect({ host: targetHost, port: targetPort }, () => {
    // VLESS 响应头：1 byte 版本(0) + 1 byte 附加信息长度(0)
    const respHeader = Buffer.from([0, 0]);
    if (session.downloadRes && !session.downloadRes.writableEnded) {
      session.downloadRes.write(respHeader);
    }
    if (payload.length > 0) {
      targetSocket.write(payload);
    }
  });

  targetSocket.on('data', (data) => {
    if (session.downloadRes && !session.downloadRes.writableEnded) {
      session.downloadRes.write(data);
    }
  });

  targetSocket.on('error', () => {
    cleanSession(session.id);
  });

  targetSocket.on('end', () => {
    cleanSession(session.id);
  });

  targetSocket.on('close', () => {
    cleanSession(session.id);
  });

  session.targetSocket = targetSocket;
}

// 伪装响应式前台页面 HTML
const FAKE_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>NexusTelemetry Cloud • High Performance Metrics</title>
  <style>
    :root {
      --bg: #0b0f19;
      --card: #111827;
      --border: #1f2937;
      --primary: #3b82f6;
      --success: #10b981;
      --text: #f3f4f6;
      --muted: #9ca3af;
    }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background-color: var(--bg);
      color: var(--text);
      display: flex;
      flex-direction: column;
      min-height: 100vh;
      align-items: center;
      justify-content: center;
      padding: 20px;
      box-sizing: border-box;
    }
    .container {
      max-width: 800px;
      width: 100%;
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 32px;
      box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.5);
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: rgba(16, 185, 129, 0.1);
      color: var(--success);
      padding: 4px 12px;
      border-radius: 9999px;
      font-size: 13px;
      font-weight: 600;
    }
    .badge::before {
      content: '';
      width: 8px;
      height: 8px;
      background: var(--success);
      border-radius: 50%;
      box-shadow: 0 0 8px var(--success);
    }
    h1 { margin: 16px 0 8px 0; font-size: 28px; }
    p.desc { color: var(--muted); margin: 0 0 24px 0; font-size: 15px; }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }
    .card {
      background: rgba(255, 255, 255, 0.02);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px;
    }
    .card-label { font-size: 13px; color: var(--muted); margin-bottom: 6px; }
    .card-val { font-size: 20px; font-weight: 700; color: #fff; }
    .footer { text-align: center; color: var(--muted); font-size: 13px; margin-top: 24px; }
  </style>
</head>
<body>
  <div class="container">
    <div class="badge">SYSTEM OPERATIONAL</div>
    <h1>NexusTelemetry Node Edge</h1>
    <p class="desc">High-throughput, real-time distributed telemetry collector with zero-latency edge streaming.</p>
    <div class="grid">
      <div class="card">
        <div class="card-label">HTTP Engine</div>
        <div class="card-val">Node.js HTTP/1.1</div>
      </div>
      <div class="card">
        <div class="card-label">Ingress State</div>
        <div class="card-val" style="color: var(--success);">Active</div>
      </div>
      <div class="card">
        <div class="card-label">Latency Metric</div>
        <div class="card-val" id="lat">0.8 ms</div>
      </div>
      <div class="card">
        <div class="card-label">Stream Protocol</div>
        <div class="card-val">Chunked Stream</div>
      </div>
    </div>
    <div style="background: rgba(0,0,0,0.3); padding: 12px 16px; border-radius: 8px; font-family: monospace; font-size: 13px; color: var(--muted);">
      GET /healthz ➔ 200 OK | Uptime: OK | Telemetry Endpoints Ready
    </div>
  </div>
  <div class="footer">© 2026 NexusTelemetry Cloud Infrastructure. All rights reserved.</div>
</body>
</html>`;

// 创建 HTTP 服务器
const server = http.createServer((req, res) => {
  const reqUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = reqUrl.pathname;

  // 1. 健康检查与伪装主页路由
  if (pathname === '/' || pathname === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(FAKE_HTML);
    return;
  }

  if (pathname === '/healthz' || pathname === '/api/ping') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', uptime: process.uptime(), timestamp: Date.now() }));
    return;
  }

  // 2. XHTTP 代理路由解析
  if (pathname.startsWith(XHTTP_PATH)) {
    // 获取会话标识（通过 Query 参数、Header 或 XHTTP 规范头部传递）
    const sessionId = req.headers['x-session-id'] || reqUrl.searchParams.get('session') || req.headers['x-request-id'] || 'default-session-' + req.socket.remotePort;
    const session = getOrCreateSession(sessionId);

    // XHTTP 下载流 (GET)
    if (req.method === 'GET') {
      res.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        'Transfer-Encoding': 'chunked',
        'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no'
      });
      session.downloadRes = res;

      req.on('close', () => {
        cleanSession(session.id);
      });
      return;
    }

    // XHTTP 上传流 (POST / PUT)
    if (req.method === 'POST' || req.method === 'PUT') {
      res.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        'Transfer-Encoding': 'chunked',
        'Connection': 'keep-alive'
      });

      req.on('data', (chunk) => {
        handleVlessData(session, chunk);
      });

      req.on('end', () => {
        res.end();
      });

      req.on('error', () => {
        cleanSession(session.id);
      });
      return;
    }
  }

  // 默认兜底 404
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Telemetry Edge Service listening on port ${PORT}`);
});
