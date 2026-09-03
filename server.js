const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

let AdmZip;
try {
  AdmZip = require('adm-zip');
} catch (e) {
  // fallback if not installed
}

const PORT = parseInt(process.env.PORT || '8080', 10);
const UUID = process.env.UUID || '81fedb63-36c2-4c88-857d-4692c1fca5f5';
const XHTTP_PATH = process.env.XHTTP_PATH || '/telemetry/v1/stream';

console.log(`[INIT] Starting Engine on Port ${PORT}...`);
console.log(`[INIT] UUID: ${UUID}, XHTTP Path: ${XHTTP_PATH}`);

// 下载文件
async function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    const getReq = (targetUrl) => {
      const client = targetUrl.startsWith('https') ? https : http;
      client.get(targetUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return getReq(res.headers.location);
        }
        if (res.statusCode !== 200) {
          return reject(new Error(`Download failed with status ${res.statusCode}`));
        }
        res.pipe(file);
        file.on('finish', () => {
          file.close(resolve);
        });
      }).on('error', reject);
    };
    getReq(url);
  });
}

async function setupXray() {
  const xrayBin = path.join(__dirname, 'xray');
  if (fs.existsSync(xrayBin)) {
    console.log('[XRAY] Binary exists.');
    return;
  }

  const zipPath = path.join(__dirname, 'xray.zip');
  const urls = [
    'https://github.com/XTLS/Xray-core/releases/download/v26.3.27/Xray-linux-64.zip',
    'https://github.com/XTLS/Xray-core/releases/latest/download/Xray-linux-64.zip'
  ];

  let downloaded = false;
  for (const u of urls) {
    try {
      console.log(`[XRAY] Downloading core from ${u}...`);
      await downloadFile(u, zipPath);
      downloaded = true;
      break;
    } catch (e) {
      console.warn(`[XRAY] Download error from ${u}: ${e.message}`);
    }
  }

  if (!downloaded) {
    throw new Error('Failed to download core from all mirrors.');
  }

  console.log('[XRAY] Extracting core archive...');
  if (AdmZip) {
    const zip = new AdmZip(zipPath);
    zip.extractEntryTo('xray', __dirname, false, true);
  } else {
    require('child_process').execSync(`unzip -o "${zipPath}" xray -d "${__dirname}"`);
  }

  fs.chmodSync(xrayBin, '755');
  if (fs.existsSync(zipPath)) fs.unlinkSync(zipPath);
  console.log('[XRAY] Core ready.');
}

function generateConfig() {
  const config = {
    log: {
      loglevel: "warning"
    },
    inbounds: [
      {
        port: PORT,
        listen: "0.0.0.0",
        protocol: "vless",
        settings: {
          clients: [
            {
              id: UUID,
              level: 0
            }
          ],
          decryption: "none"
        },
        streamSettings: {
          network: "xhttp",
          xhttpSettings: {
            path: XHTTP_PATH,
            mode: "auto"
          }
        }
      }
    ],
    outbounds: [
      {
        protocol: "freedom"
      }
    ]
  };

  const configPath = path.join(__dirname, 'config.json');
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf8');
  return configPath;
}

async function run() {
  try {
    await setupXray();
    const configPath = generateConfig();
    const xrayBin = path.join(__dirname, 'xray');

    console.log('[XRAY] Launching process...');
    const proc = spawn(xrayBin, ['run', '-c', configPath], { stdio: 'inherit' });

    proc.on('exit', (code, signal) => {
      console.warn(`[XRAY] Process exited (${code}/${signal}). Restarting in 3s...`);
      setTimeout(run, 3000);
    });

    proc.on('error', (err) => {
      console.error('[XRAY] Process error:', err);
    });
  } catch (err) {
    console.error('[FATAL] Failed to start core:', err);
    http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/plain' });
      res.end('Edge Runner Active: ' + err.message);
    }).listen(PORT);
  }
}

run();
