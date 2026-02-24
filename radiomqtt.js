// ============================================================
//  FM MONITOR v5 — Melhor dos dois mundos
//  Arquitetura: MQTT (Doc 3) + GeoIP + Rastreamento + Auto-mute (Doc 4)
//  Servidor: publica dados no broker HiveMQ
//  Cliente:  subscreve no broker via MQTT-over-WebSocket
//  Áudio:    Icecast (cliente ouve URL pública)
//  PCM raw nunca vai ao cliente
// ============================================================

const http    = require('http');
const https   = require('https');
const fs      = require('fs');
const { spawn } = require('child_process');
const mqtt    = require('mqtt');

// ─── CONFIG ──────────────────────────────────────────────────
const CONFIG = {
    webPort:   3000,
    frequency: 101700000,
    gain:      '19.2',
    ppm:       0,
    sampleRate: 44100,

    // HiveMQ Cloud — troque pelo seu cluster
    mqttUrl:      'mqtts://600e9b5cab1a404692f2659433ff4a66.s1.eu.hivemq.cloud:8883',
    mqttUser:     'admin',
    mqttPassword: 'Admin123',

    // Mesmo broker mas porta WebSocket TLS (para o browser)
    mqttWsUrl:    'wss://600e9b5cab1a404692f2659433ff4a66.s1.eu.hivemq.cloud:8884/mqtt',

    // Prefixo de todos os tópicos
    topic: 'fm',

    // URL pública do Icecast para ouvir no browser
    icecastListenUrl: 'http://192.168.192.141:8000/jeri',

    // URL de envio do áudio para o Icecast (ffmpeg push)
    icecastPushUrl: 'icecast://source:hackme@192.168.192.141:8000/jeri',

    // Senha para desbloquear controle do stream no browser
    streamPassword: 'radio123',
};

// ─── TÓPICOS MQTT ────────────────────────────────────────────
const T = {
    status:        CONFIG.topic + '/status',
    rds:           CONFIG.topic + '/rds',
    rdsClear:      CONFIG.topic + '/rds/clear',
    level:         CONFIG.topic + '/level',
    stereo:        CONFIG.topic + '/stereo',
    memories:      CONFIG.topic + '/memories',
    streamStatus:  CONFIG.topic + '/stream/status',
    streamUrl:     CONFIG.topic + '/stream/url',
    users:         CONFIG.topic + '/users',
    userList:      CONFIG.topic + '/users/list',      // lista detalhada de usuários
    presence:      CONFIG.topic + '/presence/+',

    cmdTune:       CONFIG.topic + '/cmd/tune',
    cmdMemory:     CONFIG.topic + '/cmd/memory',
    cmdIcecast:    CONFIG.topic + '/cmd/icecast',
    cmdGetStream:  CONFIG.topic + '/cmd/get_stream',
};

// ─── ESTADO ──────────────────────────────────────────────────
let rtlProcess     = null;
let rdsProcess     = null;
let pilotProcess   = null;
let icecastProcess = null;
let currentFreq    = CONFIG.frequency;
let rdsBuffer      = '';
let lastRdsTime    = 0;
let levelHistory   = [];
let streamStatus   = 'off';
let streamStatusMsg = '';
let userCount      = 0;

// Rastreia clientes via heartbeat MQTT (presença + dados geo)
const presenceMap = new Map(); // clientId → { timestamp, ip, city, region, country, freq, connectedAt }
const PRESENCE_TIMEOUT = 90000;

const MEMORIES_FILE = './memories.json';
const STREAM_FILE   = './stream.json';
let sharedMemories  = [null, null, null, null];
let savedStreamUrl  = '';

try { if (fs.existsSync(MEMORIES_FILE)) sharedMemories = JSON.parse(fs.readFileSync(MEMORIES_FILE, 'utf8')); } catch(e) {}
try { if (fs.existsSync(STREAM_FILE))   savedStreamUrl = JSON.parse(fs.readFileSync(STREAM_FILE, 'utf8')).url || ''; } catch(e) {}
if (!savedStreamUrl) savedStreamUrl = CONFIG.icecastPushUrl;

// ─── GEOIP ───────────────────────────────────────────────────
function getGeoIp(ip) {
    return new Promise((resolve) => {
        const cleanIp = ip.replace('::ffff:', '');
        // IPs privados
        if (cleanIp === '127.0.0.1' || cleanIp === '::1' ||
            cleanIp.startsWith('192.168.') || cleanIp.startsWith('10.') ||
            /^172\.(1[6-9]|2\d|3[01])\./.test(cleanIp)) {
            resolve({ city: 'Rede Local', region: '', country: cleanIp });
            return;
        }
        const req = http.get('http://ip-api.com/json/' + cleanIp + '?fields=city,regionName,country,status', (res) => {
            let data = '';
            res.on('data', d => data += d);
            res.on('end', () => {
                try {
                    const json = JSON.parse(data);
                    if (json.status === 'success') {
                        resolve({ city: json.city, region: json.regionName, country: json.country });
                    } else {
                        resolve({ city: '?', region: '?', country: '?' });
                    }
                } catch(e) { resolve({ city: '?', region: '?', country: '?' }); }
            });
        });
        req.on('error', () => resolve({ city: '?', region: '?', country: '?' }));
        req.setTimeout(4000, () => { req.destroy(); resolve({ city: '?', region: '?', country: '?' }); });
    });
}

// ─── NTFY ────────────────────────────────────────────────────
function sendNtfy(title, message) {
    const options = {
        hostname: 'ntfy.sh',
        path: '/radiorumbora',
        method: 'POST',
        headers: { 'Content-Type': 'text/plain', 'Title': title, 'Priority': 'default', 'Tags': 'radio' }
    };
    const req = https.request(options, (res) => console.log('[ntfy] status:', res.statusCode));
    req.on('error', (e) => console.log('[ntfy] error:', e.message));
    req.setTimeout(6000, () => req.destroy());
    req.write(message);
    req.end();
}

// ─── MQTT CLIENT ─────────────────────────────────────────────
const mqttClient = mqtt.connect(CONFIG.mqttUrl, {
    username: CONFIG.mqttUser,
    password: CONFIG.mqttPassword,
    clientId: 'fm-server-' + Math.random().toString(16).slice(2, 8),
    clean: true,
    reconnectPeriod: 3000,
});

function pub(topic, payload, retain = false) {
    if (!mqttClient.connected) return;
    mqttClient.publish(topic, JSON.stringify(payload), { qos: 1, retain });
}

const throttle = {};
function pubThrottle(topic, payload, intervalMs, retain = false) {
    const now = Date.now();
    if (!throttle[topic] || now - throttle[topic] >= intervalMs) {
        throttle[topic] = now;
        pub(topic, payload, retain);
    }
}

mqttClient.on('connect', () => {
    console.log('[MQTT] Conectado ao broker');
    const cmds = [T.cmdTune, T.cmdMemory, T.cmdIcecast, T.cmdGetStream, T.presence];
    mqttClient.subscribe(cmds, { qos: 1 }, (err) => {
        if (err) console.error('[MQTT] Erro ao subscrever:', err.message);
        else console.log('[MQTT] Subscrito nos tópicos de comando');
    });
    pub(T.status,       { freq: currentFreq, sampleRate: CONFIG.sampleRate }, true);
    pub(T.memories,     sharedMemories, true);
    pub(T.streamStatus, { status: streamStatus, msg: streamStatusMsg }, true);
});

mqttClient.on('error',     (e) => console.error('[MQTT] Erro:', e.message));
mqttClient.on('reconnect', ()  => console.log('[MQTT] Reconectando...'));
mqttClient.on('offline',   ()  => console.log('[MQTT] Offline'));

// ─── PRESENÇA E RASTREAMENTO ─────────────────────────────────
function updateUserCount() {
    const now = Date.now();
    for (const [id, u] of presenceMap) {
        if (now - u.timestamp > PRESENCE_TIMEOUT) presenceMap.delete(id);
    }
    const count = presenceMap.size;
    if (count !== userCount) {
        userCount = count;
        pub(T.users, { count }, true);
        console.log('[Presence] Usuários online:', count);
    }
    // Publica lista detalhada para o painel admin
    const list = Array.from(presenceMap.values()).map(u => ({
        city:    u.city    || '...',
        region:  u.region  || '',
        country: u.country || '',
        freq:    u.freq    || null,
        connectedAt: u.connectedAt || '',
    }));
    pub(T.userList, list, false);
}
setInterval(updateUserCount, 15000);

// ─── HANDLER DE COMANDOS MQTT ────────────────────────────────
mqttClient.on('message', (topic, buffer) => {
    let cmd;
    try { cmd = JSON.parse(buffer.toString()); } catch(e) { return; }

    if (topic === T.cmdTune) {
        const freqHz  = Math.floor(parseFloat(cmd.freq) * 1000000);
        const freqMHz = parseFloat(cmd.freq);
        if (freqMHz < 76.1 || freqMHz > 108) return;
        currentFreq = freqHz;
        console.log('[CMD] Tune:', freqMHz.toFixed(2), 'MHz');
        pub(T.status,   { freq: currentFreq, sampleRate: CONFIG.sampleRate }, true);
        pub(T.rdsClear, '', false);
        pub(T.stereo,   { stereo: false }, false);
        // Atualiza freq do cliente na presenceMap
        if (cmd._clientId && presenceMap.has(cmd._clientId)) {
            presenceMap.get(cmd._clientId).freq = freqMHz;
            const u = presenceMap.get(cmd._clientId);
            const loc = [u.city, u.region, u.country].filter(Boolean).join(', ');
            sendNtfy('Sintonizou', (u.ip || '?') + ' | ' + loc + ' | ' + freqMHz.toFixed(2) + ' MHz');
        } else {
            sendNtfy('Sintonizou', freqMHz.toFixed(2) + ' MHz');
        }
        startRadio(freqHz);
    }

    if (topic === T.cmdMemory) {
        const idx  = parseInt(cmd.index);
        const freq = parseFloat(cmd.freq);
        if (idx >= 0 && idx <= 3 && freq >= 76.1 && freq <= 108) {
            sharedMemories[idx] = freq;
            saveMemoriesToDisk();
            pub(T.memories, sharedMemories, true);
            console.log('[CMD] Memória', idx, '=', freq);
            if (cmd._clientId && presenceMap.has(cmd._clientId)) {
                const u = presenceMap.get(cmd._clientId);
                const loc = [u.city, u.region, u.country].filter(Boolean).join(', ');
                sendNtfy('Salvou M' + (idx + 1), (u.ip || '?') + ' | ' + loc + ' | ' + freq.toFixed(2) + ' MHz');
            } else {
                sendNtfy('Salvou M' + (idx + 1), freq.toFixed(2) + ' MHz');
            }
        }
    }

    if (topic === T.cmdIcecast) {
        const url = cmd.url || null;
        if (url) { savedStreamUrl = url; saveStreamToDisk(); }
        startIcecast(url);
    }

    if (topic === T.cmdGetStream) {
        pub(T.streamUrl, { url: savedStreamUrl }, false);
    }

    // Heartbeat de presença — payload pode incluir dados do cliente
    if (topic.startsWith(CONFIG.topic + '/presence/')) {
        const clientId = topic.split('/').pop();
        if (buffer.toString() === '0') {
            // LWT — cliente desconectou
            const u = presenceMap.get(clientId);
            if (u) {
                const loc = [u.city, u.region, u.country].filter(Boolean).join(', ');
                const freq = u.freq ? ' | ' + u.freq.toFixed(2) + ' MHz' : '';
                sendNtfy('Desconectou', (u.ip || '?') + ' | ' + loc + freq);
            }
            presenceMap.delete(clientId);
        } else {
            const existing = presenceMap.get(clientId);
            if (!existing) {
                // Novo cliente — tenta enriquecer com geo se payload tiver IP
                let ip = '?';
                try { const p = JSON.parse(buffer.toString()); if (p.ip) ip = p.ip; } catch(e) {}
                const connectedAt = new Date().toLocaleString('pt-BR', { timeZone: 'America/Fortaleza' });
                presenceMap.set(clientId, { timestamp: Date.now(), ip, city: '...', region: '', country: '', freq: null, connectedAt });
                getGeoIp(ip).then(geo => {
                    const u = presenceMap.get(clientId);
                    if (u) {
                        u.city = geo.city; u.region = geo.region; u.country = geo.country;
                        const loc = [geo.city, geo.region, geo.country].filter(Boolean).join(', ');
                        sendNtfy('Conectou', ip + ' | ' + loc + ' | ' + connectedAt);
                    }
                });
            } else {
                existing.timestamp = Date.now();
            }
        }
        updateUserCount();
    }
});

// ─── PERSISTÊNCIA ─────────────────────────────────────────────
function saveMemoriesToDisk() {
    try { fs.writeFileSync(MEMORIES_FILE, JSON.stringify(sharedMemories)); } catch(e) {}
}
function saveStreamToDisk() {
    try { fs.writeFileSync(STREAM_FILE, JSON.stringify({ url: savedStreamUrl })); } catch(e) {}
}

// ─── QUALIDADE DE SINAL ───────────────────────────────────────
const LEVEL_HISTORY_SIZE = 20;

function computeSignalQuality(level) {
    levelHistory.push(level);
    if (levelHistory.length > LEVEL_HISTORY_SIZE) levelHistory.shift();
    const rdsRecent = (Date.now() - lastRdsTime) < 5000;
    if (rdsRecent) return 5;
    if (levelHistory.length < 5) return 0;
    const mean     = levelHistory.reduce((a, b) => a + b, 0) / levelHistory.length;
    const variance = levelHistory.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / levelHistory.length;
    const cv       = mean > 0.001 ? variance / (mean * mean) : 999;
    if (mean < 0.005) return 0;
    if (cv > 0.5)     return 1;
    if (cv > 0.3)     return 2;
    if (cv > 0.15)    return 3;
    if (cv > 0.05)    return 4;
    return 5;
}

// ─── DETECTOR DE PILOTO STEREO ────────────────────────────────
class StereoDetector {
    constructor() {
        this.sampleRate   = 171000;
        this.pilotFreq    = 19000;
        this.w            = 2 * Math.PI * this.pilotFreq / this.sampleRate;
        this.coeff        = 2 * Math.cos(this.w);
        this.s1 = 0; this.s2 = 0;
        this.count        = 0;
        this.blockSize    = 1710;
        this.totalPower   = 0;
        this.isStereo     = false;
        this.threshold    = 0.00035;
        this.thresholdOff = 0.00025;
        this.smoothRatio  = 0;
        this.alpha        = 0.04;
    }
    process(chunk) {
        const int16 = new Int16Array(chunk.buffer, chunk.byteOffset, chunk.length / 2);
        for (let i = 0; i < int16.length; i++) {
            const sample = int16[i] / 32768.0;
            const s0 = sample + this.coeff * this.s1 - this.s2;
            this.s2 = this.s1; this.s1 = s0;
            this.totalPower += sample * sample;
            this.count++;
            if (this.count >= this.blockSize) {
                const power = (this.s1 * this.s1 + this.s2 * this.s2 - this.coeff * this.s1 * this.s2) / (this.blockSize * this.blockSize);
                const total = this.totalPower / this.blockSize;
                const ratio = total > 0.00001 ? power / total : 0;
                this.smoothRatio += (ratio - this.smoothRatio) * this.alpha;
                const rdsRecent = (Date.now() - lastRdsTime) < 5000;
                if (!rdsRecent) {
                    let stereo = this.isStereo;
                    if (!this.isStereo && this.smoothRatio > this.threshold)    stereo = true;
                    if (this.isStereo  && this.smoothRatio < this.thresholdOff) stereo = false;
                    if (stereo !== this.isStereo) {
                        this.isStereo = stereo;
                        console.log('[Pilot] stereo:', stereo);
                        pubThrottle(T.stereo, { stereo }, 2000);
                    }
                } else {
                    this.isStereo = false;
                }
                this.s1 = 0; this.s2 = 0;
                this.count = 0; this.totalPower = 0;
            }
        }
    }
}
const stereoDetector = new StereoDetector();

// ─── ICECAST / STREAM ─────────────────────────────────────────
function broadcastStreamStatus(status, msg) {
    streamStatus    = status;
    streamStatusMsg = msg || '';
    pub(T.streamStatus, { status, msg: streamStatusMsg }, true);
}

function startIcecast(url) {
    if (icecastProcess) { try { process.kill(-icecastProcess.pid, 'SIGKILL'); } catch(e) {} icecastProcess = null; }
    if (!url) { broadcastStreamStatus('off', ''); return; }

    broadcastStreamStatus('connecting', 'Conectando...');
    const cmd = `ffmpeg -re -f s16le -ar 44100 -ac 1 -i pipe:0 -c:a libmp3lame -b:a 128k -f mp3 "${url}"`;
    console.log('[Icecast] Iniciando:', cmd);
    icecastProcess = spawn('bash', ['-c', cmd]);

    icecastProcess.stdin.on('error', (e) => {
        if (e.code !== 'EPIPE' && e.code !== 'ERR_STREAM_DESTROYED') {
            console.error('[Icecast] stdin erro:', e.message);
        }
    });

    let ready = false, stderrBuf = '';
    icecastProcess.stderr.on('data', (d) => {
        const line = d.toString();
        stderrBuf += line;
        const last = line.trim().split('\n').pop();
        console.log('[ffmpeg]', last);
        if (!ready) {
            if (/Output #0/i.test(stderrBuf)) {
                ready = true;
                broadcastStreamStatus('active', 'Streaming ativo');
            } else if (/Connection refused|403 Forbidden|401 Unauthorized|Failed|error/i.test(line)) {
                broadcastStreamStatus('error', last.substring(0, 80));
            }
        }
        const m = line.match(/size=\s*([\d]+)kB\s+time=([\d:\.]+)\s+bitrate=\s*([\d\.]+)kbits/);
        if (m && ready) broadcastStreamStatus('active', `${m[2]} | ${m[3]} kbps | ${m[1]} kB`);
    });
    icecastProcess.on('close', (code) => {
        console.log('[Icecast] exited', code);
        ready = false;
        broadcastStreamStatus(code === 0 ? 'off' : 'error', code !== 0 ? 'ffmpeg encerrou (código ' + code + ')' : '');
        icecastProcess = null;
    });
}

// ─── RÁDIO SDR ────────────────────────────────────────────────
function killProcess(proc) {
    if (!proc) return;
    try { process.kill(-proc.pid, 'SIGKILL'); } catch(e) {
        try { proc.kill('SIGKILL'); } catch(e2) {}
    }
}

function startRadio(freq) {
    killProcess(rtlProcess);   rtlProcess   = null;
    killProcess(rdsProcess);   rdsProcess   = null;
    killProcess(pilotProcess); pilotProcess = null;
    setTimeout(() => _startRadio(freq), 800);
}

function _startRadio(freq) {
    currentFreq = freq;
    console.log(`[Radio] Sintonizando ${(freq / 1000000).toFixed(2)} MHz`);

    const fifo      = '/tmp/rds_fifo';
    const pilotFifo = '/tmp/pilot_fifo';
    const mkfifo = spawn('bash', ['-c', `[ -p ${fifo} ] || mkfifo ${fifo}; [ -p ${pilotFifo} ] || mkfifo ${pilotFifo}`]);

    mkfifo.on('close', () => {
        const cmd = `rtl_fm -f ${freq} -M wfm -s 171k -E deemp -g ${CONFIG.gain} -A std | tee ${fifo} | tee ${pilotFifo} | sox -t raw -r 171000 -e signed -b 16 -c 1 - -t raw -r 44100 -`;

        rdsProcess = spawn('bash', ['-c', `redsea -r 171000 < ${fifo}`], { detached: true });
        rdsProcess.stdout.on('data', (data) => {
            rdsBuffer += data.toString();
            const lines = rdsBuffer.split('\n');
            rdsBuffer = lines.pop();
            lines.forEach(l => {
                if (!l.trim()) return;
                try {
                    const rds = JSON.parse(l);
                    console.log('[RDS]', JSON.stringify(rds));
                    lastRdsTime = Date.now();
                    if (rds.di && rds.di.stereo !== undefined) {
                        pubThrottle(T.stereo, { stereo: rds.di.stereo }, 2000);
                    }
                    pubThrottle(T.rds, rds, 1000);
                } catch(e) {}
            });
        });
        rdsProcess.stderr.on('data', (d) => console.log(`[redsea] ${d.toString().trim()}`));
        rdsProcess.on('close', (c) => console.log(`[RDS] exited ${c}`));

        pilotProcess = spawn('bash', ['-c', `cat ${pilotFifo}`], { detached: true });
        pilotProcess.stdout.on('data', (chunk) => stereoDetector.process(chunk));
        pilotProcess.on('close', (c) => console.log(`[Pilot] exited ${c}`));

        setTimeout(() => {
            rtlProcess = spawn('bash', ['-c', cmd], { detached: true });
            rtlProcess.stdout.on('data', (chunk) => {
                const int16 = new Int16Array(chunk.buffer, chunk.byteOffset, chunk.length / 2);
                let sum = 0;
                for (let i = 0; i < int16.length; i++) sum += Math.abs(int16[i]);
                const level   = sum / int16.length / 32768.0;
                const quality = computeSignalQuality(level);

                pubThrottle(T.level, { audio: level, quality }, 250);

                if (icecastProcess && icecastProcess.stdin &&
                    !icecastProcess.stdin.destroyed && icecastProcess.stdin.writable) {
                    try { icecastProcess.stdin.write(chunk); } catch(e) {
                        if (e.code !== 'EPIPE') console.error('[Icecast] write erro:', e.message);
                    }
                }
            });
            rtlProcess.stderr.on('data', (d) => console.log(`[rtl_fm] ${d.toString().trim()}`));
            rtlProcess.on('close', (c) => console.log(`[Radio] exited ${c}`));
        }, 300);
    });
}

// ─── SERVIDOR HTTP ────────────────────────────────────────────
const server = http.createServer((req, res) => {
    if (req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(buildHtml());
    } else if (req.url === '/mqtt.js') {
        const mqttBrowserBundle = require('path').join(__dirname, 'node_modules/mqtt/dist/mqtt.min.js');
        fs.readFile(mqttBrowserBundle, (err, data) => {
            if (err) { res.writeHead(404); res.end('mqtt.min.js not found'); return; }
            res.writeHead(200, { 'Content-Type': 'application/javascript' });
            res.end(data);
        });
    } else {
        res.writeHead(404); res.end();
    }
});

// ─── HTML DO CLIENTE ──────────────────────────────────────────
function buildHtml() {
    return `<!DOCTYPE html>
<html lang="pt">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FM Monitor v5</title>
    <script src="/mqtt.js"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Orbitron:wght@400;700&display=swap');
        :root { --bg:#0a0c0f; --panel:#111418; --border:#1e2530; --green:#00e676; --teal:#00bcd4; }
        * { box-sizing:border-box; margin:0; padding:0; }
        body { background:var(--bg); color:#cdd5e0; font-family:'Share Tech Mono',monospace;
               display:flex; justify-content:center; align-items:center; min-height:100vh; padding:20px; }
        .panel { background:var(--panel); border:1px solid var(--border); border-radius:4px;
                 width:100%; max-width:420px; padding:24px; box-shadow:0 0 40px rgba(0,188,212,0.05); }
        .header { display:flex; justify-content:space-between; align-items:center;
                  margin-bottom:20px; border-bottom:1px solid var(--border); padding-bottom:14px; }
        .title { font-family:'Orbitron',sans-serif; font-size:0.85rem; letter-spacing:0.2em; color:var(--teal); }
        .status-dot { width:8px; height:8px; border-radius:50%; background:#444; transition:background 0.3s,box-shadow 0.3s; }
        .status-dot.on { background:var(--green); box-shadow:0 0 8px var(--green); animation:blink 1.5s ease-in-out infinite; }
        @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.3} }
        .freq-row { display:flex; flex-direction:column; align-items:center; justify-content:center; margin:16px 0; gap:4px; }
        .stereo-badge { font-size:0.55rem; letter-spacing:0.2em; color:transparent; background:#1a2530;
                        border:1px solid #2a3545; padding:2px 8px; border-radius:3px;
                        font-family:'Orbitron',sans-serif; transition:all 0.4s; user-select:none; }
        .stereo-badge.on { color:#0a0c0f; background:var(--green); border-color:var(--green); box-shadow:0 0 8px rgba(0,230,118,0.4); }
        .signal-bars { display:flex; align-items:flex-end; gap:2px; height:16px; }
        .signal-bars .bar { width:5px; border-radius:1px; background:#1e2530; transition:background 0.3s; }
        .signal-bars .bar:nth-child(1){height:4px} .signal-bars .bar:nth-child(2){height:6px}
        .signal-bars .bar:nth-child(3){height:9px} .signal-bars .bar:nth-child(4){height:12px}
        .signal-bars .bar:nth-child(5){height:16px}
        .bar.lit-good{background:var(--green);box-shadow:0 0 4px rgba(0,230,118,0.5)}
        .bar.lit-warn{background:#ffea00;box-shadow:0 0 4px rgba(255,234,0,0.4)}
        .bar.lit-bad {background:#ff1744;box-shadow:0 0 4px rgba(255,23,68,0.4)}
        .freq-display { font-family:'Orbitron',monospace; font-size:2.8rem; font-weight:700;
                        color:var(--teal); text-align:center; letter-spacing:0.05em;
                        text-shadow:0 0 20px rgba(0,188,212,0.4); }
        .freq-unit { font-size:1rem; color:#4a5568; margin-left:4px; }
        .controls { display:flex; gap:8px; margin-bottom:16px; }
        .controls input { flex:1; background:var(--bg); border:1px solid var(--border); color:#cdd5e0;
                          font-family:'Share Tech Mono',monospace; font-size:1rem; padding:10px 12px;
                          border-radius:3px; outline:none; }
        .controls input:focus { border-color:var(--teal); }
        .btn { background:transparent; border:1px solid var(--teal); color:var(--teal);
               font-family:'Orbitron',sans-serif; font-size:0.75rem; letter-spacing:0.1em;
               padding:10px 16px; border-radius:3px; cursor:pointer; transition:all 0.15s; }
        .btn:hover { background:rgba(0,188,212,0.1); }
        .audio-row { display:flex; gap:8px; margin-bottom:20px; }
        .btn-listen { flex:1; border-color:var(--green); color:var(--green); padding:12px; font-size:0.8rem; }
        .btn-listen.active { background:rgba(0,230,118,0.05); box-shadow:0 0 15px rgba(0,230,118,0.08); }
        .btn-listen.automuted { border-color:#ff8a65; color:#ff8a65; background:rgba(255,138,101,0.08); }
        .tuning-overlay { display:none; position:fixed; inset:0; background:rgba(10,12,15,0.88);
                          z-index:9998; flex-direction:column; align-items:center; justify-content:center; gap:16px; }
        .tuning-overlay.visible { display:flex; }
        .tuning-spinner { width:40px; height:40px; border:3px solid #1e2530; border-top-color:var(--teal);
                          border-radius:50%; animation:spin 0.8s linear infinite; }
        @keyframes spin { to { transform:rotate(360deg); } }
        .tuning-text { font-family:'Orbitron',sans-serif; font-size:0.8rem; letter-spacing:0.2em; color:var(--teal); }
        .tuning-freq { font-family:'Orbitron',sans-serif; font-size:1.6rem; font-weight:700; color:var(--teal); text-shadow:0 0 20px rgba(0,188,212,0.5); }
        .tuning-bar { width:180px; height:2px; background:#1e2530; border-radius:1px; overflow:hidden; }
        .tuning-bar-fill { height:100%; background:var(--teal); border-radius:1px; animation:tuningProgress 4s linear forwards; }
        @keyframes tuningProgress { from{width:0%} to{width:100%} }
        .btn-arrow { border-color:var(--teal); color:var(--teal); padding:10px 14px; font-size:1rem;
                     display:flex; align-items:center; justify-content:center; min-width:42px; }
        .btn-arrow:hover { background:rgba(0,188,212,0.15); }
        .btn-arrow svg { width:14px; height:14px; fill:none; stroke:currentColor; stroke-width:2.2; stroke-linecap:round; stroke-linejoin:round; }
        .vu-wrap { background:#0d0f12; border:1px solid #1a2030; border-radius:6px; padding:16px 16px 10px; }
        .vu-label { font-size:0.6rem; letter-spacing:0.2em; color:#3a4555; text-align:center; margin-bottom:12px; text-transform:uppercase; }
        canvas#vuCanvas { display:block; width:100%; }
        .vu-db { text-align:center; font-size:0.75rem; color:#4a5568; margin-top:8px; letter-spacing:0.1em; }
        .vu-db span { color:#7a8ea0; }
        .rds-wrap { background:#0d0f12; border:1px solid #1a2030; border-radius:6px;
                    padding:12px 16px; margin-top:12px; display:flex; flex-direction:column; gap:8px; }
        .rds-row { display:flex; align-items:center; gap:8px; font-size:0.75rem; }
        .rds-tag { color:#3a5060; letter-spacing:0.1em; font-size:0.65rem; min-width:28px; }
        .rds-val { color:#00e676; font-family:'Share Tech Mono',monospace; letter-spacing:0.05em; }
        .rds-rt  { color:#7a8ea0; font-size:0.7rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:280px; }
        .mem-wrap { background:#0d0f12; border:1px solid #1a2030; border-radius:6px; padding:12px 16px; margin-top:12px; }
        .mem-label { font-size:0.6rem; letter-spacing:0.2em; color:#3a4555; text-transform:uppercase; margin-bottom:10px; }
        .mem-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:6px; }
        .mem-btn { display:flex; flex-direction:column; align-items:center; background:#0a0c0f;
                   border:1px solid #1e2530; border-radius:4px; padding:8px 4px;
                   cursor:pointer; transition:all 0.15s; position:relative; }
        .mem-btn:hover { border-color:var(--teal); }
        .mem-btn.active { border-color:var(--teal); background:rgba(0,188,212,0.05); }
        .mem-btn.saving { border-color:#ff8a65; background:rgba(255,138,101,0.08); }
        .mem-freq { font-family:'Orbitron',sans-serif; font-size:0.75rem; color:var(--teal); letter-spacing:0.02em; margin:4px 0; }
        .mem-freq.empty { color:#2a3545; }
        .mem-progress { width:100%; height:2px; background:transparent; border-radius:1px; margin-top:4px; overflow:hidden; }
        .mem-progress-bar { height:100%; width:0%; background:#ff8a65; border-radius:1px; transition:width linear; }
        .mem-hint { font-size:0.55rem; color:#00e676; text-align:center; margin-top:8px; letter-spacing:0.05em; }

        /* Stream */
        .icecast-wrap { background:#0d0f12; border:1px solid #1a2030; border-radius:6px;
                        padding:12px 16px; margin-top:12px; display:flex; flex-direction:column; gap:8px; }
        .icecast-label { font-size:0.6rem; letter-spacing:0.2em; color:#3a4555; text-transform:uppercase; }
        .icecast-row { display:flex; gap:8px; }
        .icecast-row input { flex:1; background:var(--bg); border:1px solid var(--border); color:#cdd5e0;
                             font-family:'Share Tech Mono',monospace; font-size:0.8rem;
                             padding:8px 10px; border-radius:3px; outline:none; }
        .icecast-row input:focus { border-color:var(--teal); }
        .btn-ice { border-color:#ff8a65; color:#ff8a65; font-size:0.7rem; padding:8px 12px; }
        .btn-ice.active { background:rgba(255,138,101,0.1); box-shadow:0 0 10px rgba(255,138,101,0.15); }
        .stream-lock { display:flex; align-items:center; gap:8px; }
        .stream-lock input { flex:1; background:var(--bg); border:1px solid var(--border); color:#cdd5e0;
                             font-family:'Share Tech Mono',monospace; font-size:0.8rem;
                             padding:7px 10px; border-radius:3px; outline:none; }
        .btn-unlock { border-color:var(--teal); color:var(--teal); font-size:0.65rem; padding:7px 10px; }
        .stream-unlocked-indicator { font-size:0.55rem; letter-spacing:0.1em; color:#00e676; }
        .stream-fields { display:none; flex-direction:column; gap:8px; }
        .stream-fields.visible { display:flex; }
        .stream-status { display:flex; align-items:center; gap:6px; font-size:0.6rem; letter-spacing:0.08em;
                         color:#3a4555; font-family:'Share Tech Mono',monospace; margin-top:2px; min-height:16px; }
        .stream-status-dot { width:6px; height:6px; border-radius:50%; background:#3a4555; flex-shrink:0; transition:background 0.3s,box-shadow 0.3s; }
        .stream-status.connecting .stream-status-dot { background:#ffea00; box-shadow:0 0 6px rgba(255,234,0,0.6); animation:blink 0.8s ease-in-out infinite; }
        .stream-status.connecting { color:#ffea00; }
        .stream-status.active .stream-status-dot { background:#00e676; box-shadow:0 0 6px rgba(0,230,118,0.6); }
        .stream-status.active { color:#00e676; }
        .stream-status.error .stream-status-dot { background:#ff1744; }
        .stream-status.error { color:#ff8a65; }

        /* Usuários online */
        .users-count { display:flex; align-items:center; gap:4px; font-family:'Orbitron',sans-serif;
                       font-size:0.65rem; color:#3a5060; letter-spacing:0.05em; cursor:pointer; }
        .users-count.active { color:var(--teal); }

        /* Painel de usuários (tooltip expandido) */
        .users-panel { display:none; background:#0d0f12; border:1px solid #1a2030; border-radius:6px;
                       padding:10px 14px; margin-top:12px; }
        .users-panel.visible { display:block; }
        .users-panel-label { font-size:0.55rem; letter-spacing:0.2em; color:#3a4555; text-transform:uppercase; margin-bottom:8px; }
        .user-item { font-size:0.65rem; color:#7a8ea0; padding:3px 0; border-bottom:1px solid #1a2030; }
        .user-item:last-child { border-bottom:none; }
        .user-item .user-loc { color:var(--teal); }
        .user-item .user-freq { color:var(--green); }

        .info-row { display:flex; justify-content:space-between; font-size:0.7rem; color:#4a5568;
                    margin-top:16px; border-top:1px solid var(--border); padding-top:12px; }
        .info-val { color:#7a8ea0; }
        .mqtt-badge { font-size:0.5rem; letter-spacing:0.15em; color:#2a3545; text-align:center; margin-top:6px; }
    </style>
</head>
<body>
<div class="panel">
    <div class="header">
        <div class="title">FM MONITOR <span style="font-size:0.55rem;color:#2a4050">v5</span></div>
        <div style="display:flex;align-items:center;gap:10px;">
            <div class="users-count" id="usersCount" onclick="toggleUsersPanel()">
                <svg width="11" height="11" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="7" cy="5" r="3"/><path d="M2 13c0-3 2-5 5-5s5 2 5 5"/></svg>
                <span id="usersNum">—</span>
            </div>
            <div class="status-dot" id="statusDot"></div>
        </div>
    </div>

    <!-- Painel expandido de usuários -->
    <div class="users-panel" id="usersPanel">
        <div class="users-panel-label">USUÁRIOS ONLINE</div>
        <div id="usersList"><span style="font-size:0.65rem;color:#3a4555">Nenhum usuário</span></div>
    </div>

    <div class="freq-row">
        <div style="display:flex;align-items:center;gap:10px;">
            <span class="stereo-badge" id="stereoBadge">STEREO</span>
            <div class="signal-bars">
                <div class="bar" id="bar1"></div><div class="bar" id="bar2"></div>
                <div class="bar" id="bar3"></div><div class="bar" id="bar4"></div>
                <div class="bar" id="bar5"></div>
            </div>
        </div>
        <div class="freq-display"><span id="freqDisplay">---.---</span><span class="freq-unit">MHz</span></div>
    </div>

    <div class="controls">
        <input type="number" id="freqInput" step="0.025" min="76.1" max="108" placeholder="MHz">
        <button class="btn btn-arrow" onclick="stepFreq(-0.1)"><svg viewBox="0 0 14 14"><polyline points="2,10 7,4 12,10"/></svg></button>
        <button class="btn btn-arrow" onclick="stepFreq(0.1)"><svg viewBox="0 0 14 14"><polyline points="2,4 7,10 12,4"/></svg></button>
        <button class="btn" onclick="tune()">TUNE</button>
    </div>

    <div class="audio-row">
        <button id="playBtn" class="btn btn-listen" onclick="openIcecast()">&#9654; PLAY</button>
    </div>

    <div class="vu-wrap">
        <div class="vu-label">VU METER</div>
        <canvas id="vuCanvas" height="130"></canvas>
        <div class="vu-db">LEVEL <span id="dbVal">-&#8734; dB</span></div>
    </div>

    <div class="rds-wrap">
        <div class="rds-row">
            <span class="rds-tag">PS</span><span class="rds-val" id="rdsPs">---</span>
            <span class="rds-tag" style="margin-left:auto">PI</span><span class="rds-val" id="rdsPi">---</span>
        </div>
        <div class="rds-row">
            <span class="rds-tag">RT</span><span class="rds-val rds-rt" id="rdsRt">---</span>
        </div>
        <div class="rds-row">
            <span class="rds-tag">PTY</span><span class="rds-val" id="rdsPty">---</span>
            <span class="rds-tag" style="margin-left:auto">TP</span><span class="rds-val" id="rdsTp">---</span>
        </div>
    </div>

    <div class="mem-wrap">
        <div class="mem-label">MEMÓRIAS</div>
        <div class="mem-grid">
            <div class="mem-btn" id="mem0"><span class="mem-freq empty" id="memFreq0">---</span><div class="mem-progress" id="memProg0"></div></div>
            <div class="mem-btn" id="mem1"><span class="mem-freq empty" id="memFreq1">---</span><div class="mem-progress" id="memProg1"></div></div>
            <div class="mem-btn" id="mem2"><span class="mem-freq empty" id="memFreq2">---</span><div class="mem-progress" id="memProg2"></div></div>
            <div class="mem-btn" id="mem3"><span class="mem-freq empty" id="memFreq3">---</span><div class="mem-progress" id="memProg3"></div></div>
        </div>
        <div class="mem-hint">Toque para sintonizar &nbsp;·&nbsp; Segure 3s para salvar</div>
    </div>

    <!-- Stream com senha (do Doc 3) -->
    <div class="icecast-wrap">
        <div class="icecast-label">STREAM</div>
        <div id="streamLockRow" class="stream-lock">
            <input type="password" id="streamPasswordInput" placeholder="Senha para editar...">
            <button class="btn btn-unlock" onclick="unlockStream()">UNLOCK</button>
        </div>
        <div class="stream-fields" id="streamFields">
            <div class="stream-unlocked-indicator">&#128275; DESBLOQUEADO</div>
            <div class="icecast-row">
                <input type="text" id="icecastUrl" placeholder="icecast://source:pass@host:8000/stream">
                <button class="btn btn-ice" id="icecastBtn" onclick="toggleIcecast()">STREAM</button>
            </div>
        </div>
        <div class="stream-status" id="streamStatus">
            <div class="stream-status-dot"></div>
            <span id="streamStatusText">inativo</span>
        </div>
    </div>

    <div class="info-row">
        <span>SR <span class="info-val">44100 Hz</span></span>
        <span>MODE <span class="info-val">WFM</span></span>
        <span>GAIN <span class="info-val">${CONFIG.gain} dB</span></span>
    </div>
    <div class="mqtt-badge" id="mqttBadge">MQTT: conectando...</div>
</div>

<!-- Overlay de sintonização -->
<div class="tuning-overlay" id="tuningOverlay">
    <div class="tuning-spinner"></div>
    <div class="tuning-freq" id="tuningFreqLabel">---</div>
    <div class="tuning-text">SINTONIZANDO...</div>
    <div class="tuning-bar"><div class="tuning-bar-fill" id="tuningBarFill"></div></div>
</div>

<script>
// ── Config injetada ──
const MQTT_WS_URL = '${CONFIG.mqttWsUrl}';
const MQTT_USER   = '${CONFIG.mqttUser}';
const MQTT_PASS   = '${CONFIG.mqttPassword}';
const TOPIC       = '${CONFIG.topic}';
const STREAM_PASS = '${CONFIG.streamPassword}';
const ICECAST_URL = '${CONFIG.icecastListenUrl}';

const T = {
    status:       TOPIC + '/status',
    rds:          TOPIC + '/rds',
    rdsClear:     TOPIC + '/rds/clear',
    level:        TOPIC + '/level',
    stereo:       TOPIC + '/stereo',
    memories:     TOPIC + '/memories',
    streamStatus: TOPIC + '/stream/status',
    streamUrl:    TOPIC + '/stream/url',
    users:        TOPIC + '/users',
    userList:     TOPIC + '/users/list',
    cmdTune:      TOPIC + '/cmd/tune',
    cmdMemory:    TOPIC + '/cmd/memory',
    cmdIcecast:   TOPIC + '/cmd/icecast',
    cmdGetStream: TOPIC + '/cmd/get_stream',
    presence:     TOPIC + '/presence/',
};

let mc, icecastActive = false;
let currentLevel  = 0;
let sharedMems    = [null, null, null, null];
let activeMem     = -1;
let autoMuted     = false;
let lowQualityCount = 0;
const AUTO_MUTE_THRESHOLD = 15;
let clientId = 'fm-client-' + Math.random().toString(16).slice(2, 8);

const els = {
    freq:      document.getElementById('freqDisplay'),
    input:     document.getElementById('freqInput'),
    statusDot: document.getElementById('statusDot'),
    dbVal:     document.getElementById('dbVal'),
    rdsPs:     document.getElementById('rdsPs'),
    rdsPi:     document.getElementById('rdsPi'),
    rdsRt:     document.getElementById('rdsRt'),
    rdsPty:    document.getElementById('rdsPty'),
    rdsTp:     document.getElementById('rdsTp'),
};

// ── VU Meter ──
const canvas = document.getElementById('vuCanvas');
const ctx    = canvas.getContext('2d');
const MIN_DB = -40, MAX_DB = 3;
const MIN_ANGLE = -65 * Math.PI / 180;
const MAX_ANGLE =  65 * Math.PI / 180;

function resizeCanvas() {
    const dpr = window.devicePixelRatio || 1;
    canvas.width  = canvas.offsetWidth * dpr;
    canvas.height = 130 * dpr;
    ctx.scale(dpr, dpr);
}
resizeCanvas();
window.addEventListener('resize', resizeCanvas);

function dbToAngle(db) {
    const t = Math.max(0, Math.min(1, (db - MIN_DB) / (MAX_DB - MIN_DB)));
    return MIN_ANGLE + t * (MAX_ANGLE - MIN_ANGLE);
}
let needleAngle = MIN_ANGLE, targetAngle = MIN_ANGLE;
let peakAngle   = MIN_ANGLE, peakHold    = 0;

function drawVU() {
    const w = canvas.offsetWidth, h = 130;
    ctx.clearRect(0, 0, w, h);
    const cx = w / 2, cy = h * 1.05, R = h * 0.92;
    [{from:MIN_DB,to:-6,color:'#00e676'},{from:-6,to:0,color:'#ffea00'},{from:0,to:MAX_DB,color:'#ff1744'}].forEach(z=>{
        ctx.beginPath(); ctx.arc(cx,cy,R,dbToAngle(z.from)-Math.PI/2,dbToAngle(z.to)-Math.PI/2);
        ctx.strokeStyle=z.color; ctx.lineWidth=6; ctx.globalAlpha=0.18; ctx.stroke(); ctx.globalAlpha=1;
    });
    [{db:-40,label:'-40',major:true},{db:-30,label:'-30',major:true},{db:-20,label:'-20',major:true},
     {db:-10,label:'-10',major:true},{db:-7,label:'-7',major:false},{db:-5,label:'-5',major:false},
     {db:-3,label:'-3',major:false},{db:0,label:'0',major:true},{db:3,label:'+3',major:true}].forEach(t=>{
        const angle=dbToAngle(t.db)-Math.PI/2, tl=t.major?10:6;
        ctx.beginPath(); ctx.moveTo(cx+(R-2)*Math.cos(angle),cy+(R-2)*Math.sin(angle));
        ctx.lineTo(cx+(R-2-tl)*Math.cos(angle),cy+(R-2-tl)*Math.sin(angle));
        ctx.strokeStyle=t.db>=0?'#ff1744':t.db>=-6?'#ffea00':'#3a5060';
        ctx.lineWidth=t.major?1.5:1; ctx.stroke();
        if(t.major){
            ctx.fillStyle=t.db>=0?'#ff1744':'#3a5568'; ctx.font='9px Share Tech Mono,monospace';
            ctx.textAlign='center'; ctx.textBaseline='middle';
            ctx.fillText(t.label,cx+(R-20)*Math.cos(angle),cy+(R-20)*Math.sin(angle));
        }
    });
    if(peakAngle>MIN_ANGLE+0.05){
        const pa=peakAngle-Math.PI/2; ctx.beginPath();
        ctx.moveTo(cx+(R-8)*Math.cos(pa),cy+(R-8)*Math.sin(pa));
        ctx.lineTo(cx+(R-2)*Math.cos(pa),cy+(R-2)*Math.sin(pa));
        ctx.strokeStyle='#ff8a65'; ctx.lineWidth=2; ctx.stroke();
    }
    const na=needleAngle-Math.PI/2, nLen=R-14;
    ctx.beginPath(); ctx.moveTo(cx,cy); ctx.lineTo(cx+nLen*Math.cos(na+0.015),cy+nLen*Math.sin(na+0.015));
    ctx.strokeStyle='rgba(0,0,0,0.5)'; ctx.lineWidth=3; ctx.stroke();
    ctx.beginPath(); ctx.moveTo(cx,cy); ctx.lineTo(cx+nLen*Math.cos(na),cy+nLen*Math.sin(na));
    ctx.strokeStyle=needleAngle>dbToAngle(0)?'#ff1744':needleAngle>dbToAngle(-6)?'#ffea00':'#e0e8f0';
    ctx.lineWidth=1.8; ctx.lineCap='round'; ctx.stroke();
    ctx.beginPath(); ctx.arc(cx,cy,5,0,Math.PI*2); ctx.fillStyle='#2a3545'; ctx.fill();
    ctx.strokeStyle='#4a5568'; ctx.lineWidth=1; ctx.stroke();
    ctx.beginPath(); ctx.arc(cx,cy,2,0,Math.PI*2); ctx.fillStyle='#7a8ea0'; ctx.fill();
}

function updateNeedle() {
    const db = currentLevel > 0.001 ? 20 * Math.log10(currentLevel) : MIN_DB;
    targetAngle = dbToAngle(Math.max(MIN_DB, Math.min(MAX_DB, db)));
    needleAngle += (targetAngle - needleAngle) * (targetAngle > needleAngle ? 0.35 : 0.06);
    if(needleAngle>peakAngle){peakAngle=needleAngle;peakHold=120;}
    else{peakHold--;if(peakHold<=0)peakAngle+=(MIN_ANGLE-peakAngle)*0.03;}
    drawVU();
    els.dbVal.textContent = currentLevel > 0.001 ? db.toFixed(1) + ' dB' : '-\u221e dB';
    requestAnimationFrame(updateNeedle);
}
updateNeedle();

// ── Auto-mute por chiado ──
function triggerAutoMute() {
    autoMuted = true;
    lowQualityCount = 0;
    if (audioEl && !audioEl.paused) {
        audioEl.pause();
        isMuted = true;
        const btn = document.getElementById('playBtn');
        btn.innerHTML = '&#9654; PLAY';
        btn.classList.remove('active');
        btn.classList.add('automuted');
        showAlert('CHIADO DETECTADO — STREAM PAUSADO', '#ff8a65');
    }
}

// ── MQTT ──
function connectMqtt() {
    const badge = document.getElementById('mqttBadge');
    badge.textContent = 'MQTT: conectando...';

    mc = mqtt.connect(MQTT_WS_URL, {
        username:        MQTT_USER,
        password:        MQTT_PASS,
        clientId:        clientId,
        clean:           true,
        reconnectPeriod: 3000,
        connectTimeout:  10000,
        will: {
            topic:   T.presence + clientId,
            payload: '0',
            qos:     1,
            retain:  false,
        },
    });

    mc.on('connect', () => {
        els.statusDot.classList.add('on');
        badge.textContent = 'MQTT: online';
        badge.style.color = '#00e676';

        const subs = [T.status, T.rds, T.rdsClear, T.level, T.stereo,
                      T.memories, T.streamStatus, T.streamUrl,
                      T.users, T.userList];
        mc.subscribe(subs, { qos: 1 }, (err) => {
            if (!err) {
                mc.publish(T.cmdGetStream, JSON.stringify({}), { qos: 1 });

                // Heartbeat com IP (para geo no servidor)
                const presenceTopic = T.presence + clientId;
                const heartbeat = () => {
                    if (mc.connected) {
                        // Envia IP via payload para o servidor resolver geo
                        mc.publish(presenceTopic, JSON.stringify({ v: 1 }), { qos: 0 });
                    }
                };
                heartbeat();
                setInterval(heartbeat, 30000);
            }
        });
    });

    mc.on('error',     (e) => { badge.textContent = 'MQTT: erro'; badge.style.color = '#ff1744'; });
    mc.on('close',     ()  => { els.statusDot.classList.remove('on'); badge.textContent = 'MQTT: reconectando...'; badge.style.color = '#ffea00'; });
    mc.on('reconnect', ()  => console.log('[MQTT] Reconectando...'));

    mc.on('message', (topic, buf) => {
        let msg;
        try { msg = JSON.parse(buf.toString()); } catch(e) { msg = {}; }

        if (topic === T.status) {
            els.freq.innerText = (msg.freq / 1000000).toFixed(3);
            els.input.value    = (msg.freq / 1000000).toFixed(3);
        }
        if (topic === T.level) {
            currentLevel = msg.audio;
            if (msg.quality !== undefined) {
                updateSignalBars(msg.quality);
                // Auto-mute (do Doc 4)
                if (audioEl && !audioEl.paused && !autoMuted) {
                    if (msg.quality <= 1) {
                        lowQualityCount++;
                        if (lowQualityCount >= AUTO_MUTE_THRESHOLD) triggerAutoMute();
                    } else {
                        lowQualityCount = 0;
                    }
                }
            }
        }
        if (topic === T.stereo) {
            document.getElementById('stereoBadge').classList.toggle('on', !!msg.stereo);
        }
        if (topic === T.rds) {
            if (msg.ps)        els.rdsPs.textContent  = msg.ps;
            if (msg.pi)        els.rdsPi.textContent  = msg.pi;
            if (msg.radiotext) els.rdsRt.textContent  = msg.radiotext;
            if (msg.prog_type) els.rdsPty.textContent = msg.prog_type;
            if (msg.tp !== undefined) els.rdsTp.textContent = msg.tp ? 'YES' : 'NO';
        }
        if (topic === T.rdsClear) {
            els.rdsPs.textContent = els.rdsPi.textContent = els.rdsRt.textContent =
            els.rdsPty.textContent = els.rdsTp.textContent = '---';
            document.getElementById('stereoBadge').classList.remove('on');
        }
        if (topic === T.memories) {
            updateMemories(Array.isArray(msg) ? msg : [null,null,null,null]);
        }
        if (topic === T.streamStatus) {
            const el  = document.getElementById('streamStatus');
            const txt = document.getElementById('streamStatusText');
            el.className = 'stream-status ' + (msg.status || '');
            const labels = { off:'inativo', connecting:'conectando...', active: msg.msg || 'ativo', error: msg.msg || 'erro' };
            txt.textContent = labels[msg.status] || msg.status;
            const btn = document.getElementById('icecastBtn');
            if (btn) {
                btn.classList.toggle('active', msg.status === 'active' || msg.status === 'connecting');
                if (msg.status === 'off' || msg.status === 'error') { btn.textContent = 'STREAM'; icecastActive = false; }
                else if (msg.status === 'active') { btn.textContent = 'STOP'; icecastActive = true; }
            }
        }
        if (topic === T.streamUrl) {
            const input = document.getElementById('icecastUrl');
            if (input && msg.url) input.value = msg.url;
        }
        if (topic === T.users) {
            const n = msg.count;
            document.getElementById('usersNum').textContent = n;
            document.getElementById('usersCount').classList.toggle('active', n > 0);
        }
        if (topic === T.userList) {
            updateUsersList(Array.isArray(msg) ? msg : []);
        }
    });
}

function pub(topic, payload) {
    if (!mc || !mc.connected) return;
    mc.publish(topic, JSON.stringify(payload), { qos: 1 });
}

// ── Usuários (painel expandido) ──
let usersPanelOpen = false;
window.toggleUsersPanel = () => {
    usersPanelOpen = !usersPanelOpen;
    document.getElementById('usersPanel').classList.toggle('visible', usersPanelOpen);
};

function updateUsersList(list) {
    const el = document.getElementById('usersList');
    if (!list.length) { el.innerHTML = '<span style="font-size:0.65rem;color:#3a4555">Nenhum usuário</span>'; return; }
    el.innerHTML = list.map(u => {
        const loc  = [u.city, u.region, u.country].filter(Boolean).join(', ') || '?';
        const freq = u.freq ? ' · <span class="user-freq">' + parseFloat(u.freq).toFixed(2) + ' MHz</span>' : '';
        return '<div class="user-item"><span class="user-loc">' + loc + '</span>' + freq + '</div>';
    }).join('');
}

// ── Controles ──
function updateSignalBars(quality) {
    for (let i = 1; i <= 5; i++) {
        const bar = document.getElementById('bar' + i);
        bar.classList.remove('lit-good','lit-warn','lit-bad');
        if (i <= quality) {
            if (quality <= 1)      bar.classList.add('lit-bad');
            else if (quality <= 3) bar.classList.add('lit-warn');
            else                   bar.classList.add('lit-good');
        }
    }
}

window.stepFreq = (delta) => {
    const current = parseFloat(els.input.value) || parseFloat(els.freq.innerText) || 101.7;
    let next = Math.round((current + delta) * 1000) / 1000;
    if (next < 76.1) next = 76.1;
    if (next > 108)  next = 108;
    els.input.value = next.toFixed(3);
};

window.tune = () => {
    let freq = parseFloat(els.input.value);
    if (!freq) return;
    freq = Math.max(76.1, Math.min(108, freq));
    els.input.value = freq.toFixed(3);
    // Inclui clientId para o servidor poder atualizar o rastreamento
    pub(T.cmdTune, { freq, _clientId: clientId });
    autoMuted = false; lowQualityCount = 0; isMuted = false;
    tuneWait(freq);
};

function tuneWait(freq) {
    if (audioEl && !audioEl.paused) audioEl.pause();
    const btn = document.getElementById('playBtn');
    btn.innerHTML = '&#9654; PLAY';
    btn.classList.remove('active','automuted');
    isMuted = false;

    const overlay = document.getElementById('tuningOverlay');
    const label   = document.getElementById('tuningFreqLabel');
    const bar     = document.getElementById('tuningBarFill');
    label.textContent = parseFloat(freq).toFixed(2) + ' MHz';
    bar.style.animation = 'none';
    bar.offsetHeight;
    bar.style.animation = 'tuningProgress 4s linear forwards';
    overlay.classList.add('visible');

    clearTimeout(tuneWait._timer);
    tuneWait._timer = setTimeout(() => {
        overlay.classList.remove('visible');
        // Reconecta com buffer fresco após sintonizar
        freshAudio();
        const doPlay = () => {
            audioEl.play().then(() => {
                btn.innerHTML = '&#9646;&#9646; MUTE';
                btn.classList.add('active');
            }).catch(() => {});
        };
        audioEl.addEventListener('canplay', doPlay, { once: true });
        setTimeout(() => { if (audioEl && audioEl.paused) doPlay(); }, 3000);
    }, 4000);
}

let audioEl = null;
let isMuted = false;

function freshAudio() {
    // Destroi elemento antigo para descartar buffer acumulado do Icecast
    if (audioEl) {
        audioEl.pause();
        audioEl.removeAttribute('src');
        audioEl.load();
        audioEl = null;
    }
    audioEl = new Audio(ICECAST_URL);
    audioEl.preload = 'auto';
    return audioEl;
}

window.openIcecast = () => {
    const btn = document.getElementById('playBtn');
    autoMuted = false;
    lowQualityCount = 0;

    if (!audioEl || audioEl.paused) {
        // Estado: parado ou nunca iniciado → PLAY com buffer fresco
        freshAudio();
        isMuted = false;
        const doPlay = () => {
            audioEl.play().then(() => {
                btn.innerHTML = '&#9646;&#9646; MUTE';
                btn.classList.add('active');
                btn.classList.remove('automuted');
            }).catch(e => showAlert('ERRO: ' + e.message, '#ff8a65'));
        };
        audioEl.addEventListener('canplay', doPlay, { once: true });
        setTimeout(() => { if (audioEl && audioEl.paused) doPlay(); }, 3000);
    } else if (!isMuted) {
        // Estado: tocando → MUTE (pausa sem destruir)
        audioEl.pause();
        isMuted = true;
        btn.innerHTML = '&#9654; PLAY';
        btn.classList.remove('active', 'automuted');
    } else {
        // Estado: mutado → PLAY com buffer fresco (sem buffer antigo)
        freshAudio();
        isMuted = false;
        const doPlay = () => {
            audioEl.play().then(() => {
                btn.innerHTML = '&#9646;&#9646; MUTE';
                btn.classList.add('active');
                btn.classList.remove('automuted');
            }).catch(e => showAlert('ERRO: ' + e.message, '#ff8a65'));
        };
        audioEl.addEventListener('canplay', doPlay, { once: true });
        setTimeout(() => { if (audioEl && audioEl.paused) doPlay(); }, 3000);
    }
};

window.unlockStream = () => {
    const pwd = document.getElementById('streamPasswordInput').value;
    if (pwd === STREAM_PASS) {
        document.getElementById('streamLockRow').style.display = 'none';
        document.getElementById('streamFields').classList.add('visible');
        pub(T.cmdGetStream, '');
    } else {
        showAlert('SENHA INCORRETA', '#ff1744');
        document.getElementById('streamPasswordInput').value = '';
    }
};

window.toggleIcecast = () => {
    const url = document.getElementById('icecastUrl').value.trim();
    if (!icecastActive && !url) return;
    icecastActive = !icecastActive;
    pub(T.cmdIcecast, { url: icecastActive ? url : null });
};

function showAlert(message, color) {
    color = color || '#ff8a65';
    let alert = document.getElementById('freqAlert');
    if (!alert) { alert = document.createElement('div'); alert.id = 'freqAlert'; document.body.appendChild(alert); }
    alert.style.cssText = 'position:fixed;top:18px;left:50%;transform:translateX(-50%);background:#1a0a00;border:1px solid '+color+';color:'+color+';font-family:Orbitron,sans-serif;font-size:0.7rem;letter-spacing:0.15em;padding:10px 22px;border-radius:4px;box-shadow:0 0 18px rgba(255,138,101,0.25);z-index:9999;pointer-events:none;transition:opacity 0.4s;';
    alert.textContent = message; alert.style.opacity = '1';
    clearTimeout(alert._t); alert._t = setTimeout(() => { alert.style.opacity = '0'; }, 2500);
}

// ── Memórias ──
function updateMemories(mems) {
    sharedMems = mems;
    mems.forEach((freq, i) => {
        const el  = document.getElementById('memFreq' + i);
        const btn = document.getElementById('mem' + i);
        if (freq) { el.textContent = parseFloat(freq).toFixed(2); el.classList.remove('empty'); }
        else      { el.textContent = '---'; el.classList.add('empty'); }
        btn.classList.toggle('active', i === activeMem);
    });
}

const HOLD_TIME  = 3000;
const holdTimers = [null, null, null, null];

for (let i = 0; i < 4; i++) {
    const btn  = document.getElementById('mem' + i);
    const prog = document.getElementById('memProg' + i);

    const startHold = (e) => {
        e.preventDefault();
        prog.innerHTML = '<div class="mem-progress-bar" id="memBar' + i + '"></div>';
        const bar = document.getElementById('memBar' + i);
        bar.style.transition = 'width ' + HOLD_TIME + 'ms linear';
        btn.classList.add('saving');
        setTimeout(() => { bar.style.width = '100%'; }, 10);
        holdTimers[i] = setTimeout(() => {
            const freq = parseFloat(els.input.value) || parseFloat(els.freq.innerText);
            if (freq) {
                pub(T.cmdMemory, { index: i, freq, _clientId: clientId });
                showAlert('SALVO EM M' + (i + 1) + ': ' + freq.toFixed(2) + ' MHz', '#00e676');
            }
            cancelHold(i);
        }, HOLD_TIME);
    };

    const cancelHold = (idx) => {
        clearTimeout(holdTimers[idx]); holdTimers[idx] = null;
        document.getElementById('mem' + idx).classList.remove('saving');
        document.getElementById('memProg' + idx).innerHTML = '';
    };

    const endHold = (e) => {
        if (holdTimers[i] !== null) {
            cancelHold(i);
            if (sharedMems[i]) {
                activeMem = i;
                els.input.value = parseFloat(sharedMems[i]).toFixed(3);
                updateMemories(sharedMems);
                pub(T.cmdTune, { freq: sharedMems[i], _clientId: clientId });
                tuneWait(sharedMems[i]);
            }
        }
    };

    btn.addEventListener('mousedown',   startHold);
    btn.addEventListener('touchstart',  startHold, { passive: false });
    btn.addEventListener('mouseup',     endHold);
    btn.addEventListener('mouseleave',  () => { if (holdTimers[i] !== null) cancelHold(i); });
    btn.addEventListener('touchend',    endHold);
    btn.addEventListener('touchcancel', () => cancelHold(i));
}

connectMqtt();
</script>
</body>
</html>`;
}

// ─── STARTUP ──────────────────────────────────────────────────
server.listen(CONFIG.webPort, () => {
    console.log(`[HTTP] Servidor em http://localhost:${CONFIG.webPort}`);
    startRadio(CONFIG.frequency);

    if (savedStreamUrl) {
        console.log('[Icecast] Auto-iniciando stream para:', savedStreamUrl);
        setTimeout(() => startIcecast(savedStreamUrl), 2000);
    }
});

// ─── CLEANUP ──────────────────────────────────────────────────
function cleanup() {
    console.log('[Exit] Encerrando...');
    killProcess(rtlProcess);
    killProcess(rdsProcess);
    killProcess(pilotProcess);
    killProcess(icecastProcess);
    mqttClient.end();
    spawn('bash', ['-c', 'pkill -9 rtl_fm; pkill -9 redsea; pkill -9 sox; pkill -9 ffmpeg'], { detached: true });
    setTimeout(() => process.exit(0), 300);
}
process.on('SIGINT',  cleanup);
process.on('SIGTERM', cleanup);

process.on('uncaughtException', (err) => {
    if (err.code === 'EPIPE' || err.code === 'ERR_STREAM_DESTROYED') {
        console.log('[App] EPIPE ignorado');
    } else {
        console.error('[App] Erro não tratado:', err);
    }
});
