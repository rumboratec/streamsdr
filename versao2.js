const http = require('http');
const https = require('https');
const WebSocket = require('ws');
const { spawn } = require('child_process');

const CONFIG = {
    webPort: 3000,
    frequency: 101700000,
    gain: '19.2',
    ppm: 0,
    sampleRate: 44100,
};

let rtlProcess = null;
let rdsProcess = null;
let pilotProcess = null;
let icecastProcess = null;
let currentFreq = CONFIG.frequency;
let rdsBuffer = '';
let squelchLevel = 0;

// Qualidade de sinal
let lastRdsTime = 0;
let levelHistory = [];
const LEVEL_HISTORY_SIZE = 20;

function computeSignalQuality(level) {
    // Adiciona level ao histórico
    levelHistory.push(level);
    if (levelHistory.length > LEVEL_HISTORY_SIZE) levelHistory.shift();

    const rdsRecent = (Date.now() - lastRdsTime) < 5000; // RDS nos últimos 5s

    if (rdsRecent) return 5; // RDS presente = sinal excelente

    if (levelHistory.length < 5) return 0;

    // Calcula variância do level (chiado = variância alta, sinal = variância baixa)
    const mean = levelHistory.reduce((a, b) => a + b, 0) / levelHistory.length;
    const variance = levelHistory.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / levelHistory.length;
    const cv = mean > 0.001 ? variance / (mean * mean) : 999; // coeficiente de variação

    if (mean < 0.005) return 0;        // sem sinal
    if (cv > 0.5)     return 1;        // chiado puro (muito instável)
    if (cv > 0.3)     return 2;        // sinal fraco
    if (cv > 0.15)    return 3;        // sinal médio
    if (cv > 0.05)    return 4;        // sinal bom
    return 5;                           // sinal excelente
}

function startIcecast(url) {
    if (icecastProcess) { try { process.kill(-icecastProcess.pid, 'SIGKILL'); } catch(e) {} icecastProcess = null; }
    if (!url) return;
    const cmd = `ffmpeg -re -f s16le -ar 44100 -ac 1 -i pipe:0 -c:a libmp3lame -b:a 128k -f mp3 "${url}"`;
    console.log('[Icecast] Command:', cmd);
    icecastProcess = spawn('bash', ['-c', cmd], { detached: true });
    icecastProcess.stderr.on('data', (d) => console.log('[ffmpeg]', d.toString().trim().split('\n')[0]));
    icecastProcess.on('close', (code) => console.log('[Icecast] exited', code));
}

function broadcastRDS(data) {
    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'rds', data }));
        }
    });
}

class StereoDetector {
    constructor() {
        this.sampleRate = 171000;  // agora analisa a 171kHz
        this.pilotFreq  = 19000;
        this.w     = 2 * Math.PI * this.pilotFreq / this.sampleRate;
        this.coeff = 2 * Math.cos(this.w);
        this.s1 = 0; this.s2 = 0;
        this.count = 0;
        this.blockSize = 1710;     // ~10ms a 171kHz
        this.totalPower = 0;
        this.isStereo = false;
        this.threshold    = 0.00035;  // limiar para entrar em stereo
        this.thresholdOff = 0.00025;  // limiar para sair de stereo (histerese)
        this.smoothRatio  = 0;
        this.alpha        = 0.04;     // suavização mais lenta para estabilizar
    }

    process(chunk) {
        const int16 = new Int16Array(chunk.buffer, chunk.byteOffset, chunk.length / 2);
        for (let i = 0; i < int16.length; i++) {
            const sample = int16[i] / 32768.0;
            const s0 = sample + this.coeff * this.s1 - this.s2;
            this.s2 = this.s1;
            this.s1 = s0;
            this.totalPower += sample * sample;
            this.count++;

            if (this.count >= this.blockSize) {
                const power = (this.s1 * this.s1 + this.s2 * this.s2 - this.coeff * this.s1 * this.s2) / (this.blockSize * this.blockSize);
                const total = this.totalPower / this.blockSize;
                const ratio = total > 0.00001 ? power / total : 0;

                this.smoothRatio += (ratio - this.smoothRatio) * this.alpha;

                // Só usa o detector de piloto se não houver RDS recente
                const rdsRecent = (Date.now() - lastRdsTime) < 5000;
                if (!rdsRecent) {
                    let stereo = this.isStereo;
                    if (!this.isStereo && this.smoothRatio > this.threshold)    stereo = true;
                    if (this.isStereo  && this.smoothRatio < this.thresholdOff) stereo = false;

                    if (stereo !== this.isStereo) {
                        this.isStereo = stereo;
                        console.log('[Pilot] stereo:', stereo, '| ratio:', this.smoothRatio.toFixed(6));
                        wss.clients.forEach((client) => {
                            if (client.readyState === WebSocket.OPEN) {
                                client.send(JSON.stringify({ type: 'stereo', stereo }));
                            }
                        });
                    }
                } else {
                    this.isStereo = false; // reseta quando RDS assume
                }

                if (Math.random() < 0.02) console.log('[Pilot ratio]', this.smoothRatio.toFixed(6));
                this.s1 = 0; this.s2 = 0;
                this.count = 0;
                this.totalPower = 0;
            }
        }
    }
}

const stereoDetector = new StereoDetector();

function killProcess(proc) {
    if (!proc) return;
    try { process.kill(-proc.pid, 'SIGKILL'); } catch(e) {
        try { proc.kill('SIGKILL'); } catch(e2) {}
    }
}

function startRadio(freq) {
    killProcess(rtlProcess); rtlProcess = null;
    killProcess(rdsProcess); rdsProcess = null;
    killProcess(pilotProcess); pilotProcess = null;
    setTimeout(() => _startRadio(freq), 800);
}

function _startRadio(freq) {
    currentFreq = freq;
    console.log(`[Radio] Tuning to ${(freq/1000000).toFixed(2)} MHz`);

    const fifo       = '/tmp/rds_fifo';
    const pilotFifo  = '/tmp/pilot_fifo';
    const mkfifo = spawn('bash', ['-c', `[ -p ${fifo} ] || mkfifo ${fifo}; [ -p ${pilotFifo} ] || mkfifo ${pilotFifo}`]);
    mkfifo.on('close', () => {
        // pipeline: rtl_fm → tee rds_fifo → tee pilot_fifo → sox → stdout
        const cmd = `rtl_fm -f ${freq} -M wfm -s 171k -E deemp -g ${CONFIG.gain} -A std | tee ${fifo} | tee ${pilotFifo} | sox -t raw -r 171000 -e signed -b 16 -c 1 - -t raw -r 44100 -`;
        console.log('[Radio] Command:', cmd);

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
                        wss.clients.forEach((client) => {
                            if (client.readyState === WebSocket.OPEN) {
                                client.send(JSON.stringify({ type: 'stereo', stereo: rds.di.stereo }));
                            }
                        });
                    }
                    broadcastRDS(rds);
                } catch(e) {}
            });
        });
        rdsProcess.stderr.on('data', (d) => console.log(`[redsea] ${d.toString().trim()}`));
        rdsProcess.on('close', (code) => console.log(`[RDS] exited ${code}`));

        // Processo que lê o sinal a 171kHz para detectar piloto de 19kHz
        pilotProcess = spawn('bash', ['-c', `cat ${pilotFifo}`], { detached: true });
        pilotProcess.stdout.on('data', (chunk) => {
            stereoDetector.process(chunk);
        });
        pilotProcess.on('close', (code) => console.log(`[Pilot] exited ${code}`));

        setTimeout(() => {
            rtlProcess = spawn('bash', ['-c', cmd], { detached: true });
            rtlProcess.stdout.on('data', (chunk) => {
                const int16 = new Int16Array(chunk.buffer, chunk.byteOffset, chunk.length / 2);
                let sum = 0;
                for (let i = 0; i < int16.length; i++) sum += Math.abs(int16[i]);
                const level = sum / int16.length / 32768.0;

                const quality = computeSignalQuality(level);

                wss.clients.forEach((client) => {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({ type: 'level', audio: level, quality }));
                    }
                });

                // Squelch: só transmite se nível acima do limiar
                if (squelchLevel === 0 || level >= squelchLevel) {
                    broadcastAudio(chunk);
                } else {
                    // Envia silêncio para manter o buffer do cliente ativo
                    const silence = Buffer.alloc(chunk.length);
                    broadcastAudio(silence);
                }

                if (icecastProcess && icecastProcess.stdin && !icecastProcess.stdin.destroyed) {
                    try { icecastProcess.stdin.write(chunk); } catch(e) {}
                }
            });
            rtlProcess.stderr.on('data', (d) => console.log(`[rtl_fm] ${d.toString().trim()}`));
            rtlProcess.on('close', (code) => console.log(`[Radio] exited ${code}`));
        }, 300);
    });
}

const server = http.createServer((req, res) => {
    if (req.url === '/') {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(htmlContent);
    } else {
        res.writeHead(404);
        res.end();
    }
});

const wss = new WebSocket.Server({ server });

function broadcastAudio(data) {
    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(data);
        }
    });
}

function sendStatus(ws) {
    ws.send(JSON.stringify({ type: 'status', freq: currentFreq, sampleRate: CONFIG.sampleRate }));
}

// Memórias compartilhadas (banco simples em memória, persistido em JSON)
const fs = require('fs');
const MEMORIES_FILE = './memories.json';
let sharedMemories = [null, null, null, null];
try {
    if (fs.existsSync(MEMORIES_FILE)) {
        sharedMemories = JSON.parse(fs.readFileSync(MEMORIES_FILE, 'utf8'));
    }
} catch(e) {}

function saveMemoriesToDisk() {
    try { fs.writeFileSync(MEMORIES_FILE, JSON.stringify(sharedMemories)); } catch(e) {}
}

function broadcastMemories() {
    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'memories', memories: sharedMemories }));
        }
    });
}

function broadcastUserCount() {
    const count = wss.clients.size;
    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({ type: 'users', count }));
        }
    });
}

let ngrokUrl = null;

// ===== RASTREAMENTO DE USUÁRIOS =====
const onlineUsers = new Map();

function getGeoIp(ip) {
    return new Promise((resolve) => {
        const cleanIp = ip.replace('::ffff:', '');
        console.log('[GeoIP] consultando:', cleanIp);
        // IPs privados e locais
        if (cleanIp === '127.0.0.1' || cleanIp === '::1' ||
            cleanIp.startsWith('192.168.') || cleanIp.startsWith('10.') ||
            cleanIp.startsWith('172.16.') || cleanIp.startsWith('172.17.') ||
            cleanIp.startsWith('172.18.') || cleanIp.startsWith('172.19.') ||
            cleanIp.startsWith('172.2') || cleanIp.startsWith('172.30.') ||
            cleanIp.startsWith('172.31.')) {
            resolve({ city: 'Rede Local', region: '', country: cleanIp });
            return;
        }
        const req = http.get('http://ip-api.com/json/' + cleanIp + '?fields=city,regionName,country,status', (res) => {
            let data = '';
            res.on('data', d => data += d);
            res.on('end', () => {
                console.log('[GeoIP] resposta:', data.substring(0, 120));
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
        req.on('error', (e) => { console.log('[GeoIP] erro:', e.message); resolve({ city: '?', region: '?', country: '?' }); });
        req.setTimeout(4000, () => { req.destroy(); resolve({ city: '?', region: '?', country: '?' }); });
    });
}

function sendNtfy(title, message) {
    const body = message;
    const options = {
        hostname: 'ntfy.sh',
        path: '/radiorumbora',
        method: 'POST',
        headers: {
            'Content-Type': 'text/plain',
            'Title': title,
            'Priority': 'default',
            'Tags': 'radio'
        }
    };

    const req = https.request(options, (res) => {
        console.log('[ntfy] status:', res.statusCode);
    });
    req.on('error', (e) => console.log('[ntfy] error:', e.message));
    req.setTimeout(6000, () => req.destroy());
    req.write(body);
    req.end();
}

wss.on('connection', (ws, req) => {
    const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
    const connectedAt = new Date().toLocaleString('pt-BR', { timeZone: 'America/Fortaleza' });
    console.log('[Web] Client connected:', ip);

    // Registra usuário e busca geo
    onlineUsers.set(ws, { ip, city: '...', region: '', country: '', freq: null, action: '', connectedAt });
    getGeoIp(ip).then(geo => {
        const u = onlineUsers.get(ws);
        if (u) {
            u.city = geo.city; u.region = geo.region; u.country = geo.country;
            const loc = [geo.city, geo.region, geo.country].filter(Boolean).join(', ');
            sendNtfy('Conectou', ip + ' | ' + loc + ' | ' + connectedAt);
        }
    });

    sendStatus(ws);
    ws.send(JSON.stringify({ type: 'memories', memories: sharedMemories }));
    if (ngrokUrl) ws.send(JSON.stringify({ type: 'ngrok_url', url: ngrokUrl }));
    broadcastUserCount();

    ws.on('message', (message) => {
        try {
            const cmd = JSON.parse(message);
            if (cmd.type === 'tune') {
                const freqHz = Math.floor(parseFloat(cmd.freq) * 1000000);
                const freqMHz = parseFloat(cmd.freq);
                currentFreq = freqHz;
                // Atualiza freq do usuário
                const u = onlineUsers.get(ws);
                if (u) {
                    u.freq = freqMHz; u.action = 'sintonizou';
                    const loc = [u.city, u.region, u.country].filter(Boolean).join(', ');
                    sendNtfy('Sintonizou', ip + ' | ' + loc + ' | ' + freqMHz.toFixed(2) + ' MHz');
                }
                wss.clients.forEach(c => {
                    if (c.readyState === WebSocket.OPEN) {
                        sendStatus(c);
                        c.send(JSON.stringify({ type: 'rds_clear' }));
                        c.send(JSON.stringify({ type: 'stereo', stereo: false }));
                    }
                });
                startRadio(freqHz);
            }
            if (cmd.type === 'save_memory') {
                const idx = parseInt(cmd.index);
                const freq = parseFloat(cmd.freq);
                if (idx >= 0 && idx <= 3 && freq) {
                    sharedMemories[idx] = freq;
                    saveMemoriesToDisk();
                    broadcastMemories();
                    console.log('[Memory] Slot', idx, 'saved:', freq);
                    // Notifica salvamento
                    const u = onlineUsers.get(ws);
                    if (u) {
                        const loc = [u.city, u.region, u.country].filter(Boolean).join(', ');
                        sendNtfy('Salvou M' + (idx+1), ip + ' | ' + loc + ' | ' + freq.toFixed(2) + ' MHz');
                    }
                }
            }
            if (cmd.type === 'icecast') {
                startIcecast(cmd.url || null);
                ws.send(JSON.stringify({ type: 'icecast_status', active: !!cmd.url }));
            }
        } catch(e) { console.error(e); }
    });

    ws.on('close', () => {
        const u = onlineUsers.get(ws);
        if (u) {
            const loc = [u.city, u.region, u.country].filter(Boolean).join(', ');
            const freq = u.freq ? ' | ' + u.freq.toFixed(2) + ' MHz' : '';
            sendNtfy('Desconectou', ip + ' | ' + loc + freq);
        }
        onlineUsers.delete(ws);
        broadcastUserCount();
        console.log('[Web] Client disconnected:', ip);
    });
});

const htmlContent = `
<!DOCTYPE html>
<html lang="pt">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FM Monitor</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Orbitron:wght@400;700&display=swap');

        :root {
            --bg: #0a0c0f;
            --panel: #111418;
            --border: #1e2530;
            --green: #00e676;
            --teal: #00bcd4;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }

        body {
            background: var(--bg);
            color: #cdd5e0;
            font-family: 'Share Tech Mono', monospace;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            padding: 20px;
        }

        .panel {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 4px;
            width: 100%;
            max-width: 420px;
            padding: 24px;
            box-shadow: 0 0 40px rgba(0,188,212,0.05);
        }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            border-bottom: 1px solid var(--border);
            padding-bottom: 14px;
        }

        .title {
            font-family: 'Orbitron', sans-serif;
            font-size: 0.85rem;
            letter-spacing: 0.2em;
            color: var(--teal);
        }

        .status-dot {
            width: 8px; height: 8px;
            border-radius: 50%;
            background: #444;
            transition: background 0.3s, box-shadow 0.3s;
        }
        .status-dot.on {
            background: var(--green);
            box-shadow: 0 0 8px var(--green);
            animation: blink 1.5s ease-in-out infinite;
        }

        @keyframes blink {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }

        /* Freq + stereo badge acima do MHz */
        .freq-row {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            margin: 16px 0;
            gap: 4px;
        }

        .stereo-badge {
            font-size: 0.55rem;
            letter-spacing: 0.2em;
            color: transparent;
            background: #1a2530;
            border: 1px solid #2a3545;
            padding: 2px 8px;
            border-radius: 3px;
            font-family: 'Orbitron', sans-serif;
            transition: all 0.4s;
            user-select: none;
        }
        .stereo-badge.on {
            color: #0a0c0f;
            background: var(--green);
            border-color: var(--green);
            box-shadow: 0 0 8px rgba(0,230,118,0.4);
        }

        .signal-bars {
            display: flex;
            align-items: flex-end;
            gap: 2px;
            height: 16px;
        }
        .signal-bars .bar {
            width: 5px;
            border-radius: 1px;
            background: #1e2530;
            transition: background 0.3s, height 0.3s;
        }
        .signal-bars .bar:nth-child(1) { height: 4px; }
        .signal-bars .bar:nth-child(2) { height: 6px; }
        .signal-bars .bar:nth-child(3) { height: 9px; }
        .signal-bars .bar:nth-child(4) { height: 12px; }
        .signal-bars .bar:nth-child(5) { height: 16px; }
        .signal-bars .bar.lit-good  { background: var(--green); box-shadow: 0 0 4px rgba(0,230,118,0.5); }
        .signal-bars .bar.lit-warn  { background: #ffea00;      box-shadow: 0 0 4px rgba(255,234,0,0.4); }
        .signal-bars .bar.lit-bad   { background: #ff1744;      box-shadow: 0 0 4px rgba(255,23,68,0.4); }

        .freq-display {
            font-family: 'Orbitron', monospace;
            font-size: 2.8rem;
            font-weight: 700;
            color: var(--teal);
            text-align: center;
            letter-spacing: 0.05em;
            text-shadow: 0 0 20px rgba(0,188,212,0.4);
        }

        .freq-unit { font-size: 1rem; color: #4a5568; margin-left: 4px; }

        .controls { display: flex; gap: 8px; margin-bottom: 16px; }

        .controls input {
            flex: 1;
            background: var(--bg);
            border: 1px solid var(--border);
            color: #cdd5e0;
            font-family: 'Share Tech Mono', monospace;
            font-size: 1rem;
            padding: 10px 12px;
            border-radius: 3px;
            outline: none;
        }
        .controls input:focus { border-color: var(--teal); }

        .btn {
            background: transparent;
            border: 1px solid var(--teal);
            color: var(--teal);
            font-family: 'Orbitron', sans-serif;
            font-size: 0.75rem;
            letter-spacing: 0.1em;
            padding: 10px 16px;
            border-radius: 3px;
            cursor: pointer;
            transition: all 0.15s;
        }
        .btn:hover { background: rgba(0,188,212,0.1); }

        .audio-row {
            display: flex;
            gap: 8px;
            margin-bottom: 20px;
        }

        .btn-listen {
            flex: 1;
            border-color: var(--green);
            color: var(--green);
            padding: 12px;
            font-size: 0.8rem;
        }
        .btn-listen.active {
            background: rgba(0,230,118,0.05);
            box-shadow: 0 0 15px rgba(0,230,118,0.08);
        }
        .btn-listen.muted {
            border-color: #ff8a65;
            color: #ff8a65;
            background: rgba(255,138,101,0.08);
            box-shadow: 0 0 10px rgba(255,138,101,0.15);
        }

        .btn-arrow {
            border-color: var(--teal);
            color: var(--teal);
            padding: 10px 14px;
            font-size: 1rem;
            display: flex;
            align-items: center;
            justify-content: center;
            line-height: 1;
            min-width: 42px;
        }
        .btn-arrow:hover {
            background: rgba(0,188,212,0.15);
            box-shadow: 0 0 10px rgba(0,188,212,0.2);
        }
        .btn-arrow svg {
            width: 14px;
            height: 14px;
            fill: none;
            stroke: currentColor;
            stroke-width: 2.2;
            stroke-linecap: round;
            stroke-linejoin: round;
        }

        .vu-wrap {
            background: #0d0f12;
            border: 1px solid #1a2030;
            border-radius: 6px;
            padding: 16px 16px 10px;
        }

        .vu-label {
            font-size: 0.6rem;
            letter-spacing: 0.2em;
            color: #3a4555;
            text-align: center;
            margin-bottom: 12px;
            text-transform: uppercase;
        }

        canvas#vuCanvas { display: block; width: 100%; }

        .vu-db {
            text-align: center;
            font-size: 0.75rem;
            color: #4a5568;
            margin-top: 8px;
            letter-spacing: 0.1em;
        }
        .vu-db span { color: #7a8ea0; }

        .rds-wrap {
            background: #0d0f12;
            border: 1px solid #1a2030;
            border-radius: 6px;
            padding: 12px 16px;
            margin-top: 12px;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .rds-row { display: flex; align-items: center; gap: 8px; font-size: 0.75rem; }
        .rds-tag { color: #3a5060; letter-spacing: 0.1em; font-size: 0.65rem; min-width: 28px; }
        .rds-val { color: #00e676; font-family: 'Share Tech Mono', monospace; letter-spacing: 0.05em; }
        .rds-rt { color: #7a8ea0; font-size: 0.7rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 280px; }

        .ngrok-wrap {
            background: #0d0f12;
            border: 1px solid #1a2030;
            border-radius: 6px;
            padding: 12px 16px;
            margin-top: 12px;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        .ngrok-label { font-size: 0.6rem; letter-spacing: 0.2em; color: #3a4555; text-transform: uppercase; }
        .ngrok-row { display: flex; align-items: center; gap: 8px; }
        .ngrok-url {
            flex: 1;
            font-family: 'Share Tech Mono', monospace;
            font-size: 0.72rem;
            color: var(--teal);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            opacity: 0.7;
        }
        .ngrok-url.active { opacity: 1; color: var(--green); }
        .btn-ngrok { border-color: var(--teal); color: var(--teal); font-size: 0.65rem; padding: 6px 10px; }

        .icecast-wrap {
            background: #0d0f12;
            border: 1px solid #1a2030;
            border-radius: 6px;
            padding: 12px 16px;
            margin-top: 12px;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .icecast-label { font-size: 0.6rem; letter-spacing: 0.2em; color: #3a4555; text-transform: uppercase; }
        .icecast-row { display: flex; gap: 8px; }
        .icecast-row input {
            flex: 1;
            background: var(--bg);
            border: 1px solid var(--border);
            color: #cdd5e0;
            font-family: 'Share Tech Mono', monospace;
            font-size: 0.8rem;
            padding: 8px 10px;
            border-radius: 3px;
            outline: none;
        }
        .icecast-row input:focus { border-color: var(--teal); }
        .btn-ice { border-color: #ff8a65; color: #ff8a65; font-size: 0.7rem; padding: 8px 12px; }
        .btn-ice.active { background: rgba(255,138,101,0.1); box-shadow: 0 0 10px rgba(255,138,101,0.15); }

        .info-row {
            display: flex;
            justify-content: space-between;
            font-size: 0.7rem;
            color: #4a5568;
            margin-top: 16px;
            border-top: 1px solid var(--border);
            padding-top: 12px;
        }
        .info-val { color: #7a8ea0; }

        .mem-wrap {
            background: #0d0f12;
            border: 1px solid #1a2030;
            border-radius: 6px;
            padding: 12px 16px;
            margin-top: 12px;
        }

        .mem-label {
            font-size: 0.6rem;
            letter-spacing: 0.2em;
            color: #3a4555;
            text-transform: uppercase;
            margin-bottom: 10px;
        }

        .mem-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 6px;
        }

        .mem-btn {
            display: flex;
            flex-direction: column;
            align-items: center;
            background: #0a0c0f;
            border: 1px solid #1e2530;
            border-radius: 4px;
            padding: 8px 4px;
            cursor: pointer;
            transition: all 0.15s;
            position: relative;
        }

        .mem-btn:hover { border-color: var(--teal); }
        .mem-btn.active { border-color: var(--teal); background: rgba(0,188,212,0.05); }
        .mem-btn.saving { border-color: #ff8a65; background: rgba(255,138,101,0.08); }

        .mem-freq {
            font-family: 'Orbitron', sans-serif;
            font-size: 0.75rem;
            color: var(--teal);
            letter-spacing: 0.02em;
            margin: 4px 0;
        }

        .mem-freq.empty { color: #2a3545; }

        .mem-progress {
            width: 100%;
            height: 2px;
            background: transparent;
            border-radius: 1px;
            margin-top: 4px;
            overflow: hidden;
        }
        .mem-progress-bar {
            height: 100%;
            width: 0%;
            background: #ff8a65;
            border-radius: 1px;
            transition: width linear;
        }

        .mem-hint {
            font-size: 0.55rem;
            color: #2a3545;
            text-align: center;
            margin-top: 8px;
            letter-spacing: 0.05em;
        }

        .users-count {
            display: flex;
            align-items: center;
            gap: 4px;
            font-family: 'Orbitron', sans-serif;
            font-size: 0.65rem;
            color: #3a5060;
            letter-spacing: 0.05em;
        }
        .users-count svg { color: #3a5060; }
        .users-count.active { color: var(--teal); }
        .users-count.active svg { color: var(--teal); }
    </style>
</head>
<body>
    <div class="panel">
        <div class="header">
            <div class="title">FM MONITOR</div>
            <div style="display:flex;align-items:center;gap:10px;">
                <div class="users-count" id="usersCount">
                    <svg width="11" height="11" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="7" cy="5" r="3"/><path d="M2 13c0-3 2-5 5-5s5 2 5 5"/></svg>
                    <span id="usersNum">1</span>
                </div>
                <div class="status-dot" id="statusDot"></div>
            </div>
        </div>

        <div class="freq-row">
            <div style="display:flex;align-items:center;gap:10px;">
                <span class="stereo-badge" id="stereoBadge">STEREO</span>
                <div class="signal-bars" id="signalBars">
                    <div class="bar" id="bar1"></div>
                    <div class="bar" id="bar2"></div>
                    <div class="bar" id="bar3"></div>
                    <div class="bar" id="bar4"></div>
                    <div class="bar" id="bar5"></div>
                </div>
            </div>
            <div class="freq-display"><span id="freqDisplay">---.---</span><span class="freq-unit">MHz</span></div>
        </div>

        <div class="controls">
            <input type="number" id="freqInput" step="0.025" min="76.1" max="108" placeholder="MHz">
            <button class="btn btn-arrow" onclick="stepFreq(-0.1)" title="Descer 0.1 MHz">
                <svg viewBox="0 0 14 14"><polyline points="2,10 7,4 12,10"/></svg>
            </button>
            <button class="btn btn-arrow" onclick="stepFreq(0.1)" title="Subir 0.1 MHz">
                <svg viewBox="0 0 14 14"><polyline points="2,4 7,10 12,4"/></svg>
            </button>
            <button class="btn" onclick="tune()">TUNE</button>
        </div>

        <div class="audio-row">
            <button id="playBtn" class="btn btn-listen">&#9654; START AUDIO</button>
        </div>

        <div class="vu-wrap">
            <div class="vu-label">VU METER</div>
            <canvas id="vuCanvas" height="130"></canvas>
            <div class="vu-db">LEVEL <span id="dbVal">-&#8734; dB</span></div>
        </div>

        <div class="rds-wrap">
            <div class="rds-row">
                <span class="rds-tag">PS</span>
                <span class="rds-val" id="rdsPs">---</span>
                <span class="rds-tag" style="margin-left:auto">PI</span>
                <span class="rds-val" id="rdsPi">---</span>
            </div>
            <div class="rds-row">
                <span class="rds-tag">RT</span>
                <span class="rds-val rds-rt" id="rdsRt">---</span>
            </div>
            <div class="rds-row">
                <span class="rds-tag">PTY</span>
                <span class="rds-val" id="rdsPty">---</span>
                <span class="rds-tag" style="margin-left:auto">TP</span>
                <span class="rds-val" id="rdsTp">---</span>
            </div>
        </div>

        <div class="mem-wrap">
            <div class="mem-label">MEMÓRIAS</div>
            <div class="mem-grid">
                <div class="mem-btn" id="mem0">
                    <span class="mem-freq empty" id="memFreq0">---</span>
                    <div class="mem-progress" id="memProg0"></div>
                </div>
                <div class="mem-btn" id="mem1">
                    <span class="mem-freq empty" id="memFreq1">---</span>
                    <div class="mem-progress" id="memProg1"></div>
                </div>
                <div class="mem-btn" id="mem2">
                    <span class="mem-freq empty" id="memFreq2">---</span>
                    <div class="mem-progress" id="memProg2"></div>
                </div>
                <div class="mem-btn" id="mem3">
                    <span class="mem-freq empty" id="memFreq3">---</span>
                    <div class="mem-progress" id="memProg3"></div>
                </div>
            </div>
            <div class="mem-hint">Toque para sintonizar &nbsp;·&nbsp; Segure 3s para salvar</div>
        </div>

        <div class="ngrok-wrap">
            <div class="ngrok-label">NGROK URL</div>
            <div class="ngrok-row">
                <span class="ngrok-url" id="ngrokUrl">aguardando...</span>
                <button class="btn btn-ngrok" onclick="copyNgrok()">COPIAR</button>
            </div>
        </div>

        <div class="icecast-wrap">
            <div class="icecast-label">STREAM</div>
            <div class="icecast-row">
                <input type="text" id="icecastUrl" placeholder="icecast://source:pass@host:8000/stream">
                <button class="btn btn-ice" id="icecastBtn" onclick="toggleIcecast()">STREAM</button>
            </div>
        </div>

        <div class="info-row">
            <span>SR <span class="info-val">44100 Hz</span></span>
            <span>MODE <span class="info-val">WFM</span></span>
            <span>GAIN <span class="info-val">${CONFIG.gain} dB</span></span>
        </div>
    </div>

    <script>
        let ws, audioCtx, gainNode, nextStartTime = 0;
        let audioStarted = false, muted = false, icecastActive = false;
        let currentLevel = 0;
        let lowQualityCount = 0;
        const AUTO_MUTE_THRESHOLD = 15; // ~15 chunks consecutivos de chiado antes de mutar

        const els = {
            freq:      document.getElementById('freqDisplay'),
            input:     document.getElementById('freqInput'),
            playBtn:   document.getElementById('playBtn'),
            statusDot: document.getElementById('statusDot'),
            dbVal:     document.getElementById('dbVal'),
            rdsPs:     document.getElementById('rdsPs'),
            rdsPi:     document.getElementById('rdsPi'),
            rdsRt:     document.getElementById('rdsRt'),
            rdsPty:    document.getElementById('rdsPty'),
            rdsTp:     document.getElementById('rdsTp'),
        };

        // ===== VU METER =====
        const canvas = document.getElementById('vuCanvas');
        const ctx = canvas.getContext('2d');

        function resizeCanvas() {
            const dpr = window.devicePixelRatio || 1;
            canvas.width = canvas.offsetWidth * dpr;
            canvas.height = 130 * dpr;
            ctx.scale(dpr, dpr);
        }
        resizeCanvas();
        window.addEventListener('resize', resizeCanvas);

        const MIN_DB = -40, MAX_DB = 3;
        const MIN_ANGLE = -65 * Math.PI / 180;
        const MAX_ANGLE =  65 * Math.PI / 180;

        function dbToAngle(db) {
            const t = Math.max(0, Math.min(1, (db - MIN_DB) / (MAX_DB - MIN_DB)));
            return MIN_ANGLE + t * (MAX_ANGLE - MIN_ANGLE);
        }

        let needleAngle = MIN_ANGLE, targetAngle = MIN_ANGLE;
        let peakAngle = MIN_ANGLE, peakHold = 0;

        function drawVU() {
            const w = canvas.offsetWidth, h = 130;
            ctx.clearRect(0, 0, w, h);
            const cx = w / 2, cy = h * 1.05, R = h * 0.92;

            [{ from: MIN_DB, to: -6, color: '#00e676' },
             { from: -6, to: 0, color: '#ffea00' },
             { from: 0, to: MAX_DB, color: '#ff1744' }].forEach(z => {
                ctx.beginPath();
                ctx.arc(cx, cy, R, dbToAngle(z.from) - Math.PI/2, dbToAngle(z.to) - Math.PI/2);
                ctx.strokeStyle = z.color; ctx.lineWidth = 6; ctx.globalAlpha = 0.18; ctx.stroke(); ctx.globalAlpha = 1;
            });

            [{ db:-40,label:'-40',major:true },{ db:-30,label:'-30',major:true },
             { db:-20,label:'-20',major:true },{ db:-10,label:'-10',major:true },
             { db:-7, label:'-7', major:false},{ db:-5, label:'-5', major:false},
             { db:-3, label:'-3', major:false},{ db:0,  label:'0',  major:true },
             { db:3,  label:'+3', major:true }].forEach(t => {
                const angle = dbToAngle(t.db) - Math.PI/2;
                const tl = t.major ? 10 : 6;
                ctx.beginPath();
                ctx.moveTo(cx+(R-2)*Math.cos(angle), cy+(R-2)*Math.sin(angle));
                ctx.lineTo(cx+(R-2-tl)*Math.cos(angle), cy+(R-2-tl)*Math.sin(angle));
                ctx.strokeStyle = t.db >= 0 ? '#ff1744' : t.db >= -6 ? '#ffea00' : '#3a5060';
                ctx.lineWidth = t.major ? 1.5 : 1; ctx.stroke();
                if (t.major) {
                    ctx.fillStyle = t.db >= 0 ? '#ff1744' : '#3a5568';
                    ctx.font = '9px Share Tech Mono,monospace';
                    ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
                    ctx.fillText(t.label, cx+(R-20)*Math.cos(angle), cy+(R-20)*Math.sin(angle));
                }
            });

            if (peakAngle > MIN_ANGLE + 0.05) {
                const pa = peakAngle - Math.PI/2;
                ctx.beginPath();
                ctx.moveTo(cx+(R-8)*Math.cos(pa), cy+(R-8)*Math.sin(pa));
                ctx.lineTo(cx+(R-2)*Math.cos(pa), cy+(R-2)*Math.sin(pa));
                ctx.strokeStyle = '#ff8a65'; ctx.lineWidth = 2; ctx.stroke();
            }

            const na = needleAngle - Math.PI/2, nLen = R-14;
            ctx.beginPath(); ctx.moveTo(cx,cy);
            ctx.lineTo(cx+nLen*Math.cos(na+0.015), cy+nLen*Math.sin(na+0.015));
            ctx.strokeStyle = 'rgba(0,0,0,0.5)'; ctx.lineWidth = 3; ctx.stroke();
            ctx.beginPath(); ctx.moveTo(cx,cy);
            ctx.lineTo(cx+nLen*Math.cos(na), cy+nLen*Math.sin(na));
            ctx.strokeStyle = needleAngle > dbToAngle(0) ? '#ff1744' : needleAngle > dbToAngle(-6) ? '#ffea00' : '#e0e8f0';
            ctx.lineWidth = 1.8; ctx.lineCap = 'round'; ctx.stroke();
            ctx.beginPath(); ctx.arc(cx,cy,5,0,Math.PI*2); ctx.fillStyle='#2a3545'; ctx.fill();
            ctx.strokeStyle='#4a5568'; ctx.lineWidth=1; ctx.stroke();
            ctx.beginPath(); ctx.arc(cx,cy,2,0,Math.PI*2); ctx.fillStyle='#7a8ea0'; ctx.fill();
        }

        function updateNeedle() {
            if (muted || !audioStarted) {
                needleAngle += (MIN_ANGLE - needleAngle) * 0.06;
                peakAngle += (MIN_ANGLE - peakAngle) * 0.04;
                peakHold = 0;
                drawVU();
                els.dbVal.textContent = '-\u221e dB';
            } else {
                const db = currentLevel > 0.001 ? 20 * Math.log10(currentLevel) : MIN_DB;
                targetAngle = dbToAngle(Math.max(MIN_DB, Math.min(MAX_DB, db)));
                needleAngle += (targetAngle - needleAngle) * (targetAngle > needleAngle ? 0.35 : 0.06);
                if (needleAngle > peakAngle) { peakAngle = needleAngle; peakHold = 120; }
                else { peakHold--; if (peakHold <= 0) peakAngle += (MIN_ANGLE - peakAngle) * 0.03; }
                drawVU();
                els.dbVal.textContent = currentLevel > 0.001 ? db.toFixed(1) + ' dB' : '-\u221e dB';
            }
            requestAnimationFrame(updateNeedle);
        }
        updateNeedle();

        // ===== WEBSOCKET =====
        function connect() {
            const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(protocol + '//' + location.host);
            ws.binaryType = 'arraybuffer';
            ws.onopen = () => els.statusDot.classList.add('on');
            ws.onclose = () => { els.statusDot.classList.remove('on'); setTimeout(connect, 3000); };
            ws.onmessage = (event) => {
                if (typeof event.data === 'string') {
                    const msg = JSON.parse(event.data);
                    if (msg.type === 'status') {
                        els.freq.innerText = (msg.freq / 1000000).toFixed(3);
                        els.input.value = (msg.freq / 1000000).toFixed(3);
                    }
                    if (msg.type === 'level') {
                        currentLevel = msg.audio;
                        if (msg.quality !== undefined) {
                            updateSignalBars(msg.quality);
                            // Auto-mute se chiado constante
                            if (audioStarted && !muted) {
                                if (msg.quality <= 1) {
                                    lowQualityCount++;
                                    if (lowQualityCount >= AUTO_MUTE_THRESHOLD) {
                                        autoMute();
                                    }
                                } else {
                                    lowQualityCount = 0;
                                }
                            }
                        }
                    }
                    if (msg.type === 'stereo') {
                        document.getElementById('stereoBadge').classList.toggle('on', msg.stereo);
                    }
                    if (msg.type === 'rds') {
                        const d = msg.data;
                        if (d.ps) els.rdsPs.textContent = d.ps;
                        if (d.pi) els.rdsPi.textContent = d.pi;
                        if (d.radiotext) els.rdsRt.textContent = d.radiotext;
                        if (d.prog_type) els.rdsPty.textContent = d.prog_type;
                        if (d.tp !== undefined) els.rdsTp.textContent = d.tp ? 'YES' : 'NO';
                    }
                    if (msg.type === 'rds_clear') {
                        els.rdsPs.textContent = els.rdsPi.textContent = els.rdsRt.textContent =
                        els.rdsPty.textContent = els.rdsTp.textContent = '---';
                        document.getElementById('stereoBadge').classList.remove('on');
                    }
                    if (msg.type === 'ngrok_url') {
                        const el = document.getElementById('ngrokUrl');
                        el.textContent = msg.url;
                        el.classList.add('active');
                    }
                    if (msg.type === 'memories') {
                        updateMemories(msg.memories);
                    }
                    if (msg.type === 'users') {
                        const n = msg.count;
                        document.getElementById('usersNum').textContent = n;
                        document.getElementById('usersCount').classList.toggle('active', n > 1);
                    }
                    if (msg.type === 'icecast_status') {
                        const btn = document.getElementById('icecastBtn');
                        btn.classList.toggle('active', msg.active);
                        btn.textContent = msg.active ? 'STOP' : 'STREAM';
                    }
                } else {
                    playAudio(event.data);
                }
            };
        }

        function playAudio(arrayBuffer) {
            if (!audioCtx || !audioStarted) return;
            const int16 = new Int16Array(arrayBuffer);
            const float32 = new Float32Array(int16.length);
            for (let i = 0; i < int16.length; i++) float32[i] = int16[i] / 32768.0;
            if (!gainNode) {
                gainNode = audioCtx.createGain();
                gainNode.gain.value = muted ? 0 : 1;
                gainNode.connect(audioCtx.destination);
            }
            const audioBuffer = audioCtx.createBuffer(1, float32.length, 44100);
            audioBuffer.getChannelData(0).set(float32);
            const source = audioCtx.createBufferSource();
            source.buffer = audioBuffer;
            source.connect(gainNode);
            const now = audioCtx.currentTime;
            if (nextStartTime < now) nextStartTime = now + 0.2;
            source.start(nextStartTime);
            nextStartTime += audioBuffer.duration;
        }

        els.playBtn.addEventListener('click', () => {
            if (!audioStarted) {
                // Primeira vez: inicia o áudio
                if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 44100 });
                if (audioCtx.state === 'suspended') audioCtx.resume();
                if (!gainNode) {
                    gainNode = audioCtx.createGain();
                    gainNode.gain.value = 1;
                    gainNode.connect(audioCtx.destination);
                }
                audioStarted = true;
                muted = false;
                els.playBtn.innerHTML = '&#9646;&#9646; MUTE';
                els.playBtn.classList.add('active');
                els.playBtn.classList.remove('muted');
            } else {
                // Já iniciado: toggle mute
                muted = !muted;
                if (gainNode) gainNode.gain.value = muted ? 0 : 1;
                if (muted) {
                    els.playBtn.innerHTML = '&#128263; MUTED';
                    els.playBtn.classList.add('muted');
                    els.playBtn.classList.remove('active');
                } else {
                    els.playBtn.innerHTML = '&#9646;&#9646; MUTE';
                    els.playBtn.classList.remove('muted');
                    els.playBtn.classList.add('active');
                }
            }
        });

        window.toggleMute = () => {};  // não usado mais

        window.stepFreq = (delta) => {
            const MIN_FREQ = 76.1, MAX_FREQ = 108.0;
            const current = parseFloat(els.input.value) || parseFloat(els.freq.innerText) || 101.7;
            let next = Math.round((current + delta) * 1000) / 1000;
            if (next < MIN_FREQ) next = MIN_FREQ;
            if (next > MAX_FREQ) next = MAX_FREQ;
            els.input.value = next.toFixed(3);
        };

        window.tune = () => {
            let freq = parseFloat(els.input.value);
            if (!freq) return;
            if (freq < 76.1 || freq > 108) {
                freq = freq < 76.1 ? 76.1 : 108;
                els.input.value = freq.toFixed(3);
                showFreqAlert();
            }
            if (ws) ws.send(JSON.stringify({ type: 'tune', freq }));
        };

        function autoMute() {
            lowQualityCount = 0;
            muted = true;
            if (gainNode) gainNode.gain.value = 0;
            els.playBtn.innerHTML = '&#128263; MUTED';
            els.playBtn.classList.add('muted');
            els.playBtn.classList.remove('active');
            showAlert('CHIADO DETECTADO — ÁUDIO MUTADO', '#ff8a65');
        }

        function updateSignalBars(quality) {
            for (let i = 1; i <= 5; i++) {
                const bar = document.getElementById('bar' + i);
                bar.classList.remove('lit-good', 'lit-warn', 'lit-bad');
                if (i <= quality) {
                    if (quality <= 1)      bar.classList.add('lit-bad');
                    else if (quality <= 3) bar.classList.add('lit-warn');
                    else                   bar.classList.add('lit-good');
                }
            }
        }

        function showAlert(message, color) {
            color = color || '#ff8a65';
            let alert = document.getElementById('freqAlert');
            if (!alert) {
                alert = document.createElement('div');
                alert.id = 'freqAlert';
                document.body.appendChild(alert);
            }
            alert.style.cssText = 'position:fixed;top:18px;left:50%;transform:translateX(-50%);background:#1a0a00;border:1px solid ' + color + ';color:' + color + ';font-family:Orbitron,sans-serif;font-size:0.7rem;letter-spacing:0.15em;padding:10px 22px;border-radius:4px;box-shadow:0 0 18px rgba(255,138,101,0.25);z-index:9999;pointer-events:none;transition:opacity 0.4s;';
            alert.textContent = message;
            alert.style.opacity = '1';
            clearTimeout(alert._t);
            alert._t = setTimeout(() => { alert.style.opacity = '0'; }, 2500);
        }

        function showFreqAlert() {
            showAlert('FAIXA FM: 76.1 – 108.0 MHz');
        }

        // ===== MEMÓRIAS =====
        // ===== MEMÓRIAS COMPARTILHADAS =====
        let sharedMems = [null, null, null, null];
        let activeMem = -1;

        function updateMemories(mems) {
            sharedMems = mems;
            mems.forEach((freq, i) => {
                const el = document.getElementById('memFreq' + i);
                const btn = document.getElementById('mem' + i);
                if (freq) {
                    el.textContent = parseFloat(freq).toFixed(2);
                    el.classList.remove('empty');
                } else {
                    el.textContent = '---';
                    el.classList.add('empty');
                }
                btn.classList.toggle('active', i === activeMem);
            });
        }

        const HOLD_TIME = 3000;
        let holdTimers = [null, null, null, null];

        for (let i = 0; i < 4; i++) {
            const btn = document.getElementById('mem' + i);
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
                    if (freq && ws) {
                        ws.send(JSON.stringify({ type: 'save_memory', index: i, freq }));
                        showAlert('SALVO EM M' + (i+1) + ': ' + freq.toFixed(2) + ' MHz', '#00e676');
                    }
                    cancelHold(i);
                }, HOLD_TIME);
            };

            const cancelHold = (idx) => {
                clearTimeout(holdTimers[idx]);
                holdTimers[idx] = null;
                const b = document.getElementById('mem' + idx);
                const p = document.getElementById('memProg' + idx);
                b.classList.remove('saving');
                p.innerHTML = '';
            };

            const endHold = (e) => {
                if (holdTimers[i] !== null) {
                    cancelHold(i);
                    if (sharedMems[i]) {
                        activeMem = i;
                        els.input.value = parseFloat(sharedMems[i]).toFixed(3);
                        updateMemories(sharedMems);
                        if (ws) ws.send(JSON.stringify({ type: 'tune', freq: sharedMems[i] }));
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

        window.saveMem   = () => {};
        window.recallMem = () => {};

        window.copyNgrok = () => {
            const url = document.getElementById('ngrokUrl').textContent;
            if (!url || url === 'aguardando...') return;
            navigator.clipboard.writeText(url).then(() => {
                showAlert('URL COPIADA!', '#00e676');
            }).catch(() => {
                showAlert('ERRO AO COPIAR', '#ff8a65');
            });
        };

        window.toggleIcecast = () => {
            const url = document.getElementById('icecastUrl').value.trim();
            if (!icecastActive && !url) return;
            icecastActive = !icecastActive;
            ws.send(JSON.stringify({ type: 'icecast', url: icecastActive ? url : null }));
        };

        connect();
    </script>
</body>
</html>
`;

server.listen(CONFIG.webPort, () => {
    console.log(`Server running at http://localhost:${CONFIG.webPort}`);
    startRadio(CONFIG.frequency);

    // Aguarda ngrok iniciar e envia URL via ntfy
    setTimeout(() => {
        http.get('http://localhost:4040/api/tunnels', (res) => {
            let data = '';
            res.on('data', d => data += d);
            res.on('end', () => {
                try {
                    const json = JSON.parse(data);
                    const tunnel = json.tunnels.find(t => t.proto === 'https');
                    if (tunnel) {
                        ngrokUrl = url;
                        console.log('[ngrok] URL publica:', url);
                        sendNtfy('FM Monitor Online', 'Acesse: ' + url);
                        // Envia URL para todos os clientes conectados
                        wss.clients.forEach(c => {
                            if (c.readyState === WebSocket.OPEN) {
                                c.send(JSON.stringify({ type: 'ngrok_url', url }));
                            }
                        });
                    } else {
                        console.log('[ngrok] Nenhum tunnel https encontrado');
                    }
                } catch(e) {
                    console.log('[ngrok] Erro ao ler tunnels:', e.message);
                }
            });
        }).on('error', (e) => {
            console.log('[ngrok] Nao encontrado (ainda nao iniciado?):', e.message);
        });
    }, 5000); // aguarda 5s para o ngrok subir
});

function cleanup() {
    console.log('[Exit] Encerrando processos...');
    killProcess(rtlProcess);
    killProcess(rdsProcess);
    killProcess(pilotProcess);
    killProcess(icecastProcess);
    spawn('bash', ['-c', 'pkill -9 rtl_fm; pkill -9 redsea; pkill -9 sox'], { detached: true });
    setTimeout(() => process.exit(0), 300);
}

process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);
