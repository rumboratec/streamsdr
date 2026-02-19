const http = require('http');
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
let icecastProcess = null;
let currentFreq = CONFIG.frequency;
let rdsBuffer = '';
let squelchLevel = 0; // 0 = desligado, 0.01~0.5 = limiar

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
        this.sampleRate = 44100;
        this.pilotFreq  = 19000;
        this.w     = 2 * Math.PI * this.pilotFreq / this.sampleRate;
        this.coeff = 2 * Math.cos(this.w);
        this.s1 = 0; this.s2 = 0;
        this.count = 0;
        this.blockSize = 441;
        this.totalPower = 0;
        this.isStereo = false;
        this.threshold = 0.003;
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
                const stereo = ratio > this.threshold;

                if (stereo !== this.isStereo) {
                    this.isStereo = stereo;
                    wss.clients.forEach((client) => {
                        if (client.readyState === WebSocket.OPEN) {
                            client.send(JSON.stringify({ type: 'stereo', stereo }));
                        }
                    });
                    console.log('[Stereo]', stereo ? 'STEREO detected' : 'MONO');
                }
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
    setTimeout(() => _startRadio(freq), 800);
}

function _startRadio(freq) {
    currentFreq = freq;
    console.log(`[Radio] Tuning to ${(freq/1000000).toFixed(2)} MHz`);

    const fifo = '/tmp/rds_fifo';
    const mkfifo = spawn('bash', ['-c', `[ -p ${fifo} ] || mkfifo ${fifo}`]);
    mkfifo.on('close', () => {
        const cmd = `rtl_fm -f ${freq} -M wfm -s 171k -E deemp -g ${CONFIG.gain} -A std | tee ${fifo} | sox -t raw -r 171000 -e signed -b 16 -c 1 - -t raw -r 44100 -`;
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
                    broadcastRDS(rds);
                } catch(e) {}
            });
        });
        rdsProcess.stderr.on('data', (d) => console.log(`[redsea] ${d.toString().trim()}`));
        rdsProcess.on('close', (code) => console.log(`[RDS] exited ${code}`));

        setTimeout(() => {
            rtlProcess = spawn('bash', ['-c', cmd], { detached: true });
            rtlProcess.stdout.on('data', (chunk) => {
                const int16 = new Int16Array(chunk.buffer, chunk.byteOffset, chunk.length / 2);
                let sum = 0;
                for (let i = 0; i < int16.length; i++) sum += Math.abs(int16[i]);
                const level = sum / int16.length / 32768.0;

                wss.clients.forEach((client) => {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({ type: 'level', audio: level }));
                    }
                });

                stereoDetector.process(chunk);

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

wss.on('connection', (ws) => {
    console.log('[Web] Client connected');
    sendStatus(ws);

    ws.on('message', (message) => {
        try {
            const cmd = JSON.parse(message);
            if (cmd.type === 'tune') {
                const freqHz = Math.floor(parseFloat(cmd.freq) * 1000000);
                currentFreq = freqHz;
                wss.clients.forEach(c => {
                    if (c.readyState === WebSocket.OPEN) {
                        sendStatus(c);
                        c.send(JSON.stringify({ type: 'rds_clear' }));
                        c.send(JSON.stringify({ type: 'stereo', stereo: false }));
                    }
                });
                startRadio(freqHz);
            }
            if (cmd.type === 'icecast') {
                startIcecast(cmd.url || null);
                ws.send(JSON.stringify({ type: 'icecast_status', active: !!cmd.url }));
            }
            if (cmd.type === 'squelch') {
                squelchLevel = parseFloat(cmd.level) || 0;
                console.log('[Squelch] level:', squelchLevel);
            }
        } catch(e) { console.error(e); }
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

        .btn-mute {
            border-color: #ff8a65;
            color: #ff8a65;
            padding: 12px 14px;
            font-size: 0.75rem;
        }
        .btn-mute.muted {
            background: rgba(255,138,101,0.15);
            box-shadow: 0 0 10px rgba(255,138,101,0.2);
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

        .mem-freq {
            font-family: 'Orbitron', sans-serif;
            font-size: 0.7rem;
            color: var(--teal);
            letter-spacing: 0.02em;
        }

        .mem-freq.empty { color: #2a3545; }

        .mem-slot {
            font-size: 0.55rem;
            color: #3a5060;
            margin-bottom: 2px;
        }

        .mem-save {
            font-size: 0.5rem;
            color: #3a5060;
            margin-top: 4px;
            cursor: pointer;
            letter-spacing: 0.05em;
        }

        .mem-save:hover { color: #ff8a65; }

        .squelch-wrap {
            background: #0d0f12;
            border: 1px solid #1a2030;
            border-radius: 6px;
            padding: 12px 16px;
            margin-top: 12px;
        }

        .squelch-row {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .squelch-label {
            font-size: 0.6rem;
            letter-spacing: 0.15em;
            color: #3a5060;
            text-transform: uppercase;
            min-width: 56px;
        }

        .squelch-val {
            font-family: 'Orbitron', sans-serif;
            font-size: 0.75rem;
            color: var(--teal);
            min-width: 36px;
            text-align: right;
        }

        input[type=range] {
            flex: 1;
            -webkit-appearance: none;
            height: 4px;
            border-radius: 2px;
            background: #1a2030;
            outline: none;
        }

        input[type=range]::-webkit-slider-thumb {
            -webkit-appearance: none;
            width: 14px;
            height: 14px;
            border-radius: 50%;
            background: var(--teal);
            cursor: pointer;
        }

        input[type=range].active::-webkit-slider-thumb {
            background: #ff8a65;
        }
    </style>
</head>
<body>
    <div class="panel">
        <div class="header">
            <div class="title">FM MONITOR</div>
            <div class="status-dot" id="statusDot"></div>
        </div>

        <div class="freq-row">
            <span class="stereo-badge" id="stereoBadge">STEREO</span>
            <div class="freq-display"><span id="freqDisplay">---.---</span><span class="freq-unit">MHz</span></div>
        </div>

        <div class="controls">
            <input type="number" id="freqInput" step="0.025" placeholder="MHz">
            <button class="btn" onclick="tune()">TUNE</button>
        </div>

        <div class="audio-row">
            <button id="playBtn" class="btn btn-listen">&#9654; START AUDIO</button>
            <button id="muteBtn" class="btn btn-mute" onclick="toggleMute()">&#128264; MUTE</button>
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

        <div class="icecast-wrap">
            <div class="icecast-label">Icecast Stream</div>
            <div class="icecast-row">
                <input type="text" id="icecastUrl" placeholder="icecast://source:pass@host:8000/stream">
                <button class="btn btn-ice" id="icecastBtn" onclick="toggleIcecast()">STREAM</button>
            </div>
        </div>

        <div class="mem-wrap">
            <div class="mem-label">MEMÓRIAS</div>
            <div class="mem-grid">
                <div class="mem-btn" id="mem0" onclick="recallMem(0)">
                    <span class="mem-slot">M1</span>
                    <span class="mem-freq empty" id="memFreq0">---</span>
                    <span class="mem-save" onclick="event.stopPropagation();saveMem(0)">SALVAR</span>
                </div>
                <div class="mem-btn" id="mem1" onclick="recallMem(1)">
                    <span class="mem-slot">M2</span>
                    <span class="mem-freq empty" id="memFreq1">---</span>
                    <span class="mem-save" onclick="event.stopPropagation();saveMem(1)">SALVAR</span>
                </div>
                <div class="mem-btn" id="mem2" onclick="recallMem(2)">
                    <span class="mem-slot">M3</span>
                    <span class="mem-freq empty" id="memFreq2">---</span>
                    <span class="mem-save" onclick="event.stopPropagation();saveMem(2)">SALVAR</span>
                </div>
                <div class="mem-btn" id="mem3" onclick="recallMem(3)">
                    <span class="mem-slot">M4</span>
                    <span class="mem-freq empty" id="memFreq3">---</span>
                    <span class="mem-save" onclick="event.stopPropagation();saveMem(3)">SALVAR</span>
                </div>
            </div>
        </div>

        <div class="squelch-wrap">
            <div class="squelch-row">
                <span class="squelch-label">SQUELCH</span>
                <input type="range" id="squelchSlider" min="0" max="100" value="0" oninput="updateSquelch(this.value)">
                <span class="squelch-val" id="squelchVal">OFF</span>
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
        let muted = false, icecastActive = false;
        let currentLevel = 0;

        const els = {
            freq:      document.getElementById('freqDisplay'),
            input:     document.getElementById('freqInput'),
            playBtn:   document.getElementById('playBtn'),
            muteBtn:   document.getElementById('muteBtn'),
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
            const db = currentLevel > 0.001 ? 20 * Math.log10(currentLevel) : MIN_DB;
            targetAngle = dbToAngle(Math.max(MIN_DB, Math.min(MAX_DB, db)));
            needleAngle += (targetAngle - needleAngle) * (targetAngle > needleAngle ? 0.35 : 0.06);
            if (needleAngle > peakAngle) { peakAngle = needleAngle; peakHold = 120; }
            else { peakHold--; if (peakHold <= 0) peakAngle += (MIN_ANGLE - peakAngle) * 0.03; }
            drawVU();
            els.dbVal.textContent = currentLevel > 0.001 ? db.toFixed(1) + ' dB' : '-\u221e dB';
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
                    if (msg.type === 'level') currentLevel = msg.audio;
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
                        if (d.di && d.di.stereo !== undefined) {
                            document.getElementById('stereoBadge').classList.toggle('on', d.di.stereo);
                        }
                    }
                    if (msg.type === 'rds_clear') {
                        els.rdsPs.textContent = els.rdsPi.textContent = els.rdsRt.textContent =
                        els.rdsPty.textContent = els.rdsTp.textContent = '---';
                        document.getElementById('stereoBadge').classList.remove('on');
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
            if (!audioCtx) return;
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
            if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: 44100 });
            if (audioCtx.state === 'suspended') audioCtx.resume();
            els.playBtn.innerHTML = '&#9646;&#9646; LISTENING...';
            els.playBtn.classList.add('active');
        });

        window.toggleMute = () => {
            muted = !muted;
            if (gainNode) gainNode.gain.value = muted ? 0 : 1;
            els.muteBtn.classList.toggle('muted', muted);
            els.muteBtn.innerHTML = muted ? '&#128263; MUTED' : '&#128264; MUTE';
        };

        window.tune = () => {
            const freq = parseFloat(els.input.value);
            if (freq && ws) ws.send(JSON.stringify({ type: 'tune', freq }));
        };

        // ===== MEMÓRIAS =====
        const memories = JSON.parse(localStorage.getItem('fmMemories') || '[null,null,null,null]');
        let activeMem = -1;

        function renderMemories() {
            memories.forEach((freq, i) => {
                const el = document.getElementById('memFreq' + i);
                const btn = document.getElementById('mem' + i);
                if (freq) {
                    el.textContent = freq.toFixed(3);
                    el.classList.remove('empty');
                } else {
                    el.textContent = '---';
                    el.classList.add('empty');
                }
                btn.classList.toggle('active', i === activeMem);
            });
        }
        renderMemories();

        window.saveMem = (i) => {
            const freq = parseFloat(els.input.value || els.freq.innerText);
            if (!freq) return;
            memories[i] = freq;
            localStorage.setItem('fmMemories', JSON.stringify(memories));
            renderMemories();
        };

        window.recallMem = (i) => {
            if (!memories[i]) return;
            activeMem = i;
            els.input.value = memories[i].toFixed(3);
            renderMemories();
            if (ws) ws.send(JSON.stringify({ type: 'tune', freq: memories[i] }));
        };

        window.updateSquelch = (val) => {
            const level = parseInt(val);
            const squelchVal = document.getElementById('squelchVal');
            const slider = document.getElementById('squelchSlider');
            if (level === 0) {
                squelchVal.textContent = 'OFF';
                slider.classList.remove('active');
            } else {
                squelchVal.textContent = level + '%';
                slider.classList.add('active');
            }
            // Converte 0-100 para 0-0.3 (faixa útil de nível)
            const squelchLevel = level === 0 ? 0 : level / 100 * 0.15;
            if (ws) ws.send(JSON.stringify({ type: 'squelch', level: squelchLevel }));
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
});

function cleanup() {
    console.log('[Exit] Encerrando processos...');
    killProcess(rtlProcess);
    killProcess(rdsProcess);
    killProcess(icecastProcess);
    spawn('bash', ['-c', 'pkill -9 rtl_fm; pkill -9 redsea; pkill -9 sox'], { detached: true });
    setTimeout(() => process.exit(0), 300);
}

process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);
