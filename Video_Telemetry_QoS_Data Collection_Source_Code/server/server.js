/**
 * Telemetry API Server
 *
 * Express server that:
 *  - Serves the frontend player from the /public directory
 *  - Exposes POST /api/telemetry to receive batched telemetry events
 *  - Logs all activity to server/logs/server.log and the terminal
 *
 * Start: node server/server.js
 * Port:  8080
 */

const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

// Absolute path to the log file inside the logs/ subdirectory
const logFilePath = path.join(__dirname, 'logs', 'server.log');

/**
 * Appends a timestamped log line to server.log.
 * Objects are serialised to pretty JSON; primitives are stringified.
 * @param {"LOG"|"ERROR"|"WARN"} level
 * @param {any[]} args - Arguments passed to the console overrides below
 */
function writeLog(level, args) {
    const line = `[${new Date().toISOString()}] [${level}] ${args.map((arg) => {
        if (typeof arg === 'string') return arg;
        try {
            return typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg);
        } catch {
            return String(arg);
        }
    }).join(' ')}\n`;

    fs.appendFileSync(logFilePath, line);
}

// ── Console overrides ────────────────────────────────────────────────────────
// Intercept console.log/error/warn so all output goes to both terminal and file
const originalLog = console.log.bind(console);
const originalError = console.error.bind(console);
const originalWarn = console.warn.bind(console);

console.log = (...args) => {
    writeLog('LOG', args);
    originalLog(...args);
};

console.error = (...args) => {
    writeLog('ERROR', args);
    originalError(...args);
};

console.warn = (...args) => {
    writeLog('WARN', args);
    originalWarn(...args);
};

// ── Express app setup ────────────────────────────────────────────────────────
const app = express();
app.use(cors()); // Allow cross-origin requests from the frontend
app.use(express.json());

// Request logger middleware — logs method, path, status, and duration
app.use((req, res, next) => {
    const startedAt = Date.now();
    res.on('finish', () => {
        const durationMs = Date.now() - startedAt;
        console.log(`[HTTP] ${req.method} ${req.originalUrl} -> ${res.statusCode} (${durationMs}ms)`);
    });
    next();
});

// Serve frontend static files (index.html, telemetry-client.js, schemas)
app.use(express.static(path.join(__dirname, '..', 'public')));

// Support navigator.sendBeacon which sends the body as text/plain
app.use(express.text({ type: '*/*' }));

// ── Telemetry endpoint ───────────────────────────────────────────────────────
/**
 * POST /api/telemetry
 *
 * Receives a JSON batch of telemetry events from the video player client.
 * Body: { events: TelemetryEvent[] }
 *
 * Responds 200 { status: "ok" } on success.
 * Responds 400 { error: "Invalid JSON" } if the body cannot be parsed.
 */
app.post('/api/telemetry', (req, res) => {
    try {
        // sendBeacon sends the body as a string; fetch sends parsed JSON
        const data = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
        const eventCount = data.events?.length || 0;
        const prettyJson = JSON.stringify(data, null, 2);

        console.log(`\n📥 [Server] Received batch with ${eventCount} events:`);
        console.log(prettyJson);
        res.status(200).send({ status: 'ok' });
    } catch (e) {
        console.error("❌ Error parsing telemetry data", e);
        res.status(400).send({ error: 'Invalid JSON' });
    }
});

// ── Start server ─────────────────────────────────────────────────────────────
app.listen(8080, () => {
    console.log('🚀 Telemetry server is running at http://localhost:8080');
    console.log('Open http://localhost:8080 in your browser to see requests in the terminal.');
    console.log(`Logs are also written to ${logFilePath}`);
    console.log('Waiting for telemetry from the video player...');
});
