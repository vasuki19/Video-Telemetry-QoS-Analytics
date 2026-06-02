# Hive Streaming - Video Telemetry Collection System

A comprehensive telemetry collection system for HLS.js video streaming that captures playback metrics, network performance, quality of experience (QoE), and error events.

## 📋 Project Overview

This system monitors video playback in real-time, collecting detailed telemetry data including:
- **Playback Events**: Play, pause, seek, quality switches
- **Network Metrics**: Bandwidth, connection type, RTT, downlink speed
- **Quality Distribution**: Bitrate distribution across stream fragments
- **Player State**: Current time, muted state, volume level
- **QoE Metrics**: Rebuffer duration, stall count, watch time
- **Error Tracking**: Fatal and non-fatal error aggregation with recovery info

## 🏗️ Project Structure

```
Hive Streaming-Data Collection/
├── public/                          # Frontend assets
│   ├── index.html                   # Video player HTML page
│   ├── telemetry-client.js          # Main telemetry tracker class
│   └── telemetry/
│       └── schemas/                 # Modular event schemas
│           ├── telemetry-event-schema.js
│           ├── network-schema.js
│           ├── player-state-schema.js
│           ├── playback-quality-metrics-schema.js
│           ├── quality-distribution-schema.js
│           ├── identities-schema.js
│           ├── content-schema.js
│           └── environment-schema.js
├── server/                          # Backend server
│   ├── server.js                    # Express telemetry API server
│   └── logs/
│       └── server.log               # Server activity logs
├── package.json                     # Project dependencies
├── package-lock.json
└── README.md                        # This file
```

## 🛠️ Setup Instructions

### Prerequisites
- Node.js (v14+)
- npm

### Installation

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Verify structure:**
   ```bash
   ls -la
   # Should show: public/, server/, package.json, README.md
   ```

## 🚀 Running the Server

Start the telemetry server from the project root:

```bash
node server/server.js
```

Expected output:
```
🚀 Telemetry server is running at http://localhost:8080
Open http://localhost:8080 in your browser to see requests in the terminal.
Logs are also written to [project]/server/logs/server.log
Waiting for telemetry from the video player...
```

The server listens on **http://localhost:8080** and serves:
- Frontend: `public/index.html`
- API: `POST /api/telemetry` - Receives telemetry batches

## 🎬 Testing the Telemetry System

1. **Start the server:**
   ```bash
   node server/server.js
   ```

2. **Open in browser:**
   - Navigate to `http://localhost:8080`
   - You should see the Hive Streaming Player

3. **Play video:**
   - The page loads a live HLS stream from Hive Streaming
   - The `AdvancedVideoTelemetry` client automatically starts tracking

4. **Monitor telemetry:**
   - Watch the server terminal for incoming telemetry events
   - Events are logged with color-coded prefixes:
     - `[Telemetry Queued]` - Event added to queue
     - `[Telemetry Flush]` - Batch sent to server
     - `📥 [Server] Received batch` - Server received events

5. **Check logs:**
   - All server activity is logged to `server/logs/server.log`

## 📊 Telemetry Event Schema (v4.0.0)

Each event includes:

```javascript
{
  schemaVersion: "4.0.0",
  eventId: "uuid-timestamp-sequence",
  playbackSessionId: "uuid",
  sequenceNumber: 0,
  event_timestamp: 1234567890,
  elapsedMs: 1000,
  eventType: "PLAYBACK_START|BUFFER_START|QUALITY_CHANGE|ERROR|...",
  
  identities: {
    tenantId: "hive_local_dev",
    userId: "developer_01"
  },
  
  content: {
    manifestUrl: "https://...",
    durationSec: 120
  },
  
  environment: {
    playerType: "hls.js",
    playerVersion: "1.6.16",
    telemetryVersion: "4.0.0",
    userAgent: "..."
  },
  
  network: {
    estimatedBandwidthBps: 5000000,
    connectionType: "4g",
    downlinkMbps: 10,
    rttMs: 50,
    trafficBreakdown: { "1080p": { bytesDownloaded: 1000, ... }, ... },
    qualityDistribution: { "1080p": { byteSharePercent: 45.2, ... }, ... }
  },
  
  playerState: {
    currentTimeSec: 30.5,
    isPaused: false,
    isMuted: false,
    volume: 1
  },
  
  playbackQualityMetrics: {
    totalRebufferMs: 500,
    stallCount: 2,
    startupStallCount: 1,
    watchTimeSec: 30.5,
    rebufferRatio: 0.0163
  },
  
  payload: { /* Event-specific data */ }
}
```

## 📡 API Endpoints

### POST /api/telemetry
Receives batched telemetry events from the client.

**Request:**
```json
{
  "events": [
    { /* telemetry event object */ },
    { /* telemetry event object */ }
  ]
}
```

**Response:**
```json
{
  "status": "ok"
}
```

## 🔑 Key Features

### Modular Schema Architecture
- Each telemetry object is defined in a separate schema module
- Schemas export `defaults()` and `build(data)` methods
- Composition pattern for nested object normalization

### Event Types Tracked
- `PLAYBACK_START` - Video playback begins
- `BUFFER_START` / `BUFFER_END` - Rebuffering events
- `QUALITY_SWITCH_START` / `QUALITY_CHANGE` - Adaptive bitrate switching
- `ERROR` - Fatal errors requiring failover
- `ERROR_SUMMARY` - Aggregated non-fatal errors
- Custom events via `_queueEvent(eventType, payload)`

### Batching & Transmission
- Events queued in memory and flushed every 10 seconds (configurable)
- Uses `navigator.sendBeacon()` with fetch fallback
- Graceful flushing on page unload

### Error Classification
- **Fatal Errors**: Immediately flushed; trigger failover logic
- **Non-Fatal Errors**: Aggregated; summarized on flush
- Error tracking includes: code, message, category, severity, recovery status

## ⚙️ Configuration

Initialize telemetry in `public/index.html`:

```javascript
const tracker = new AdvancedVideoTelemetry(hls, video, {
  tenantId: "your_tenant_id",      // Default: "hive_test_tenant"
  userId: "your_user_id",          // Default: "anonymous_user"
  endpoint: "http://your-api",     // Default: "http://localhost:3000/api/telemetry"
  batchIntervalMs: 10000           // Default: 10000 (milliseconds)
});
```

## 📦 Dependencies

- **express** - Web server framework
- **cors** - Cross-origin request handling

## 🔍 Debugging

### View Real-Time Logs
```bash
tail -f server/logs/server.log
```

### Browser Console
The client logs telemetry events to the browser console with color-coded output:
```javascript
[Telemetry Queued #1] PLAYBACK_START
[Telemetry Flush] Sending batch to server...
```

### Event Queue Status
Access telemetry tracker from browser console:
```javascript
// If tracker is exposed globally
console.log(tracker.eventQueue);
console.log(tracker.sessionId);
console.log(tracker.trafficStats);
```

## 🛡️ Best Practices

1. **Batch Flushes**: Configure `batchIntervalMs` based on your network conditions
   - Faster networks: 5000-10000ms
   - Slower networks: 15000-30000ms

2. **Error Handling**: Monitor `ERROR_SUMMARY` events for non-fatal errors
   - These indicate recoverable issues

3. **Bandwidth Tracking**: Monitor `network.trafficBreakdown` to understand quality distribution

4. **QoE Metrics**: Track `rebufferRatio` and `stallCount` for user experience measurement
   - Ideal: <5% rebuffer ratio, <2 stalls per session

## 📝 License

Proprietary - Hive Streaming

## 🤝 Support

For issues or questions, check `server/logs/server.log` for detailed error traces.
