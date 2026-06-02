/**
 * AdvancedVideoTelemetry
 *
 * Tracks HLS.js video playback and sends batched telemetry events to a backend API.
 *
 * Captured events:
 *  - PLAYBACK_START       : First frame rendered; includes time-to-first-frame
 *  - BUFFER_START         : Player stalled waiting for data (STARTUP / SEEK / STALL)
 *  - BUFFER_END           : Player resumed after buffering; includes duration
 *  - QUALITY_SWITCH_START : ABR engine began switching to a new bitrate level
 *  - QUALITY_CHANGE       : ABR switch completed; includes from/to bitrate and resolution
 *  - ERROR                : Fatal HLS error; flushed immediately
 *  - ERROR_SUMMARY        : Aggregated non-fatal errors; flushed on each batch interval
 *
 * Usage:
 *   const tracker = new AdvancedVideoTelemetry(hlsInstance, videoElement, {
 *     tenantId: "my_tenant",
 *     userId: "user_123",
 *     endpoint: "https://api.example.com/telemetry",
 *     batchIntervalMs: 10000
 *   });
 */
class AdvancedVideoTelemetry {
  /**
   * @param {Hls}             playerInstance  - Initialised hls.js instance
   * @param {HTMLVideoElement} videoElement   - The <video> DOM element attached to the player
   * @param {Object}          config          - Optional configuration overrides
   * @param {string}          config.tenantId        - Tenant identifier (default: "hive_test_tenant")
   * @param {string}          config.userId          - User identifier (default: "anonymous_user")
   * @param {string}          config.endpoint        - Telemetry API endpoint URL
   * @param {number}          config.batchIntervalMs - How often to flush events in ms (default: 10000)
   */
  constructor(playerInstance, videoElement, config = {}) {
    if (!playerInstance || !videoElement) {
      throw new Error("Missing required player or media elements.");
    }
    this.hls = playerInstance;
    this.video = videoElement;
    this.tenantId = config.tenantId || "hive_test_tenant";
    this.userId = config.userId || "anonymous_user";
    this.endpoint = config.endpoint || "http://localhost:3000/api/telemetry";
    this.batchIntervalMs = config.batchIntervalMs || 10000;
    this.schemaVersion = "4.0.0";

    // Unique ID for this playback session
    this.sessionId = this._generateUUID();
    // Timestamp when the tracker was initialised (used to compute elapsedMs per event)
    this.sessionStartTs = Date.now();
    // Monotonically increasing counter stamped on every event
    this.sequenceNumber = 0;
    // Timestamp set when buffering starts; used to compute buffer duration
    this.bufferStartTime = null;
    // True while the user is seeking (distinguishes SEEK buffers from STALL buffers)
    this.isSeeking = false;
    // True after the first playing event fires
    this.isStarted = false;
    // Timestamp of first frame; used to compute time-to-first-frame
    this.playbackStartTs = null;
    // Cumulative dropped frame count from the previous poll (delta = current - last)
    this.lastDroppedFrames = 0;
    // Timestamp when an ABR level switch began; null when no switch is in progress
    this.qualitySwitchStartTime = null;
    // Bitrate/resolution of the currently active quality level
    this.previousQuality = { bitrateBps: null, resolution: null };
    // Per-resolution download stats accumulated from FRAG_LOADED events
    this.trafficStats = {};
    // In-memory queue of telemetry events pending transmission
    this.eventQueue = [];
    // Number of mid-playback stall events
    this.stallCount = 0;
    // Number of stall events that occurred before the first frame
    this.startupStallCount = 0;
    // Total time spent rebuffering across the session in milliseconds
    this.totalRebufferMs = 0;
    // Map of non-fatal errors keyed by "code|message" for deduplication
    this.nonFatalErrorAggregate = {};

    this._initListeners();
    this._startQueueTimer();
  }

  /**
   * Generates a RFC 4122 v4 UUID for session and event identification.
   * @returns {string}
   */
  _generateUUID() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
      const r = Math.random() * 16 | 0;
      const v = c === 'x' ? r : (r & 0x3 | 0x8);
      return v.toString(16);
    });
  }

  /**
   * Builds a unique event ID composed of session ID, current timestamp, and sequence number.
   * Format: "<sessionId>-<timestampMs>-<sequenceNumber>"
   * @returns {string}
   */
  _generateEventId() {
    return `${this.sessionId}-${Date.now()}-${this.sequenceNumber}`;
  }

  /**
   * Classifies the current buffering reason based on player state.
   *  - STARTUP : Buffering before the first frame has been rendered
   *  - SEEK    : Buffering triggered by a user seek action
   *  - STALL   : Mid-playback buffer underrun
   * @returns {"STARTUP"|"SEEK"|"STALL"}
   */
  _getBufferType() {
    if (!this.isStarted) return "STARTUP";
    if (this.isSeeking) return "SEEK";
    return "STALL";
  }

  /**
   * Returns the number of dropped frames since the last call using the
   * browser's Video Playback Quality API. Falls back to 0 if unsupported.
   * @returns {number}
   */
  _getDroppedFramesDelta() {
    if (this.video.getVideoPlaybackQuality) {
      const quality = this.video.getVideoPlaybackQuality();
      const currentDropped = quality.droppedVideoFrames || 0;
      const delta = currentDropped - this.lastDroppedFrames;
      this.lastDroppedFrames = currentDropped;
      return delta;
    }
    return 0;
  }

  /**
   * Returns a deep copy of the accumulated per-resolution traffic stats.
   * Deep-copying prevents mutation while the snapshot is being processed.
   * @returns {Object}
   */
  _getTrafficSnapshot() {
    return JSON.parse(JSON.stringify(this.trafficStats));
  }

  /**
   * Converts raw traffic stats into a per-resolution quality distribution map.
   * Each entry includes bytes downloaded, fragments loaded, and the percentage
   * share of total bytes for that resolution.
   *
   * @param {Object} trafficSnapshot - Snapshot from _getTrafficSnapshot()
   * @returns {Object|null} Quality distribution map, or null if no data
   */
  _getDistributionSnapshot(trafficSnapshot) {
    const qualityEntries = Object.entries(trafficSnapshot);
    if (qualityEntries.length === 0) return null;

    const totals = qualityEntries.reduce(
      (acc, [, stat]) => {
        acc.totalBytes += stat.bytesDownloaded || 0;
        acc.totalFragments += stat.fragmentsLoaded || 0;
        return acc;
      },
      { totalBytes: 0, totalFragments: 0 }
    );

    const qualityDistribution = qualityEntries.reduce((acc, [quality, stat]) => {
      const bytesDownloaded = stat.bytesDownloaded || 0;
      const fragmentsLoaded = stat.fragmentsLoaded || 0;
      const byteSharePercent = totals.totalBytes > 0
        ? Number(((bytesDownloaded / totals.totalBytes) * 100).toFixed(2))
        : 0;

      acc[quality] = {
        bytesDownloaded,
        fragmentsLoaded,
        byteSharePercent
      };

      return acc;
    }, {});

    return qualityDistribution;
  }

  /**
   * Reads the Network Information API (where available) to capture connection
   * type, estimated downlink speed, and round-trip time.
   * Falls back to "unknown" on unsupported browsers.
   * @returns {{ connectionType: string, downlinkMbps?: number, rttMs?: number }}
   */
  _getConnectionSnapshot() {
    const connection = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
    const snapshot = {
      connectionType: connection?.effectiveType || "unknown"
    };

    if (typeof connection?.downlink === "number") {
      snapshot.downlinkMbps = connection.downlink;
    }
    if (typeof connection?.rtt === "number") {
      snapshot.rttMs = connection.rtt;
    }

    return snapshot;
  }

  /**
   * Truncates the User-Agent string to avoid oversized payloads.
   * @param {number} maxLength - Maximum allowed character length (default: 180)
   * @returns {string}
   */
  _getTrimmedUserAgent(maxLength = 180) {
    const ua = navigator.userAgent || "unknown";
    return ua.length > maxLength ? `${ua.slice(0, maxLength)}...` : ua;
  }

  /**
   * Computes cumulative QoE metrics at the point of event creation.
   * rebufferRatio = totalRebufferMs / (watchTimeSec * 1000)
   * @returns {{ totalRebufferMs, stallCount, startupStallCount, watchTimeSec, rebufferRatio }}
   */
  _getPlaybackQualityMetrics() {
    const watchTimeSec = Math.max(this.video.currentTime || 0, 0);
    return {
      totalRebufferMs: this.totalRebufferMs,
      stallCount: this.stallCount,
      startupStallCount: this.startupStallCount,
      watchTimeSec: watchTimeSec,
      rebufferRatio: watchTimeSec > 0 ? Number((this.totalRebufferMs / (watchTimeSec * 1000)).toFixed(4)) : 0
    };
  }

  /**
   * Normalises raw HLS.js error data into a structured error object.
   * Severity levels:
   *  - "fatal" : Playback cannot continue; requires player recovery
   *  - "error" : Non-fatal but notable issue
   *  - "warn"  : Internal exception; typically recoverable automatically
   *
   * @param {string}  details - HLS.js error detail code (e.g. "fragLoadError")
   * @param {string}  type    - HLS.js error type (e.g. "networkError")
   * @param {boolean} fatal   - Whether the error stopped playback
   * @returns {{ code, message, category, fatal, severity, recoverable }}
   */
  _classifyError(details, type, fatal) {
    const code = details || "unknown";
    const category = (type || "unknown").toUpperCase();
    const severity = fatal ? "fatal" : (code === "internalException" ? "warn" : "error");

    return {
      code,
      message: type || "unknown",
      category,
      fatal: !!fatal,
      severity,
      recoverable: !fatal
    };
  }

  /**
   * Adds a non-fatal error to the in-memory aggregate map.
   * Errors with the same code+message are deduplicated and their count incremented.
   * The aggregate is flushed as an ERROR_SUMMARY event on each batch send.
   * @param {{ code, message, category, severity }} errorObj
   */
  _recordNonFatalError(errorObj) {
    const key = `${errorObj.code}|${errorObj.message}`;
    if (!this.nonFatalErrorAggregate[key]) {
      this.nonFatalErrorAggregate[key] = {
        code: errorObj.code,
        message: errorObj.message,
        category: errorObj.category,
        severity: errorObj.severity,
        recoverable: true,
        count: 0,
        firstSeenTs: Date.now(),
        lastSeenTs: Date.now()
      };
    }

    this.nonFatalErrorAggregate[key].count += 1;
    this.nonFatalErrorAggregate[key].lastSeenTs = Date.now();
  }

  /**
   * Emits an ERROR_SUMMARY event containing all aggregated non-fatal errors
   * since the last flush, then resets the aggregate map.
   * No-op if there are no pending errors.
   */
  _flushNonFatalErrorSummary() {
    const summaryItems = Object.values(this.nonFatalErrorAggregate);
    if (summaryItems.length === 0) return;

    this._queueEvent("ERROR_SUMMARY", {
      errors: summaryItems,
      totals: {
        distinctCodes: summaryItems.length,
        totalCount: summaryItems.reduce((sum, item) => sum + item.count, 0)
      }
    });

    this.nonFatalErrorAggregate = {};
  }

  /**
   * Builds a full telemetry event and pushes it onto the event queue.
   * For QUALITY_CHANGE events, also attaches the dropped-frames delta.
   * For fatal ERROR events, immediately triggers a flush to avoid data loss.
   *
   * @param {string} eventType   - One of: PLAYBACK_START, BUFFER_START, BUFFER_END,
   *                               QUALITY_SWITCH_START, QUALITY_CHANGE, ERROR, ERROR_SUMMARY
   * @param {Object} payloadData - Event-specific data attached to the "payload" field
   */
  _queueEvent(eventType, payloadData = {}) {
    // Attach dropped-frames delta only when a quality switch completes
    if (eventType === "QUALITY_CHANGE") {
      payloadData.quality = payloadData.quality || {};
      payloadData.quality.droppedFramesDelta = this._getDroppedFramesDelta();
    }

    const nowTs = Date.now();
    const trafficSnapshot = this._getTrafficSnapshot();
    const qualityDistribution = this._getDistributionSnapshot(trafficSnapshot);

    // Build network context: HLS bandwidth estimate + browser connection info
    const networkSnapshot = {
      estimatedBandwidthBps: this.hls.bandwidthEstimate ? Math.round(this.hls.bandwidthEstimate) : null,
      ...this._getConnectionSnapshot()
    };

    // Only include traffic/quality breakdown when data has been accumulated
    if (Object.keys(trafficSnapshot).length > 0) {
      networkSnapshot.trafficBreakdown = trafficSnapshot;
    }
    if (qualityDistribution) {
      networkSnapshot.qualityDistribution = qualityDistribution;
    }

    const telemetryEvent = TelemetryEventSchema.build({
      schemaVersion: this.schemaVersion,
      eventId: this._generateEventId(),
      playbackSessionId: this.sessionId,
      sequenceNumber: this.sequenceNumber++,
      event_timestamp: nowTs,
      elapsedMs: nowTs - this.sessionStartTs,
      eventType: eventType,
      identities: { tenantId: this.tenantId, userId: this.userId },
      content: { manifestUrl: this.hls.url || "unknown", durationSec: this.video.duration || 0 },
      environment: {
        playerType: "hls.js",
        playerVersion: Hls.version || "unknown",
        telemetryVersion: this.schemaVersion,
        userAgent: this._getTrimmedUserAgent()
      },
      network: networkSnapshot,
      playerState: {
        currentTimeSec: this.video.currentTime,
        isPaused: this.video.paused,
        isMuted: this.video.muted,
        volume: this.video.volume
      },
      playbackQualityMetrics: this._getPlaybackQualityMetrics(),
      payload: payloadData
    });

    this.eventQueue.push(telemetryEvent);
    console.log(`%c[Telemetry Queued #${telemetryEvent.sequenceNumber}] ${eventType}`, "color: #00bcd4; font-weight: bold;", telemetryEvent);

    // Fatal errors are sent immediately rather than waiting for the batch timer
    if (eventType === "ERROR" && payloadData.error?.fatal) {
      this.flush();
    }
  }

  /**
   * Sends all queued events to the telemetry endpoint as a single JSON batch.
   * Prefers navigator.sendBeacon (fire-and-forget, survives page unload),
   * falling back to fetch with keepalive for older browsers.
   * Non-fatal error summary is always prepended before the batch is sent.
   */
  flush() {
    this._flushNonFatalErrorSummary();

    if (this.eventQueue.length === 0) return;
    const payloadToSend = JSON.stringify({ events: this.eventQueue });
    console.log("%c[Telemetry Flush] Sending batch to server...", "color: #4caf50; font-weight: bold;", this.eventQueue);
    this.eventQueue = [];

    if (navigator.sendBeacon) {
      // sendBeacon is non-blocking and works even after the page starts unloading
      navigator.sendBeacon(this.endpoint, payloadToSend);
    } else {
      fetch(this.endpoint, {
        method: "POST",
        body: payloadToSend,
        headers: { "Content-Type": "application/json" },
        keepalive: true
      }).catch(err => console.error("Telemetry fetch failed", err));
    }
  }

  /**
   * Starts the periodic flush timer and registers a pagehide listener
   * to ensure the final batch is sent when the user navigates away.
   */
  _startQueueTimer() {
    this.queueTimer = setInterval(() => this.flush(), this.batchIntervalMs);
    window.addEventListener("pagehide", () => this.flush());
  }

  /**
   * Registers all HLS.js and native video element event listeners.
   * This is the main wiring point that connects player events to telemetry events.
   */
  _initListeners() {
    // ── Fragment loaded ────────────────────────────────────────────────────────
    // Accumulates bytes and fragment count per resolution for traffic breakdown
    this.hls.on(Hls.Events.FRAG_LOADED, (event, data) => {
      const levelInfo = this.hls.levels[data.frag.level];
      if (!levelInfo) return;
      const resolution = `${levelInfo.width}x${levelInfo.height}`;
      const bytes = data.stats.total;

      if (!this.trafficStats[resolution]) {
        this.trafficStats[resolution] = { bytesDownloaded: 0, fragmentsLoaded: 0 };
      }
      this.trafficStats[resolution].bytesDownloaded += bytes;
      this.trafficStats[resolution].fragmentsLoaded += 1;
    });

    // ── First frame / playback resume ──────────────────────────────────────────
    // Fires on initial play and after each rebuffer; guarded by isStarted flag
    this.video.addEventListener("playing", () => {
      if (!this.isStarted) {
        this.isStarted = true;
        this.playbackStartTs = Date.now();
        this._queueEvent("PLAYBACK_START", {
          startup: {
            // Time from tracker init to first frame visible to the user
            timeToFirstFrameMs: this.playbackStartTs - this.sessionStartTs
          }
        });
        if (this.hls.currentLevel !== -1) {
          const currentLevelInfo = this.hls.levels[this.hls.currentLevel];
          this.previousQuality = {
            bitrateBps: currentLevelInfo?.bitrate || null,
            resolution: currentLevelInfo ? `${currentLevelInfo.width}x${currentLevelInfo.height}` : null
          };
        }
      }
    });

    // ── ABR quality switch started ─────────────────────────────────────────────
    // Captures the target level before the switch completes
    this.hls.on(Hls.Events.LEVEL_SWITCHING, (event, data) => {
      const targetLevelInfo = this.hls.levels[data.level];
      if (targetLevelInfo && !this.qualitySwitchStartTime) {
        this.qualitySwitchStartTime = Date.now();
        this._queueEvent("QUALITY_SWITCH_START", {
          quality: {
            targetBitrateBps: targetLevelInfo.bitrate,
            targetResolution: `${targetLevelInfo.width}x${targetLevelInfo.height}`
          }
        });
      }
    });

    // ── ABR quality switch completed ───────────────────────────────────────────
    // Records the new level, switch duration, and updates previousQuality baseline
    this.hls.on(Hls.Events.LEVEL_SWITCHED, (event, data) => {
      const levelInfo = this.hls.levels[data.level];
      if (levelInfo) {
        let duration = null;
        if (this.qualitySwitchStartTime) {
          duration = Date.now() - this.qualitySwitchStartTime;
          this.qualitySwitchStartTime = null;
        }

        this._queueEvent("QUALITY_CHANGE", {
          quality: {
            bitrateBps: levelInfo.bitrate,
            resolution: `${levelInfo.width}x${levelInfo.height}`,
            fromBitrateBps: this.previousQuality.bitrateBps,
            fromResolution: this.previousQuality.resolution,
            switchDurationMs: duration
          }
        });

        this.previousQuality = {
          bitrateBps: levelInfo.bitrate,
          resolution: `${levelInfo.width}x${levelInfo.height}`
        };
      }
    });

    // ── Seek tracking ──────────────────────────────────────────────────────────
    // isSeeking flag changes _getBufferType() from STALL → SEEK
    this.video.addEventListener("seeking", () => { this.isSeeking = true; });
    this.video.addEventListener("seeked",  () => { this.isSeeking = false; });

    // ── Buffering started ──────────────────────────────────────────────────────
    // Fires when the player runs out of buffered data
    this.video.addEventListener("waiting", () => {
      this.bufferStartTime = Date.now();
      const bufferType = this._getBufferType();
      if (bufferType === "STARTUP") {
        this.startupStallCount += 1;
      } else if (bufferType === "STALL") {
        // Mid-playback buffer underrun
        this.stallCount += 1;
      }
      this._queueEvent("BUFFER_START", { buffer: { type: bufferType } });
    });

    // ── Buffering ended ────────────────────────────────────────────────────────
    // Fires on "playing" (rebuffer resolved) and "seeked" (seek buffer resolved)
    const handleBufferEnd = () => {
      if (this.bufferStartTime) {
        const duration = Date.now() - this.bufferStartTime;
        const type = this._getBufferType();
        this.bufferStartTime = null;
        this.totalRebufferMs += duration; // accumulate total rebuffer time
        this._queueEvent("BUFFER_END", {
          buffer: {
            type: type,
            durationMs: duration,
            totalRebufferMs: this.totalRebufferMs
          }
        });
      }
    };
    this.video.addEventListener("playing", handleBufferEnd);
    this.video.addEventListener("seeked",  handleBufferEnd);

    // ── HLS error handling ─────────────────────────────────────────────────────
    // Fatal errors are queued and flushed immediately
    // Non-fatal errors are aggregated and sent in the next batch as ERROR_SUMMARY
    this.hls.on(Hls.Events.ERROR, (event, data) => {
      const errorObj = this._classifyError(data.details, data.type, data.fatal);

      if (errorObj.fatal) {
        this._queueEvent("ERROR", { error: errorObj });
      } else {
        this._recordNonFatalError(errorObj);
      }
    });
  }
}

// Expose on window so index.html inline scripts can instantiate the tracker
window.AdvancedVideoTelemetry = AdvancedVideoTelemetry;
