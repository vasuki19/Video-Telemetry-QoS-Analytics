class TelemetryEventSchema {
  static defaults() {
    if (typeof TelemetryQualityDistributionSchema === "undefined") {
      throw new Error("TelemetryQualityDistributionSchema is not loaded.");
    }
    if (typeof TelemetryNetworkSchema === "undefined") {
      throw new Error("TelemetryNetworkSchema is not loaded.");
    }
    if (typeof TelemetryPlayerStateSchema === "undefined") {
      throw new Error("TelemetryPlayerStateSchema is not loaded.");
    }
    if (typeof TelemetryPlaybackQualityMetricsSchema === "undefined") {
      throw new Error("TelemetryPlaybackQualityMetricsSchema is not loaded.");
    }
    if (typeof TelemetryIdentitiesSchema === "undefined") {
      throw new Error("TelemetryIdentitiesSchema is not loaded.");
    }
    if (typeof TelemetryContentSchema === "undefined") {
      throw new Error("TelemetryContentSchema is not loaded.");
    }
    if (typeof TelemetryEnvironmentSchema === "undefined") {
      throw new Error("TelemetryEnvironmentSchema is not loaded.");
    }

    return {
      schemaVersion: "unknown",
      eventId: "unknown-event-id",
      playbackSessionId: "unknown-session",
      sequenceNumber: 0,
      event_timestamp: Date.now(),
      elapsedMs: 0,
      eventType: "UNKNOWN_EVENT",
      identities: TelemetryIdentitiesSchema.defaults(),
      content: TelemetryContentSchema.defaults(),
      environment: TelemetryEnvironmentSchema.defaults(),
      network: TelemetryNetworkSchema.defaults(),
      playerState: TelemetryPlayerStateSchema.defaults(),
      playbackQualityMetrics: TelemetryPlaybackQualityMetricsSchema.defaults(),
      payload: {}
    };
  }

  static build(data = {}) {
    const defaults = TelemetryEventSchema.defaults();

    return {
      ...defaults,
      schemaVersion: data.schemaVersion || defaults.schemaVersion,
      eventId: data.eventId || defaults.eventId,
      playbackSessionId: data.playbackSessionId || defaults.playbackSessionId,
      sequenceNumber: Number.isFinite(data.sequenceNumber) ? data.sequenceNumber : defaults.sequenceNumber,
      event_timestamp: Number.isFinite(data.event_timestamp) ? data.event_timestamp : defaults.event_timestamp,
      elapsedMs: Number.isFinite(data.elapsedMs) ? data.elapsedMs : defaults.elapsedMs,
      eventType: data.eventType || defaults.eventType,
      identities: TelemetryIdentitiesSchema.build(data.identities),
      content: TelemetryContentSchema.build(data.content),
      environment: TelemetryEnvironmentSchema.build({
        ...data.environment,
        telemetryVersion: data.environment?.telemetryVersion || data.schemaVersion || defaults.environment.telemetryVersion
      }),
      network: TelemetryNetworkSchema.build(data.network),
      playerState: TelemetryPlayerStateSchema.build(data.playerState),
      playbackQualityMetrics: TelemetryPlaybackQualityMetricsSchema.build(data.playbackQualityMetrics),
      payload: data.payload && typeof data.payload === "object" ? data.payload : defaults.payload
    };
  }
}

window.TelemetryEventSchema = TelemetryEventSchema;
