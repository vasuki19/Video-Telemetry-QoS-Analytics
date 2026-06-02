class TelemetryPlaybackQualityMetricsSchema {
  static defaults() {
    return {
      totalRebufferMs: 0,
      stallCount: 0,
      startupStallCount: 0,
      watchTimeSec: 0,
      rebufferRatio: 0
    };
  }

  static build(data = {}) {
    const defaults = TelemetryPlaybackQualityMetricsSchema.defaults();

    return {
      totalRebufferMs: Number.isFinite(data.totalRebufferMs) ? data.totalRebufferMs : defaults.totalRebufferMs,
      stallCount: Number.isFinite(data.stallCount) ? data.stallCount : defaults.stallCount,
      startupStallCount: Number.isFinite(data.startupStallCount) ? data.startupStallCount : defaults.startupStallCount,
      watchTimeSec: Number.isFinite(data.watchTimeSec) ? data.watchTimeSec : defaults.watchTimeSec,
      rebufferRatio: Number.isFinite(data.rebufferRatio) ? data.rebufferRatio : defaults.rebufferRatio
    };
  }
}

window.TelemetryPlaybackQualityMetricsSchema = TelemetryPlaybackQualityMetricsSchema;
