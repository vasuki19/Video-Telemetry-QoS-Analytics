class TelemetryNetworkSchema {
  static defaults() {
    return {
      estimatedBandwidthBps: null,
      connectionType: "unknown",
      downlinkMbps: null,
      rttMs: null,
      trafficBreakdown: {},
      qualityDistribution: TelemetryQualityDistributionSchema.defaults()
    };
  }

  static build(data = {}) {
    const defaults = TelemetryNetworkSchema.defaults();

    return {
      ...defaults,
      estimatedBandwidthBps: Number.isFinite(data.estimatedBandwidthBps)
        ? data.estimatedBandwidthBps
        : defaults.estimatedBandwidthBps,
      connectionType: data.connectionType || defaults.connectionType,
      downlinkMbps: typeof data.downlinkMbps === "number" ? data.downlinkMbps : defaults.downlinkMbps,
      rttMs: typeof data.rttMs === "number" ? data.rttMs : defaults.rttMs,
      trafficBreakdown:
        data.trafficBreakdown && typeof data.trafficBreakdown === "object"
          ? data.trafficBreakdown
          : defaults.trafficBreakdown,
      qualityDistribution: TelemetryQualityDistributionSchema.build(data.qualityDistribution)
    };
  }
}

window.TelemetryNetworkSchema = TelemetryNetworkSchema;
