class TelemetryQualityDistributionSchema {
  static defaults() {
    return {};
  }

  static build(data = {}) {
    if (!data || typeof data !== "object") {
      return TelemetryQualityDistributionSchema.defaults();
    }

    return Object.entries(data).reduce((acc, [quality, entry]) => {
      acc[quality] = {
        bytesDownloaded: Number.isFinite(entry?.bytesDownloaded) ? entry.bytesDownloaded : 0,
        fragmentsLoaded: Number.isFinite(entry?.fragmentsLoaded) ? entry.fragmentsLoaded : 0,
        byteSharePercent: Number.isFinite(entry?.byteSharePercent) ? entry.byteSharePercent : 0
      };
      return acc;
    }, {});
  }
}

window.TelemetryQualityDistributionSchema = TelemetryQualityDistributionSchema;
