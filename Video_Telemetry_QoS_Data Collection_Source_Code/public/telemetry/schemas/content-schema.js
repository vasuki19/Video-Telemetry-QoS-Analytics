class TelemetryContentSchema {
  static defaults() {
    return {
      manifestUrl: "unknown",
      durationSec: 0
    };
  }

  static build(data = {}) {
    const defaults = TelemetryContentSchema.defaults();

    return {
      manifestUrl: data.manifestUrl || defaults.manifestUrl,
      durationSec: Number.isFinite(data.durationSec) ? data.durationSec : defaults.durationSec
    };
  }
}

window.TelemetryContentSchema = TelemetryContentSchema;
