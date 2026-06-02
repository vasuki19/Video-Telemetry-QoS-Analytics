class TelemetryEnvironmentSchema {
  static defaults() {
    return {
      playerType: "hls.js",
      playerVersion: "unknown",
      telemetryVersion: "unknown",
      userAgent: "unknown"
    };
  }

  static build(data = {}) {
    const defaults = TelemetryEnvironmentSchema.defaults();

    return {
      playerType: data.playerType || defaults.playerType,
      playerVersion: data.playerVersion || defaults.playerVersion,
      telemetryVersion: data.telemetryVersion || defaults.telemetryVersion,
      userAgent: data.userAgent || defaults.userAgent
    };
  }
}

window.TelemetryEnvironmentSchema = TelemetryEnvironmentSchema;
