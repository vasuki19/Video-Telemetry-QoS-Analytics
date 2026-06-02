class TelemetryPlayerStateSchema {
  static defaults() {
    return {
      currentTimeSec: 0,
      isPaused: false,
      isMuted: false,
      volume: 1
    };
  }

  static build(data = {}) {
    const defaults = TelemetryPlayerStateSchema.defaults();

    return {
      currentTimeSec: Number.isFinite(data.currentTimeSec) ? data.currentTimeSec : defaults.currentTimeSec,
      isPaused: typeof data.isPaused === "boolean" ? data.isPaused : defaults.isPaused,
      isMuted: typeof data.isMuted === "boolean" ? data.isMuted : defaults.isMuted,
      volume: Number.isFinite(data.volume) ? data.volume : defaults.volume
    };
  }
}

window.TelemetryPlayerStateSchema = TelemetryPlayerStateSchema;
