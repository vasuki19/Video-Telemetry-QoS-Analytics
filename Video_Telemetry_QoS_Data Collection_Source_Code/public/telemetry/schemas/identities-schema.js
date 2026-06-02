class TelemetryIdentitiesSchema {
  static defaults() {
    return {
      tenantId: "unknown-tenant",
      userId: "anonymous_user"
    };
  }

  static build(data = {}) {
    const defaults = TelemetryIdentitiesSchema.defaults();

    return {
      tenantId: data.tenantId || defaults.tenantId,
      userId: data.userId || defaults.userId
    };
  }
}

window.TelemetryIdentitiesSchema = TelemetryIdentitiesSchema;
