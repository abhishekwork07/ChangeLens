package io.changelens.core.context;

public record AuditContext(
        String applicationName,
        String applicationVersion,
        String serviceName,
        String environment,
        String requestId,
        String correlationId,
        String traceId,
        String sourceIp,
        String userAgent
) {
}