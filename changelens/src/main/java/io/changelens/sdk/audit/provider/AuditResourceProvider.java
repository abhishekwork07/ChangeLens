package io.changelens.sdk.audit.provider;

import io.changelens.core.domain.resource.Resource;
import io.changelens.sdk.audit.AuditCaptureContext;

public interface AuditResourceProvider {

    Resource getResource(AuditCaptureContext context);
}
