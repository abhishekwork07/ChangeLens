package io.changelens.sdk.audit.provider;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;


@ConditionalOnMissingBean(AuditTenantProvider.class)
public class DefaultAuditTenantProvider implements AuditTenantProvider {

    @Override
    public String getTenantId() {
        return "defaultId";
    }
}