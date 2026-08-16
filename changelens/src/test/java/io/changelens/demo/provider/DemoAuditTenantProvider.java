package io.changelens.demo.provider;
import io.changelens.sdk.audit.provider.AuditTenantProvider;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
public class DemoAuditTenantProvider
        implements AuditTenantProvider {

    @Override
    public String getTenantId() {
        return "demo-tenant-id";
    }
}
