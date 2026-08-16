package io.changelens.demo.provider;


import io.changelens.core.context.AuditContext;
import io.changelens.sdk.context.AuditContextProvider;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
public class DemoAuditContextProvider
        implements AuditContextProvider {

    @Override
    public AuditContext getContext() {

        return new AuditContext(
                "ChangeLens Demo Application",
                "1.0.0",
                "demo-service",
                "demo",
                "demo-request",
                "demo-correlation",
                "demo-trace",
                "127.0.0.1",
                "ChangeLens-Demo"
        );
    }
}
