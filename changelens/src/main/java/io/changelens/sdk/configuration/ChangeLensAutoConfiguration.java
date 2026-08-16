package io.changelens.sdk.configuration;

import io.changelens.sdk.aspect.AuditAnnotationResolver;
import io.changelens.sdk.aspect.AuditAspect;
import io.changelens.sdk.audit.DefaultAuditEventFactory;
import io.changelens.sdk.audit.provider.*;
import io.changelens.sdk.context.AuditContextProvider;
import io.changelens.sdk.context.DefaultAuditContextProvider;
import io.changelens.sdk.entity.AuditEntityMetadataResolver;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@AutoConfiguration
@EnableConfigurationProperties(ChangeLensProperties.class)
@ConditionalOnProperty(
        prefix = "changelens",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
@Import({
        AuditAspect.class,
        AuditAnnotationResolver.class,
        DefaultAuditEventFactory.class,
        AuditEntityMetadataResolver.class
})
public class ChangeLensAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(AuditContextProvider.class)
    public AuditContextProvider auditContextProvider() {
        return new DefaultAuditContextProvider();
    }

    @Bean
    @ConditionalOnMissingBean(AuditActorProvider.class)
    public AuditActorProvider auditActorProvider() {
        return new DefaultAuditActorProvider();
    }

    @Bean
    @ConditionalOnMissingBean(AuditTenantProvider.class)
    public AuditTenantProvider auditTenantProvider() {
        return new DefaultAuditTenantProvider();
    }

    @Bean
    @ConditionalOnMissingBean(AuditResourceProvider.class)
    public AuditResourceProvider auditResourceProvider() {
        return new DefaultAuditResourceProvider();
    }
}
