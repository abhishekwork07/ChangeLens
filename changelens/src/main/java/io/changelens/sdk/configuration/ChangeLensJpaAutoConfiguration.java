package io.changelens.sdk.configuration;

import io.changelens.sdk.audit.AuditEventFactory;
import io.changelens.sdk.audit.AuditEventPublisher;
import io.changelens.sdk.entity.AuditEntityMetadataResolver;
import io.changelens.sdk.entity.hibernate.AuditHibernateEventListenerRegistrar;
import io.changelens.sdk.entity.hibernate.AuditPostUpdateEventListener;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.event.spi.PostUpdateEventListener;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnProperty(
        prefix = "changelens",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
@ConditionalOnClass({
        EntityManagerFactory.class,
        PostUpdateEventListener.class
})
@ConditionalOnBean(EntityManagerFactory.class)
public class ChangeLensJpaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public AuditPostUpdateEventListener auditPostUpdateEventListener(
            AuditEntityMetadataResolver metadataResolver,
            AuditEventFactory auditEventFactory,
            AuditEventPublisher auditEventPublisher) {

        return new AuditPostUpdateEventListener(
                metadataResolver,
                auditEventFactory,
                auditEventPublisher
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public AuditHibernateEventListenerRegistrar
    auditHibernateEventListenerRegistrar(
            EntityManagerFactory entityManagerFactory,
            AuditPostUpdateEventListener listener) {

        return new AuditHibernateEventListenerRegistrar(
                entityManagerFactory,
                listener
        );
    }
}