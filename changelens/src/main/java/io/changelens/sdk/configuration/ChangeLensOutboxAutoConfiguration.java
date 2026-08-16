package io.changelens.sdk.configuration;

import io.changelens.outbox.publisher.OutboxAuditEventPublisher;
import io.changelens.outbox.publisher.service.OutboxEventWriter;
import io.changelens.outbox.repository.OutboxEventRepository;
import io.changelens.sdk.audit.AuditEventPublisher;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import tools.jackson.databind.json.JsonMapper;

@AutoConfiguration(
        after = ChangeLensJpaAutoConfiguration.class
)
@ConditionalOnProperty(
        prefix = "changelens",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
public class ChangeLensOutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OutboxEventWriter outboxEventWriter(
            OutboxEventRepository repository,
            JsonMapper jsonMapper) {

        return new OutboxEventWriter(
                repository,
                jsonMapper
        );
    }

    @Bean
    @ConditionalOnMissingBean(AuditEventPublisher.class)
    public AuditEventPublisher auditEventPublisher(
            OutboxEventWriter outboxEventWriter) {

        return new OutboxAuditEventPublisher(
                outboxEventWriter
        );
    }
}