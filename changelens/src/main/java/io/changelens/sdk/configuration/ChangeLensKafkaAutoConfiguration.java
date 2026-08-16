package io.changelens.sdk.configuration;

import io.changelens.outbox.kafka.KafkaEventProducer;
import io.changelens.outbox.publisher.OutboxPublisher;
import io.changelens.outbox.publisher.OutboxPublishingScheduler;
import io.changelens.outbox.publisher.service.OutboxClaimService;
import io.changelens.outbox.publisher.service.OutboxStatusService;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import org.springframework.kafka.core.KafkaTemplate;

@AutoConfiguration
@ConditionalOnProperty(
        prefix = "changelens",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
@ConditionalOnClass(KafkaTemplate.class)
public class ChangeLensKafkaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public OutboxPublisher outboxPublisher(
            OutboxClaimService claimService,
            KafkaEventProducer kafkaEventProducer,
            OutboxStatusService statusService) {

        return new OutboxPublisher(
                claimService,
                kafkaEventProducer,
                statusService
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxPublishingScheduler outboxPublishingScheduler(
            OutboxPublisher outboxPublisher,
            ChangeLensProperties properties) {

        return new OutboxPublishingScheduler(
                outboxPublisher,
                properties.getOutbox().batchSize()
        );
    }
}
