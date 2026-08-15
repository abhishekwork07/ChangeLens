package io.changelens.processing.consumer.config;

import io.changelens.processing.consumer.AuditEventRecoveryHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerErrorHandler {

    private final AuditEventRecoveryHandler recoveryHandler;

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {

        FixedBackOff backOff =
                new FixedBackOff(
                        1000L,
                        2L
                );

        return new DefaultErrorHandler(recoveryHandler, backOff);
    }
}
