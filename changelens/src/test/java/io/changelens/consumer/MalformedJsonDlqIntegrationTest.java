package io.changelens.consumer;

import io.changelens.core.enums.DlqStatusType;
import io.changelens.storage.entity.AuditDlqEntity;
import io.changelens.storage.repository.AuditDlqRepository;
import io.changelens.support.IntegrationTestContainers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "changelens.kafka.topics.audit-events=audit-events",
        "changelens.kafka.consumer.group-id=changelens-malformed-test"
})
@EmbeddedKafka(
        partitions = 1,
        topics = "audit-events"
)
@Import(IntegrationTestContainers.class)
class MalformedJsonDlqIntegrationTest {

    private static final String TOPIC = "audit-events";

    @TestConfiguration
    static class TestKafkaConfig {

        @Bean
        @Primary
        ConsumerFactory<String, String> testConsumerFactory(
                KafkaProperties kafkaProperties) {

            Map<String, Object> props =
                    kafkaProperties.buildConsumerProperties();

            return new DefaultKafkaConsumerFactory<>(
                    props,
                    new StringDeserializer(),
                    new StringDeserializer()
            );
        }

        @Bean
        ProducerFactory<String, String> malformedJsonProducerFactory(
                KafkaProperties kafkaProperties) {

            Map<String, Object> props =
                    kafkaProperties.buildProducerProperties();

            return new DefaultKafkaProducerFactory<>(
                    props,
                    new StringSerializer(),
                    new StringSerializer()
            );
        }

        @Bean
        KafkaTemplate<String, String> malformedJsonKafkaTemplate(
                @Qualifier("malformedJsonProducerFactory")
                ProducerFactory<String, String> producerFactory) {

            return new KafkaTemplate<>(producerFactory);
        }
    }

    @Autowired
    @Qualifier("malformedJsonKafkaTemplate")
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private AuditDlqRepository auditDlqRepository;

    @BeforeEach
    void setUp() {
        auditDlqRepository.deleteAll();
    }

    @Test
    void shouldMoveMalformedPayloadToRawDlq() throws Exception {

        String malformedPayload =
                "{\"eventId\":\"not-valid-json\"";

        kafkaTemplate
                .send(
                        TOPIC,
                        "malformed-event",
                        malformedPayload
                )
                .get();

        await()
                .atMost(Duration.ofSeconds(15))
                .untilAsserted(() ->
                        assertThat(auditDlqRepository.findAll())
                                .isNotEmpty()
                );

        AuditDlqEntity dlq =
                auditDlqRepository.findAll()
                        .stream()
                        .findFirst()
                        .orElseThrow();

        assertThat(dlq.getStatus())
                .isEqualTo(DlqStatusType.FAILED);

        assertThat(dlq.getPayload())
                .isEqualTo(malformedPayload);

        assertThat(dlq.getErrorMessage())
                .isNotBlank();

        assertThat(dlq.getFailedAt())
                .isNotNull();
    }
}