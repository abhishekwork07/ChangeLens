package io.changelens.support;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

@TestConfiguration(proxyBeanMethods = false)
public class IntegrationTestContainers {

    @Bean
    PostgreSQLContainer<?> postgresContainer() {
        PostgreSQLContainer<?> container =
                new PostgreSQLContainer<>("postgres:17");

        container.start();

        return container;
    }

    @Bean
    public KafkaContainer kafkaContainer() {
        DockerImageName kafkaImage =
                DockerImageName.parse("apache/kafka:4.0.0")
                        .asCompatibleSubstituteFor("confluentinc/cp-kafka");

        return new KafkaContainer(kafkaImage);
    }

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
}