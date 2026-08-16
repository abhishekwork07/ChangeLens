package io.changelens.support;

import io.changelens.sdk.configuration.*;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
@Import({
        ChangeLensAutoConfiguration.class,
        ChangeLensJpaAutoConfiguration.class,
        ChangeLensRedisAutoConfiguration.class,
        ChangeLensOutboxAutoConfiguration.class,
        ChangeLensKafkaAutoConfiguration.class
})
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
}