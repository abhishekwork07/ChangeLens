package io.changelens.sdk.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "changelens")
public class ChangeLensProperties {

    private boolean enabled = true;

    private Application application = new Application();

    private Redis redis = new Redis();

    private Outbox outbox = new Outbox();

    public record Application(
            String name,
            String version,
            String serviceName,
            String environment
    ) {
        public Application() {
            this(
                    "ChangeLens",
                    "unknown",
                    "unknown",
                    "unknown"
            );
        }
    }

    public record Redis(
            Duration processedEventTtl
    ) {
        public Redis() {
            this(Duration.ofHours(24));
        }
    }

    public record Outbox(
            int batchSize,
            Duration publisherFixedDelay
    ) {
        public Outbox() {
            this(
                    100,
                    Duration.ofSeconds(5)
            );
        }
    }
}
