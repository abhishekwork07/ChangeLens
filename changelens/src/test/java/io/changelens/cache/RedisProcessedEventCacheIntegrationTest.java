package io.changelens.cache;

import io.changelens.support.IntegrationTestContainers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class RedisProcessedEventCacheIntegrationTest {

    private static final String KEY_PREFIX = "changelens:processed:";

    @Autowired
    private ProcessedEventCache processedEventCache;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @AfterEach
    void cleanUp() {

        /*
         * Remove all keys created by this test class.
         */
        var keys = redisTemplate.keys(
                KEY_PREFIX + "*"
        );

        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
    }

    @Test
    void shouldReturnFalseWhenEventIsNotCached() {

        UUID eventId = UUID.randomUUID();

        assertThat(
                processedEventCache.contains(eventId)
        ).isFalse();
    }

    @Test
    void shouldReturnTrueWhenProcessedEventIsCached() {

        UUID eventId = UUID.randomUUID();

        processedEventCache.put(eventId);

        assertThat(
                processedEventCache.contains(eventId)
        ).isTrue();
    }

    @Test
    void shouldEvictProcessedEvent() {

        UUID eventId = UUID.randomUUID();

        processedEventCache.put(eventId);

        assertThat(
                processedEventCache.contains(eventId)
        ).isTrue();

        processedEventCache.evict(eventId);

        assertThat(
                processedEventCache.contains(eventId)
        ).isFalse();
    }

    @Test
    void shouldStoreProcessedEventWithTtl() {

        UUID eventId = UUID.randomUUID();

        processedEventCache.put(eventId);

        String key = KEY_PREFIX + eventId;

        Long ttl =
                redisTemplate.getExpire(
                        key,
                        java.util.concurrent.TimeUnit.SECONDS
                );

        assertThat(ttl)
                .isPositive();
    }
}
