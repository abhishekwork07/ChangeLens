package io.changelens.cache;

import org.springframework.data.redis.RedisConnectionFailureException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.UUID;

@Slf4j
public class RedisProcessedEventCache implements ProcessedEventCache {

    private static final String KEY_PREFIX = "changelens:processed:";

    private final StringRedisTemplate redisTemplate;
    private final Duration ttl;

    public RedisProcessedEventCache(StringRedisTemplate redisTemplate,
            @Value("${changelens.redis.processed-event-ttl:24h}") Duration ttl) {

        this.redisTemplate = redisTemplate;
        this.ttl = ttl;
    }

    @Override
    public boolean contains(UUID eventId) {

        try {
            return Boolean.TRUE.equals(
                    redisTemplate.hasKey(buildKey(eventId))
            );
        } catch (RedisConnectionFailureException exception) {
            log.warn("Redis unavailable while checking processed event {}. " +
                            "Falling back to database.", eventId, exception);

            return false;
        }
    }

    @Override
    public void put(UUID eventId) {
        try {
            redisTemplate.opsForValue()
                    .set(buildKey(eventId), "1", ttl);
        } catch (RedisConnectionFailureException exception) {
            log.warn("Unable to cache processed event {} in Redis. " +
                            "Database remains the source of truth.", eventId, exception);
        }
    }

    @Override
    public void evict(UUID eventId) {
        try {
            redisTemplate.delete(buildKey(eventId));
        } catch (RedisConnectionFailureException exception) {
            log.warn("Unable to evict processed event {} from Redis.",
                    eventId, exception);
        }
    }

    private String buildKey(UUID eventId) {
        return KEY_PREFIX + eventId;
    }
}
