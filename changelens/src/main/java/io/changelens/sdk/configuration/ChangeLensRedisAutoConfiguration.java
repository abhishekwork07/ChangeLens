package io.changelens.sdk.configuration;

import io.changelens.cache.NoOpProcessedEventCache;
import io.changelens.cache.ProcessedEventCache;
import io.changelens.cache.RedisProcessedEventCache;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;


@AutoConfiguration()
@ConditionalOnProperty(
        prefix = "changelens",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
@ConditionalOnClass(StringRedisTemplate.class)
public class ChangeLensRedisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ProcessedEventCache.class)
    public ProcessedEventCache processedEventCache(
            StringRedisTemplate redisTemplate,
            ChangeLensProperties properties) {

        return new RedisProcessedEventCache(
                redisTemplate,
                properties.getRedis()
                        .processedEventTtl()
        );
    }

    @Bean
    @ConditionalOnMissingBean(ProcessedEventCache.class)
    public ProcessedEventCache noOpProcessedEventCache() {
        return new NoOpProcessedEventCache();
    }
}