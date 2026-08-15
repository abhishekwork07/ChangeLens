package io.changelens.cache;


import io.changelens.support.IntegrationTestContainers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(IntegrationTestContainers.class)
public class RedisIntegrationTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void shouldConnectToRedis() {

        String key = "changelens:test";
        String value = "connected";

        redisTemplate.opsForValue().set(key, value);

        assertThat(
                redisTemplate.opsForValue().get(key)
        ).isEqualTo(value);

        redisTemplate.delete(key);
    }
}
