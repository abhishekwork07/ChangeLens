package io.changelens.cache;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;

import java.util.UUID;

@ConditionalOnMissingBean(ProcessedEventCache.class)
public class NoOpProcessedEventCache
        implements ProcessedEventCache {

    @Override
    public boolean contains(UUID eventId) {
        return false;
    }

    @Override
    public void put(UUID eventId) {
        // Database remains the source of truth.
    }

    @Override
    public void evict(UUID eventId) {
        // Nothing to evict.
    }
}