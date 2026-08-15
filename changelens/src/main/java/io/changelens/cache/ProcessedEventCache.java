package io.changelens.cache;

import java.util.UUID;

public interface ProcessedEventCache {

    boolean contains(UUID eventId);

    void put(UUID eventId);

    void evict(UUID eventId);
}
