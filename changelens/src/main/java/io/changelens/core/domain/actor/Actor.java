package io.changelens.core.domain.actor;

public record Actor(
        ActorType type,
        String id,
        String name
) {
}
