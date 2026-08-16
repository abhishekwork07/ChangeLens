package io.changelens.sdk.audit.provider;

import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.actor.ActorType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;


@ConditionalOnMissingBean(AuditActorProvider.class)
public class DefaultAuditActorProvider implements AuditActorProvider{

    @Override
    public Actor getActor() {
        return new Actor(ActorType.SYSTEM,
                "systemId", "SystemName");
    }
}
