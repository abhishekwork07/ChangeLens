package io.changelens.demo.provider;

import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.actor.ActorType;
import io.changelens.sdk.audit.provider.AuditActorProvider;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

@Component
@Primary
public class DemoAuditActorProvider
        implements AuditActorProvider {

    @Override
    public Actor getActor() {

        return new Actor(
                ActorType.USER,
                "demo-user-id",
                "Demo User"
        );
    }
}