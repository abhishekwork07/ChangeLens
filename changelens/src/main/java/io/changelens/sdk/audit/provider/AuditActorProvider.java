package io.changelens.sdk.audit.provider;

import io.changelens.core.domain.actor.Actor;

public interface AuditActorProvider {

    Actor getActor();
}
