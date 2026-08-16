package io.changelens.sdk.entity.hibernate;

import jakarta.annotation.PostConstruct;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;

import java.util.Objects;

@RequiredArgsConstructor
public class AuditHibernateEventListenerRegistrar {

    private final EntityManagerFactory entityManagerFactory;
    private final AuditPostUpdateEventListener listener;

    @PostConstruct
    public void register() {

        SessionFactoryImplementor sessionFactory =
                entityManagerFactory.unwrap(
                        SessionFactoryImplementor.class
                );

        EventListenerRegistry registry =
                sessionFactory
                        .getServiceRegistry()
                        .getService(
                                EventListenerRegistry.class
                        );

        Objects.requireNonNull(registry)
                .getEventListenerGroup(
                        EventType.POST_UPDATE
                )
                .appendListener(listener);
    }
}
