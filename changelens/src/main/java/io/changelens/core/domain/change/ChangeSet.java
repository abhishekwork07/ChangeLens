package io.changelens.core.domain.change;

import java.util.Collection;
import java.util.List;

public record ChangeSet(
        String summary,
        Collection<AuditChange> changes
) {

    public ChangeSet {
        changes = changes == null
                ? List.of()
                : List.copyOf(changes);
    }
}
