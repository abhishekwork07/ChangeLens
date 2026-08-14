package io.changelens.outbox.publisher;

public class OutboxStateTransitionException extends RuntimeException {

    public OutboxStateTransitionException(String message) {
        super(message);
    }
}
