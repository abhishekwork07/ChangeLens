package io.changelens.processing.processor;

import tools.jackson.core.JacksonException;

public class AuditDeserializationException extends RuntimeException {

    public AuditDeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
