package io.changelens.processing.service;

public class AuditProcessingException extends RuntimeException {

    public AuditProcessingException(String message) {
        super(message);
    }

    public AuditProcessingException(String message, Exception ex) {
        super(message);
    }
}
