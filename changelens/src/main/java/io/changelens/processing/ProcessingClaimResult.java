package io.changelens.processing;

public enum ProcessingClaimResult {
    STARTED,
    ALREADY_PROCESSED,
    ALREADY_PROCESSING,
    RETRY_STARTED,
    MAX_ATTEMPTS_REACHED
}
