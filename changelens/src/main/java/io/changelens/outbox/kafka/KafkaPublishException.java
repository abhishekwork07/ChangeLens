package io.changelens.outbox.kafka;

public class KafkaPublishException extends RuntimeException {

    public KafkaPublishException(String kafkaPublishingInterrupted, Exception e) {
        super(e.getMessage());
    }
}
