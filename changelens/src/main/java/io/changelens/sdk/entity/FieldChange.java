package io.changelens.sdk.entity;

public record FieldChange(
        String fieldName,
        Object previousValue,
        Object currentValue
) {
}
