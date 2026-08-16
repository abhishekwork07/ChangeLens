package io.changelens.demo.dto;

public record CreateCustomerRequest(
        String name,
        String email
) {
}