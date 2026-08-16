package io.changelens.demo.dto;

public record UpdateCustomerRequest(
        String name,
        String email,
        String status
) {
}