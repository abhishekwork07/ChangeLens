package io.changelens.demo.entity;

import io.changelens.sdk.annotation.Audit;
import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "demo_customer")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Audit(
        action = "UPDATE",
        resource = "CUSTOMER"
)
public class DemoCustomer {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, unique = true)
    private String email;

    @Column(nullable = false)
    private String status;
}