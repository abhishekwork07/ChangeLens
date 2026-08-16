package io.changelens.demo.repository;

import io.changelens.demo.entity.DemoCustomer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DemoCustomerRepository
        extends JpaRepository<DemoCustomer, Long> {
}