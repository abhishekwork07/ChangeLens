package io.changelens.demo.service;

import io.changelens.demo.dto.CreateCustomerRequest;
import io.changelens.demo.dto.UpdateCustomerRequest;
import io.changelens.demo.entity.DemoCustomer;
import io.changelens.demo.repository.DemoCustomerRepository;
import io.changelens.sdk.annotation.Audit;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Audit(
        action = "UPDATE",
        resource = "CUSTOMER"
)
public class DemoCustomerService {

    private final DemoCustomerRepository customerRepository;

    @Transactional
    @Audit(
            action = "CREATE",
            resource = "CUSTOMER"
    )
    public DemoCustomer createCustomer(
            CreateCustomerRequest request) {

        DemoCustomer customer =
                DemoCustomer.builder()
                        .name(request.name())
                        .email(request.email())
                        .status("ACTIVE")
                        .build();

        return customerRepository.save(customer);
    }

    @Transactional
    public DemoCustomer updateCustomer(
            Long id,
            UpdateCustomerRequest request) {

        DemoCustomer customer =
                customerRepository.findById(id)
                        .orElseThrow(() ->
                                new IllegalArgumentException(
                                        "Customer not found: " + id
                                ));

        customer.setName(request.name());
        customer.setEmail(request.email());
        customer.setStatus(request.status());

        return customerRepository.save(customer);
    }

    @Transactional
    @Audit(
            action = "DELETE",
            resource = "CUSTOMER"
    )
    public void deleteCustomer(Long id) {

        DemoCustomer customer =
                customerRepository.findById(id)
                        .orElseThrow(() ->
                                new IllegalArgumentException(
                                        "Customer not found: " + id
                                ));

        customerRepository.delete(customer);
    }

    @Transactional(readOnly = true)
    public DemoCustomer getCustomer(Long id) {

        return customerRepository.findById(id)
                .orElseThrow(() ->
                        new IllegalArgumentException(
                                "Customer not found: " + id
                        ));
    }
}