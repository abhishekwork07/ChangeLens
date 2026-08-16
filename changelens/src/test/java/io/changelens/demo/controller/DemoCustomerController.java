package io.changelens.demo.controller;

import io.changelens.demo.dto.CreateCustomerRequest;
import io.changelens.demo.dto.UpdateCustomerRequest;
import io.changelens.demo.entity.DemoCustomer;
import io.changelens.demo.service.DemoCustomerService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/demo/customers")
@RequiredArgsConstructor
public class DemoCustomerController {

    private final DemoCustomerService customerService;

    @PostMapping
    public DemoCustomer create(
            @RequestBody CreateCustomerRequest request) {

        return customerService.createCustomer(request);
    }

    @PutMapping("/{id}")
    public DemoCustomer update(
            @PathVariable Long id,
            @RequestBody UpdateCustomerRequest request) {

        return customerService.updateCustomer(id, request);
    }

    @GetMapping("/{id}")
    public DemoCustomer get(
            @PathVariable Long id) {

        return customerService.getCustomer(id);
    }

    @DeleteMapping("/{id}")
    public void delete(
            @PathVariable Long id) {

        customerService.deleteCustomer(id);
    }
}