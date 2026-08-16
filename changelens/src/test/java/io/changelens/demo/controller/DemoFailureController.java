package io.changelens.demo.controller;

import io.changelens.demo.service.DemoFailureMode;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/demo/failure")
@RequiredArgsConstructor
public class DemoFailureController {

    private final DemoFailureMode failureMode;

    @PostMapping("/enable")
    public String enable() {
        failureMode.enable();
        return "Demo failure mode enabled";
    }

    @PostMapping("/disable")
    public String disable() {
        failureMode.disable();
        return "Demo failure mode disabled";
    }

    @GetMapping
    public boolean status() {
        return failureMode.isEnabled();
    }
}