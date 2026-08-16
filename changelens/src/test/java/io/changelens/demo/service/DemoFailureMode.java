package io.changelens.demo.service;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class DemoFailureMode {

    private final AtomicBoolean enabled =
            new AtomicBoolean(false);

    public void enable() {
        enabled.set(true);
    }

    public void disable() {
        enabled.set(false);
    }

    public boolean isEnabled() {
        return enabled.get();
    }
}
