package com.flowforge.jobservice.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class HomeController {

    @GetMapping("/")
    public Map<String, String> home() {
        return Map.of(
                "service", "FlowForge",
                "description", "Distributed Job & Workflow Processing Platform",
                "status", "UP",
                "version", "1.0.0"
        );
    }
}