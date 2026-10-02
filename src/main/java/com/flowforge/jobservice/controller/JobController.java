package com.flowforge.jobservice.controller;

import com.flowforge.jobservice.model.Job;
import com.flowforge.jobservice.service.JobService;
import com.flowforge.jobservice.service.RateLimitService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;
    private final RateLimitService rateLimitService;

    public JobController(
            JobService jobService,
            RateLimitService rateLimitService) {
        this.jobService = jobService;
        this.rateLimitService = rateLimitService;
    }

    @PostMapping
    public ResponseEntity<?> createJob(
            @RequestParam String type,
            @RequestBody String payload,
            HttpServletRequest request) {

        String clientIp = request.getRemoteAddr();

        if (!rateLimitService.isAllowed(clientIp)) {
            return ResponseEntity
                    .status(HttpStatus.TOO_MANY_REQUESTS)
                    .body("Rate limit exceeded. Try again later.");
        }

        Job job = jobService.createJob(type, payload);

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(job);
    }
}