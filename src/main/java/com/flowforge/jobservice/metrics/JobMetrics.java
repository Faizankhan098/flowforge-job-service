package com.flowforge.jobservice.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class JobMetrics {

    private final Counter jobsCreated;
    private final Counter jobsCompleted;
    private final Counter jobsFailed;
    private final Counter jobsRetried;

    public JobMetrics(MeterRegistry meterRegistry) {

        jobsCreated = Counter.builder("flowforge.jobs.created")
                .description("Total number of jobs created")
                .register(meterRegistry);

        jobsCompleted = Counter.builder("flowforge.jobs.completed")
                .description("Total number of jobs completed")
                .register(meterRegistry);

        jobsFailed = Counter.builder("flowforge.jobs.failed")
                .description("Total number of jobs permanently failed")
                .register(meterRegistry);

        jobsRetried = Counter.builder("flowforge.jobs.retried")
                .description("Total number of job retry attempts")
                .register(meterRegistry);
    }

    public void jobCreated() {
        jobsCreated.increment();
    }

    public void jobCompleted() {
        jobsCompleted.increment();
    }

    public void jobFailed() {
        jobsFailed.increment();
    }

    public void jobRetried() {
        jobsRetried.increment();
    }
}