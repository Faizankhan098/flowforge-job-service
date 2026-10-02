package com.flowforge.jobservice.service;

import com.flowforge.jobservice.model.Job;
import com.flowforge.jobservice.model.JobStatus;
import com.flowforge.jobservice.repository.JobRepository;
import org.springframework.stereotype.Service;
import com.flowforge.jobservice.metrics.JobMetrics;
import java.time.Instant;
import com.flowforge.jobservice.model.OutboxEvent;
import com.flowforge.jobservice.repository.OutboxEventRepository;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobService {

    private final JobRepository jobRepository;
    private final JobMetrics jobMetrics;
    private final OutboxEventRepository outboxEventRepository;

    public JobService(JobRepository jobRepository, JobMetrics jobMetrics, OutboxEventRepository outboxEventRepository) {
        this.jobRepository = jobRepository;
        this.jobMetrics = jobMetrics;
        this.outboxEventRepository = outboxEventRepository;

    }
    @Transactional
    public Job createJob(String type, String payload) {

        Job job = new Job();

        job.setType(type);
        job.setPayload(payload);
        job.setStatus(JobStatus.PENDING);
        job.setRetryCount(0);

        Instant now = Instant.now();
        job.setCreatedAt(now);
        job.setUpdatedAt(now);

        Job savedJob = jobRepository.save(job);
        OutboxEvent outboxEvent = new OutboxEvent();
        outboxEvent.setAggregateId(savedJob.getId());
        outboxEvent.setEventType(savedJob.getType());
        outboxEvent.setPayload(savedJob.getPayload());
        outboxEvent.setPublished(false);
        outboxEvent.setCreatedAt(Instant.now());

        outboxEventRepository.save(outboxEvent);
        jobMetrics.jobCreated();

        return savedJob;
    }
}