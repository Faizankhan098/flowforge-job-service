package com.flowforge.jobservice.service;

import com.flowforge.jobservice.metrics.JobMetrics;
import com.flowforge.jobservice.model.Job;
import com.flowforge.jobservice.model.JobStatus;
import com.flowforge.jobservice.model.OutboxEvent;
import com.flowforge.jobservice.repository.JobRepository;
import com.flowforge.jobservice.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobServiceTest {

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobMetrics jobMetrics;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @InjectMocks
    private JobService jobService;

    @Test
    void shouldCreateJobAndPublishEvent() {

        UUID jobId = UUID.randomUUID();

        Job savedJob = new Job();
        savedJob.setId(jobId);
        savedJob.setType("EMAIL");
        savedJob.setPayload("{\"to\":\"test@example.com\"}");
        savedJob.setStatus(JobStatus.PENDING);
        savedJob.setRetryCount(0);

        when(jobRepository.save(any(Job.class)))
                .thenReturn(savedJob);

        Job result = jobService.createJob(
                "EMAIL",
                "{\"to\":\"test@example.com\"}"
        );

        assertNotNull(result);
        assertEquals(jobId, result.getId());
        assertEquals("EMAIL", result.getType());
        assertEquals(JobStatus.PENDING, result.getStatus());
        assertEquals(0, result.getRetryCount());

        verify(jobRepository).save(any(Job.class));

        verify(outboxEventRepository).save(any(OutboxEvent.class));

        verify(jobMetrics).jobCreated();
    }

    @Test
    void shouldCreateJobWithPendingStatusAndZeroRetries() {

        Job savedJob = new Job();
        savedJob.setId(UUID.randomUUID());
        savedJob.setType("EMAIL");
        savedJob.setPayload("{}");
        savedJob.setStatus(JobStatus.PENDING);
        savedJob.setRetryCount(0);

        when(jobRepository.save(any(Job.class)))
                .thenReturn(savedJob);

        Job result = jobService.createJob("EMAIL", "{}");

        assertEquals(JobStatus.PENDING, result.getStatus());
        assertEquals(0, result.getRetryCount());

        ArgumentCaptor<Job> jobCaptor =
                ArgumentCaptor.forClass(Job.class);

        verify(jobRepository).save(jobCaptor.capture());

        Job jobSentToRepository = jobCaptor.getValue();

        assertEquals("EMAIL", jobSentToRepository.getType());
        assertEquals("{}", jobSentToRepository.getPayload());
        assertEquals(JobStatus.PENDING, jobSentToRepository.getStatus());
        assertEquals(0, jobSentToRepository.getRetryCount());

        assertNotNull(jobSentToRepository.getCreatedAt());
        assertNotNull(jobSentToRepository.getUpdatedAt());

        verify(outboxEventRepository).save(any(OutboxEvent.class));

        verify(jobMetrics).jobCreated();
    }
}