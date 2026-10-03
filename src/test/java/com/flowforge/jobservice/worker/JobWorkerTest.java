package com.flowforge.jobservice.worker;
import java.time.Instant;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import com.flowforge.jobservice.kafka.JobEvent;
import com.flowforge.jobservice.metrics.JobMetrics;
import com.flowforge.jobservice.model.Job;
import com.flowforge.jobservice.model.JobStatus;
import com.flowforge.jobservice.repository.JobRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import java.util.UUID;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobWorkerTest {

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobMetrics jobMetrics;

    @InjectMocks
    private JobWorker jobWorker;

    @Test
    void shouldProcessJobSuccessfully() {

        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setType("EMAIL");
        job.setPayload("{\"to\":\"test@example.com\"}");
        job.setStatus(JobStatus.PENDING);
        job.setRetryCount(0);

        JobEvent event = new JobEvent(
                jobId.toString(),
                "EMAIL",
                "{\"to\":\"test@example.com\"}"
        );

        when(jobRepository.findById(jobId))
                .thenReturn(Optional.of(job));

        when(jobRepository.updateStatusIfCurrentStatus(
                eq(jobId),
                eq(JobStatus.PENDING),
                eq(JobStatus.PROCESSING),
                any(Instant.class)
        )).thenReturn(1);

        jobWorker.processJob(event);

        verify(jobRepository).findById(jobId);

        verify(jobRepository).updateStatusIfCurrentStatus(
                eq(jobId),
                eq(JobStatus.PENDING),
                eq(JobStatus.PROCESSING),
                any(Instant.class)
        );

        verify(jobRepository).save(job);

        verify(jobMetrics).jobCompleted();
    }
    @Test
    void shouldSkipAlreadyCompletedJob() {

        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setType("EMAIL");
        job.setPayload("{\"to\":\"test@example.com\"}");
        job.setStatus(JobStatus.COMPLETED);
        job.setRetryCount(0);

        JobEvent event = new JobEvent(
                jobId.toString(),
                "EMAIL",
                "{\"to\":\"test@example.com\"}"
        );

        when(jobRepository.findById(jobId))
                .thenReturn(Optional.of(job));

        jobWorker.processJob(event);

        verify(jobRepository).findById(jobId);

        verify(jobRepository, never()).updateStatusIfCurrentStatus(
                any(),
                any(),
                any(),
                any()
        );

        verify(jobRepository, never()).save(any(Job.class));

        verify(jobMetrics, never()).jobCompleted();
    }
    @Test
    void shouldRetryFailedJob() {

        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setType("FAIL_TEST");
        job.setPayload("{}");
        job.setStatus(JobStatus.PENDING);
        job.setRetryCount(0);

        JobEvent event = new JobEvent(
                jobId.toString(),
                "FAIL_TEST",
                "{}"
        );

        when(jobRepository.findById(jobId))
                .thenReturn(Optional.of(job));

        when(jobRepository.updateStatusIfCurrentStatus(
                eq(jobId),
                eq(JobStatus.PENDING),
                eq(JobStatus.PROCESSING),
                any(Instant.class)
        )).thenReturn(1);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> jobWorker.processJob(event)
        );

        assertEquals("Simulated job failure", exception.getMessage());

        assertEquals(1, job.getRetryCount());
        assertEquals(JobStatus.PENDING, job.getStatus());

        verify(jobRepository).save(job);

        verify(jobMetrics).jobRetried();

        verify(jobMetrics, never()).jobCompleted();
        verify(jobMetrics, never()).jobFailed();
    }

    @Test
    void shouldSkipJobWhenAnotherWorkerAlreadyClaimedIt() {

        UUID jobId = UUID.randomUUID();

        Job job = new Job();
        job.setId(jobId);
        job.setType("EMAIL");
        job.setPayload("{}");
        job.setStatus(JobStatus.PROCESSING);
        job.setRetryCount(0);

        JobEvent event = new JobEvent(
                jobId.toString(),
                "EMAIL",
                "{}"
        );

        when(jobRepository.findById(jobId))
                .thenReturn(Optional.of(job));

        when(jobRepository.updateStatusIfCurrentStatus(
                eq(jobId),
                eq(JobStatus.PENDING),
                eq(JobStatus.PROCESSING),
                any(Instant.class)
        )).thenReturn(0);

        jobWorker.processJob(event);

        verify(jobRepository).findById(jobId);

        verify(jobRepository).updateStatusIfCurrentStatus(
                eq(jobId),
                eq(JobStatus.PENDING),
                eq(JobStatus.PROCESSING),
                any(Instant.class)
        );

        verify(jobRepository, never()).save(any(Job.class));

        verify(jobMetrics, never()).jobCompleted();
    }
}