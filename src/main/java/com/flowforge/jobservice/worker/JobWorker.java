package com.flowforge.jobservice.worker;

import com.flowforge.jobservice.kafka.JobEvent;
import com.flowforge.jobservice.model.Job;
import com.flowforge.jobservice.model.JobStatus;
import com.flowforge.jobservice.repository.JobRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.flowforge.jobservice.metrics.JobMetrics;
import java.time.Instant;

import java.util.UUID;

@Service
public class JobWorker {

    private final JobRepository jobRepository;
    private static final int MAX_RETRIES = 3;
    private final JobMetrics jobMetrics;

    public JobWorker(JobRepository jobRepository, JobMetrics jobMetrics) {
        this.jobRepository = jobRepository;
        this.jobMetrics = jobMetrics;
    }

    @KafkaListener(
            topics = "flowforge.jobs",
            groupId = "flowforge-workers"
    )
    public void processJob(JobEvent event) {

        System.out.println(
                "Processing job: " + event.getJobId()
        );

        System.out.println(
                "Job type: " + event.getType()
        );

        System.out.println(
                "Payload: " + event.getPayload()
        );

        UUID jobId = UUID.fromString(event.getJobId());

        Job job = jobRepository.findById(jobId)
                .orElseThrow();

        if (job.getStatus() == JobStatus.COMPLETED) {
            System.out.println(
                    "Skipping already completed job: " + job.getId()
            );
            return;
        }

        int updatedRows = jobRepository.updateStatusIfCurrentStatus(
                jobId,
                JobStatus.PENDING,
                JobStatus.PROCESSING,
                Instant.now()
        );

        if (updatedRows == 0) {
            System.out.println(
                    "Job was already claimed or processed: " + jobId
            );
            return;
        }

//Failed job
        if ("FAIL_TEST".equals(event.getType())) {

            int currentRetries = job.getRetryCount();

            if (currentRetries < 3) {
                job.setRetryCount(currentRetries + 1);
                job.setStatus(JobStatus.PENDING);
                jobRepository.save(job);

                jobMetrics.jobRetried();

                throw new RuntimeException("Simulated job failure");
            }

            job.setStatus(JobStatus.FAILED);
            jobRepository.save(job);

            jobMetrics.jobFailed();

            System.out.println(
                    "Job permanently failed: " + job.getId()
            );

            return;
        }
        // Simulate actual job processing
        System.out.println("Executing job...");

        // Job completed successfully
        job.setStatus(JobStatus.COMPLETED);
        jobRepository.save(job);
        jobMetrics.jobCompleted();

        System.out.println(
                "Job completed: " + job.getId()
        );
    }
}