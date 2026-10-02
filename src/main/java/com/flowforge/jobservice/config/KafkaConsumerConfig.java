package com.flowforge.jobservice.config;

import com.flowforge.jobservice.kafka.JobEvent;
import com.flowforge.jobservice.model.Job;
import com.flowforge.jobservice.model.JobStatus;
import com.flowforge.jobservice.repository.JobRepository;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.ExponentialBackOff;
import com.flowforge.jobservice.metrics.JobMetrics;

import java.util.UUID;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(
            KafkaTemplate<String, Object> kafkaTemplate,
            JobRepository jobRepository,JobMetrics jobMetrics) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) -> {

                            if (record.value() instanceof JobEvent event) {

                                Job job = jobRepository.findById(
                                        UUID.fromString(event.getJobId())
                                ).orElse(null);

                                if (job != null) {
                                    job.setStatus(JobStatus.FAILED);
                                    job.setRetryCount(3);
                                    jobRepository.save(job);

                                    jobMetrics.jobFailed();
                                }
                            }

                            return new TopicPartition(
                                    "flowforge.jobs.dlq",
                                    record.partition()
                            );
                        }
                );

        ExponentialBackOff backOff = new ExponentialBackOff(
                1000L,
                2.0
        );

        backOff.setMaxInterval(10000L);

        return new DefaultErrorHandler(
                recoverer,
                backOff
        );
    }
}