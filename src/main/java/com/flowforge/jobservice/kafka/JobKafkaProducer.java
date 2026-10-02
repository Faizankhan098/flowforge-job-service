package com.flowforge.jobservice.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class JobKafkaProducer {

    private static final String TOPIC = "flowforge.jobs";

    private final KafkaTemplate<String, JobEvent> kafkaTemplate;

    public JobKafkaProducer(KafkaTemplate<String, JobEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishJob(String jobId, String type, String payload) {

        JobEvent event = new JobEvent(
                jobId,
                type,
                payload
        );

        try {
            kafkaTemplate
                    .send(TOPIC, jobId, event)
                    .get(10, TimeUnit.SECONDS);

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to publish job to Kafka: " + jobId,
                    e
            );
        }
    }
}