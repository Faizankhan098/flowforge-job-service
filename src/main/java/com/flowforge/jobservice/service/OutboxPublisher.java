package com.flowforge.jobservice.service;

import com.flowforge.jobservice.kafka.JobKafkaProducer;
import com.flowforge.jobservice.model.OutboxEvent;
import com.flowforge.jobservice.repository.OutboxEventRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final JobKafkaProducer jobKafkaProducer;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            JobKafkaProducer jobKafkaProducer) {

        this.outboxEventRepository = outboxEventRepository;
        this.jobKafkaProducer = jobKafkaProducer;
    }

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingEvents() {

        List<OutboxEvent> events =
                outboxEventRepository
                        .findTop100ByPublishedFalseOrderByCreatedAtAsc();

        for (OutboxEvent event : events) {

            jobKafkaProducer.publishJob(
                    event.getAggregateId().toString(),
                    event.getEventType(),
                    event.getPayload()
            );

            event.setPublished(true);
            outboxEventRepository.save(event);
        }
    }
}