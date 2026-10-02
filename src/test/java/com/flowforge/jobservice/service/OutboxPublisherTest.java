package com.flowforge.jobservice.service;

import com.flowforge.jobservice.kafka.JobKafkaProducer;
import com.flowforge.jobservice.model.OutboxEvent;
import com.flowforge.jobservice.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private JobKafkaProducer jobKafkaProducer;

    @InjectMocks
    private OutboxPublisher outboxPublisher;

    @Test
    void shouldPublishPendingOutboxEvent() {

        UUID jobId = UUID.randomUUID();

        OutboxEvent event = new OutboxEvent();
        event.setAggregateId(jobId);
        event.setEventType("EMAIL");
        event.setPayload("{\"to\":\"test@example.com\"}");
        event.setPublished(false);
        event.setCreatedAt(Instant.now());

        when(outboxEventRepository
                .findTop100ByPublishedFalseOrderByCreatedAtAsc())
                .thenReturn(List.of(event));

        outboxPublisher.publishPendingEvents();

        verify(jobKafkaProducer).publishJob(
                jobId.toString(),
                "EMAIL",
                "{\"to\":\"test@example.com\"}"
        );

        verify(outboxEventRepository).save(event);

        assert event.isPublished();
    }

    @Test
    void shouldDoNothingWhenNoPendingEventsExist() {

        when(outboxEventRepository
                .findTop100ByPublishedFalseOrderByCreatedAtAsc())
                .thenReturn(List.of());

        outboxPublisher.publishPendingEvents();

        verifyNoInteractions(jobKafkaProducer);
        verify(outboxEventRepository)
                .findTop100ByPublishedFalseOrderByCreatedAtAsc();
    }

    @Test
    void shouldNotMarkEventPublishedWhenKafkaPublishFails() {

        UUID jobId = UUID.randomUUID();

        OutboxEvent event = new OutboxEvent();
        event.setAggregateId(jobId);
        event.setEventType("EMAIL");
        event.setPayload("{\"to\":\"failure@example.com\"}");
        event.setPublished(false);
        event.setCreatedAt(Instant.now());

        when(outboxEventRepository
                .findTop100ByPublishedFalseOrderByCreatedAtAsc())
                .thenReturn(List.of(event));

        doThrow(new RuntimeException("Kafka unavailable"))
                .when(jobKafkaProducer)
                .publishJob(
                        jobId.toString(),
                        "EMAIL",
                        "{\"to\":\"failure@example.com\"}"
                );

        assertThrows(
                RuntimeException.class,
                () -> outboxPublisher.publishPendingEvents()
        );

        assertFalse(event.isPublished());

        verify(jobKafkaProducer).publishJob(
                jobId.toString(),
                "EMAIL",
                "{\"to\":\"failure@example.com\"}"
        );

        verify(outboxEventRepository, never()).save(event);
    }
}