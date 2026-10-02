package com.flowforge.jobservice.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RateLimitServiceTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private RateLimitService rateLimitService;

    @BeforeEach
    void setUp() {
        rateLimitService = new RateLimitService(
                redisTemplate,
                10,
                60
        );
    }

    @Test
    void shouldAllowFirstRequest() {

        when(redisTemplate.opsForValue())
                .thenReturn(valueOperations);

        when(valueOperations.increment("rate_limit:127.0.0.1"))
                .thenReturn(1L);

        boolean allowed =
                rateLimitService.isAllowed("127.0.0.1");

        assertTrue(allowed);

        verify(valueOperations)
                .increment("rate_limit:127.0.0.1");

        verify(redisTemplate)
                .expire(
                        "rate_limit:127.0.0.1",
                        Duration.ofSeconds(60)
                );
    }

    @Test
    void shouldRejectRequestAboveLimit() {

        when(redisTemplate.opsForValue())
                .thenReturn(valueOperations);

        when(valueOperations.increment("rate_limit:127.0.0.1"))
                .thenReturn(11L);

        boolean allowed =
                rateLimitService.isAllowed("127.0.0.1");

        assertFalse(allowed);
    }
}