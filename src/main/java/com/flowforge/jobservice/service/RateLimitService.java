package com.flowforge.jobservice.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class RateLimitService {

    private final StringRedisTemplate redisTemplate;
    private final int maxRequests;
    private final Duration window;

    public RateLimitService(
            StringRedisTemplate redisTemplate,
            @Value("${flowforge.rate-limit.max-requests:10}") int maxRequests,
            @Value("${flowforge.rate-limit.window-seconds:60}") long windowSeconds) {

        this.redisTemplate = redisTemplate;
        this.maxRequests = maxRequests;
        this.window = Duration.ofSeconds(windowSeconds);
    }

    public boolean isAllowed(String clientIp) {

        String key = "rate_limit:" + clientIp;

        Long requestCount = redisTemplate.opsForValue().increment(key);

        if (requestCount == 1) {
            redisTemplate.expire(key, window);
        }

        return requestCount <= maxRequests;
    }
}