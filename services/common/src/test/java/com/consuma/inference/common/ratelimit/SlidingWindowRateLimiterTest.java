package com.consuma.inference.common.ratelimit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SlidingWindowRateLimiterTest {

    private StringRedisTemplate redisTemplate;
    private SlidingWindowRateLimiter limiter;

    @BeforeEach
    void setUp() {
        redisTemplate = mock(StringRedisTemplate.class);
        limiter = new SlidingWindowRateLimiter(redisTemplate);
    }

    @Test
    void allowsWhenRedisScriptsReturnSuccess() {
        when(redisTemplate.execute(any(), anyList(), any(), any(), any(), any())).thenReturn(1L);
        when(redisTemplate.execute(any(), anyList(), any(), any(), any(), any(), any())).thenReturn(1L);
        assertTrue(limiter.tryAcquire("model-a", "req-1", 1000, 50_000, 100_000_000));
    }

    @Test
    void deniesWhenRpmExceeded() {
        when(redisTemplate.execute(any(), anyList(), any(), any(), any(), any())).thenReturn(0L);
        assertFalse(limiter.tryAcquire("model-a", "req-1", 1000, 50_000, 100_000_000));
    }

    @Test
    void deniesWhenTpmExceededAndRollsBackRpm() {
        when(redisTemplate.execute(any(), anyList(), any(), any(), any(), any())).thenReturn(1L);
        when(redisTemplate.execute(any(), anyList(), any(), any(), any(), any(), any())).thenReturn(0L);
        ZSetOperations<String, String> zSet = mock(ZSetOperations.class);
        when(redisTemplate.opsForZSet()).thenReturn(zSet);
        assertFalse(limiter.tryAcquire("model-a", "req-1", 1000, 50_000, 100_000_000));
    }
}
