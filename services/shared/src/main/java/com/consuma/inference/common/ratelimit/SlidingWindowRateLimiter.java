package com.consuma.inference.common.ratelimit;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SlidingWindowRateLimiter {

    private static final long WINDOW_MS = 60_000L;

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> rpmScript;
    private final DefaultRedisScript<Long> tpmScript;

    public SlidingWindowRateLimiter(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.rpmScript = new DefaultRedisScript<>();
        this.rpmScript.setLocation(new ClassPathResource("redis/rpm_limiter.lua"));
        this.rpmScript.setResultType(Long.class);
        this.tpmScript = new DefaultRedisScript<>();
        this.tpmScript.setLocation(new ClassPathResource("redis/tpm_limiter.lua"));
        this.tpmScript.setResultType(Long.class);
    }

    public boolean tryAcquire(String model, String requestId, int estimatedTokens, long rpmLimit, long tpmLimit) {
        long now = System.currentTimeMillis();
        Long rpmAllowed = redisTemplate.execute(
                rpmScript,
                List.of("rpm:" + model),
                String.valueOf(now),
                String.valueOf(WINDOW_MS),
                String.valueOf(rpmLimit),
                requestId
        );
        if (rpmAllowed == null || rpmAllowed == 0L) {
            return false;
        }
        Long tpmAllowed = redisTemplate.execute(
                tpmScript,
                List.of("tpm:" + model),
                String.valueOf(now),
                String.valueOf(WINDOW_MS),
                String.valueOf(tpmLimit),
                String.valueOf(estimatedTokens),
                requestId
        );
        if (tpmAllowed == null || tpmAllowed == 0L) {
            redisTemplate.opsForZSet().remove("rpm:" + model, requestId);
            return false;
        }
        return true;
    }
}
