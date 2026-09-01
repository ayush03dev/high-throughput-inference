package com.consuma.inference.common.service;

import com.consuma.inference.common.entity.ModelEntity;
import com.consuma.inference.common.repository.ModelRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
public class ModelConfigService {

    private static final String LIMITS_PREFIX = "limits:";

    private final ModelRepository modelRepository;
    private final StringRedisTemplate redisTemplate;

    public ModelConfigService(ModelRepository modelRepository, StringRedisTemplate redisTemplate) {
        this.modelRepository = modelRepository;
        this.redisTemplate = redisTemplate;
    }

    public Optional<ModelEntity> findModel(String name) {
        return modelRepository.findById(name);
    }

    public List<ModelEntity> findAll() {
        return modelRepository.findAll();
    }

    @Transactional
    public ModelEntity updateLimits(String name, long rpmLimit, long tpmLimit) {
        ModelEntity model = modelRepository.findById(name)
                .orElseThrow(() -> new IllegalArgumentException("Unknown model: " + name));
        model.setRpmLimit(rpmLimit);
        model.setTpmLimit(tpmLimit);
        model.setUpdatedAt(Instant.now());
        ModelEntity saved = modelRepository.save(model);
        cacheLimits(saved);
        return saved;
    }

    public ModelLimits getLimits(String name) {
        String cached = redisTemplate.opsForValue().get(LIMITS_PREFIX + name);
        if (cached != null) {
            String[] parts = cached.split(":");
            return new ModelLimits(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
        }
        return modelRepository.findById(name)
                .map(m -> {
                    cacheLimits(m);
                    return new ModelLimits(m.getRpmLimit(), m.getTpmLimit());
                })
                .orElseThrow(() -> new IllegalArgumentException("Unknown model: " + name));
    }

    public void cacheLimits(ModelEntity model) {
        redisTemplate.opsForValue().set(
                LIMITS_PREFIX + model.getName(),
                model.getRpmLimit() + ":" + model.getTpmLimit()
        );
    }

    public record ModelLimits(long rpmLimit, long tpmLimit) {
    }
}
