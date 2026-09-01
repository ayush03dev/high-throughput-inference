package com.consuma.inference.common.repository;

import com.consuma.inference.common.entity.RequestEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface RequestRepository extends JpaRepository<RequestEntity, String> {
    List<RequestEntity> findByBatchId(String batchId);
}
