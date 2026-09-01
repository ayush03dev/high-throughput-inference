package com.consuma.inference.common.repository;

import com.consuma.inference.common.entity.BatchEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface BatchRepository extends JpaRepository<BatchEntity, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT b FROM BatchEntity b WHERE b.batchId = :batchId")
    Optional<BatchEntity> findByIdForUpdate(@Param("batchId") String batchId);
}
