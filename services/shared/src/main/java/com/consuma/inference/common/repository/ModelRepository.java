package com.consuma.inference.common.repository;

import com.consuma.inference.common.entity.ModelEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ModelRepository extends JpaRepository<ModelEntity, String> {
}
