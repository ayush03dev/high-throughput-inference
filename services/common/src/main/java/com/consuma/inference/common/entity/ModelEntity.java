package com.consuma.inference.common.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "models")
public class ModelEntity {

    @Id
    @Column(name = "name")
    private String name;

    @Column(name = "rpm_limit", nullable = false)
    private long rpmLimit;

    @Column(name = "tpm_limit", nullable = false)
    private long tpmLimit;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public ModelEntity() {
    }

    public ModelEntity(String name, long rpmLimit, long tpmLimit) {
        this.name = name;
        this.rpmLimit = rpmLimit;
        this.tpmLimit = tpmLimit;
        this.updatedAt = Instant.now();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getRpmLimit() {
        return rpmLimit;
    }

    public void setRpmLimit(long rpmLimit) {
        this.rpmLimit = rpmLimit;
    }

    public long getTpmLimit() {
        return tpmLimit;
    }

    public void setTpmLimit(long tpmLimit) {
        this.tpmLimit = tpmLimit;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
