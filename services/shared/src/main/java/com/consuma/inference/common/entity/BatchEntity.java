package com.consuma.inference.common.entity;

import com.consuma.inference.common.domain.BatchStatus;
import com.consuma.inference.common.domain.CallbackStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "batches")
public class BatchEntity {

    @Id
    @Column(name = "batch_id")
    private String batchId;

    @Column(name = "callback_url", nullable = false)
    private String callbackUrl;

    @Column(name = "total_requests", nullable = false)
    private int totalRequests;

    @Column(name = "terminal_count", nullable = false)
    private int terminalCount;

    @Column(name = "succeeded_count", nullable = false)
    private int succeededCount;

    @Column(name = "failed_count", nullable = false)
    private int failedCount;

    @Column(name = "expired_count", nullable = false)
    private int expiredCount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private BatchStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "callback_status")
    private CallbackStatus callbackStatus;

    @Column(name = "callback_attempts", nullable = false)
    private int callbackAttempts;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    public BatchEntity() {
    }

    public BatchEntity(String batchId, String callbackUrl, int totalRequests) {
        this.batchId = batchId;
        this.callbackUrl = callbackUrl;
        this.totalRequests = totalRequests;
        this.terminalCount = 0;
        this.succeededCount = 0;
        this.failedCount = 0;
        this.expiredCount = 0;
        this.status = BatchStatus.ACCEPTED;
        this.callbackStatus = CallbackStatus.PENDING;
        this.callbackAttempts = 0;
        this.createdAt = Instant.now();
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getCallbackUrl() {
        return callbackUrl;
    }

    public void setCallbackUrl(String callbackUrl) {
        this.callbackUrl = callbackUrl;
    }

    public int getTotalRequests() {
        return totalRequests;
    }

    public void setTotalRequests(int totalRequests) {
        this.totalRequests = totalRequests;
    }

    public int getTerminalCount() {
        return terminalCount;
    }

    public void setTerminalCount(int terminalCount) {
        this.terminalCount = terminalCount;
    }

    public int getSucceededCount() {
        return succeededCount;
    }

    public void setSucceededCount(int succeededCount) {
        this.succeededCount = succeededCount;
    }

    public int getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(int failedCount) {
        this.failedCount = failedCount;
    }

    public int getExpiredCount() {
        return expiredCount;
    }

    public void setExpiredCount(int expiredCount) {
        this.expiredCount = expiredCount;
    }

    public BatchStatus getStatus() {
        return status;
    }

    public void setStatus(BatchStatus status) {
        this.status = status;
    }

    public CallbackStatus getCallbackStatus() {
        return callbackStatus;
    }

    public void setCallbackStatus(CallbackStatus callbackStatus) {
        this.callbackStatus = callbackStatus;
    }

    public int getCallbackAttempts() {
        return callbackAttempts;
    }

    public void setCallbackAttempts(int callbackAttempts) {
        this.callbackAttempts = callbackAttempts;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }
}
