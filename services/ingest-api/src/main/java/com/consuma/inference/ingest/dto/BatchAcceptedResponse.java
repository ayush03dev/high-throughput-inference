package com.consuma.inference.ingest.dto;

public record BatchAcceptedResponse(String batchId, String status, int totalRequests) {
}
