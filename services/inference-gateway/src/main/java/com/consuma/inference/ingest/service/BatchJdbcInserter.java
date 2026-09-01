package com.consuma.inference.ingest.service;

import com.consuma.inference.ingest.dto.SubmitInferenceRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Service
public class BatchJdbcInserter {

    private static final String BULK_INSERT_SQL = """
            INSERT INTO requests (request_id, batch_id, model, estimated_tokens, state, payload, submitted_at)
            SELECT r.request_id, ?, r.model, r.estimated_tokens, 'QUEUED', r.payload, ?
            FROM jsonb_to_recordset(?::jsonb) AS r(
                request_id text,
                model text,
                estimated_tokens int,
                payload jsonb
            )
            """;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public BatchJdbcInserter(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void insertAll(String batchId, List<SubmitInferenceRequest> requests, Instant submittedAt) {
        ArrayNode rows = objectMapper.createArrayNode();
        for (SubmitInferenceRequest req : requests) {
            ObjectNode row = objectMapper.createObjectNode();
            row.put("request_id", req.requestId());
            row.put("model", req.model());
            row.put("estimated_tokens", req.estimatedTokens());
            row.set("payload", objectMapper.valueToTree(req.payload()));
            rows.add(row);
        }
        jdbcTemplate.update(
                BULK_INSERT_SQL,
                batchId,
                Timestamp.from(submittedAt),
                rows.toString()
        );
    }
}
