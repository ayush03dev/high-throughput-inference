package com.consuma.inference.common.domain;

public enum RequestState {
    QUEUED,
    IN_FLIGHT,
    SUCCEEDED,
    FAILED,
    EXPIRED,
    REJECTED
}
