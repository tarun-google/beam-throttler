package com.example.ExternalServiceThrottler;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class ExternalThrottlerDTOs {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AcquireRequest {
        private int permits;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AcquireResponse {
        private boolean acquired;
        private long retryAfterMillis;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RateUpdateRequest {
        private int permitsPerSecond;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StatusResponse {
        private String status;
    }
}
