package com.example.ExternalThrottler;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class ExternalThrottlerDTOs {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AcquireRequest {
        private long permits;
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
        private long permitsPerSecond;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StatusResponse {
        private String status;
    }
}
