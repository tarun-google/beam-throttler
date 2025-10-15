package com.example.ExternalThrottler;

import com.example.ExternalThrottler.ExternalThrottlerDTOs.AcquireRequest;
import com.example.ExternalThrottler.ExternalThrottlerDTOs.AcquireResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class ExternalThrottler implements Serializable {

    private final String rateLimiterUrl;
    private final int permitBatchSize;

    private transient HttpClient httpClient;
    private transient ObjectMapper objectMapper;
    private transient Long availablePermits;

    private final Counter throttledRequests = Metrics.counter(ExternalThrottler.class, "throttled-requests");
    private final Counter failedRequests = Metrics.counter(ExternalThrottler.class, "failed-requests-to-ratelimiter");
    private final Counter permitsAcquired = Metrics.counter(ExternalThrottler.class, "permits-acquired");

    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final long BASE_SLEEP_MS = 200;
    private static final long MAX_SLEEP_MS = 60000;

    private ExternalThrottler(String rateLimiterUrl, int permitBatchSize) {
        this.rateLimiterUrl = rateLimiterUrl;
        this.permitBatchSize = permitBatchSize;
    }

    public static ExternalThrottler of(String rateLimiterUrl, int permitBatchSize) {
        return new ExternalThrottler(rateLimiterUrl, permitBatchSize);
    }

    public void setup() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(REQUEST_TIMEOUT)
                .build();
        this.objectMapper = new ObjectMapper();
        this.availablePermits = 0L;
    }

    /**
     * Waits until a permit is available. If the local cache is empty, it requests a new batch
     * of permits from the external rate limiter service. This method will block if throttled.
     */
    public void waitAndAcquire() throws IOException, InterruptedException {
        if (availablePermits == 0) {
            acquirePermits(this.permitBatchSize);
            availablePermits--;
        }
    }

    private void acquirePermits(long numPermits) throws IOException, InterruptedException {

        AcquireRequest payload = new AcquireRequest(numPermits);
        String requestBody = objectMapper.writeValueAsString(payload);

        int retryCount = 0;
        while (true) {
            HttpResponse<String> response;
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(rateLimiterUrl + "/acquire"))
                        .header("Content-Type", "application/json")
                        .timeout(REQUEST_TIMEOUT)
                        .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                        .build();
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException e) {
                failedRequests.inc();
                Thread.sleep(calculateBackoff(++retryCount));
                continue;
            }

            if (response.statusCode() != 200) {
                failedRequests.inc();
                Thread.sleep(calculateBackoff(++retryCount));
                continue;
            }

            AcquireResponse acquireResponse = objectMapper.readValue(response.body(), AcquireResponse.class);
            if (acquireResponse.isAcquired()) {
                availablePermits = numPermits;
                permitsAcquired.inc(numPermits);
                System.out.println("Got Permits");
                return;
            } else {
                throttledRequests.inc();
                System.out.println("Got throttled");
                Thread.sleep(Math.max(acquireResponse.getRetryAfterMillis(), BASE_SLEEP_MS));
            }
        }
    }

    // Exponential Backoff with Jitter for failures
    private long calculateBackoff(int retryCount) {
        long backoff = (long) Math.min(MAX_SLEEP_MS, BASE_SLEEP_MS * Math.pow(2, retryCount));
        return backoff + (long) (Math.random() * backoff * 0.2); // Add jitter
    }
}
