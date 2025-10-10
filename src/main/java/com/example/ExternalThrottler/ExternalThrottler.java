package com.example.ExternalThrottler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.example.ExternalThrottler.ExternalThrottlerDTOs.AcquireRequest;
import com.example.ExternalThrottler.ExternalThrottlerDTOs.AcquireResponse;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public class ExternalThrottler<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private final int permitBatchSize;
    private final String rateLimiterUrl;

    public ExternalThrottler(int permitBatchSize, String rateLimiterUrl) {
        this.permitBatchSize = permitBatchSize;
        this.rateLimiterUrl = rateLimiterUrl;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return input.apply("ThrottleElements", ParDo.of(new ThrottlingDoFn<T>(this.rateLimiterUrl, this.permitBatchSize)));
    }

    private static class ThrottlingDoFn<T> extends DoFn<T, T> {

        private final String rateLimiterUrl;
        private final int permitBatchSize;
        private transient HttpClient httpClient;
        private transient ObjectMapper objectMapper;
        private transient AtomicLong availablePermits;

        private final Counter throttledRequests = Metrics.counter(ThrottlingDoFn.class, "throttled-requests");
        private final Counter failedRequests = Metrics.counter(ThrottlingDoFn.class, "failed-requests-to-ratelimiter");

        private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
        private static final long BASE_SLEEP_MS = 200;
        private static final long MAX_SLEEP_MS = 60000;

        public ThrottlingDoFn(String rateLimiterUrl, int permitBatchSize) {
            this.rateLimiterUrl = rateLimiterUrl;
            this.permitBatchSize = permitBatchSize;
        }

        @Setup
        public void setup() {
            this.httpClient = HttpClient.newBuilder()
                    .connectTimeout(REQUEST_TIMEOUT)
                    .build();
            this.objectMapper = new ObjectMapper();
            this.availablePermits = new AtomicLong(0);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException, InterruptedException {
            if (availablePermits.get() <= 0) {
                this.availablePermits.set(acquirePermits(this.permitBatchSize));
            }

            c.output(c.element());
            availablePermits.getAndDecrement();
        }

        private int acquirePermits(int numPermits) throws IOException, InterruptedException {
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
                    retryCount++;
                    Thread.sleep(calculateBackoff(retryCount));
                    continue;
                }

                if (response.statusCode() != 200) {
                    failedRequests.inc();
                    retryCount++;
                    continue;
                }

                AcquireResponse acquireResponse = objectMapper.readValue(response.body(), AcquireResponse.class);
                if (acquireResponse.isAcquired()) {
                    return numPermits;
                } else {
                    throttledRequests.inc();
                    retryCount++;
                    long retryAfterMillis = 0;
                    try {
                        retryAfterMillis = acquireResponse.getRetryAfterMillis();
                    } catch (Exception e) {
                        throw new IOException("Error parsing rate limiter response: " + e.getMessage());
                    }
                    Thread.sleep(retryAfterMillis);
                }
            }
        }

        // Exponential Backoff for Failure
        private long calculateBackoff(int retryCount) {
            long backoff = (long) Math.min(MAX_SLEEP_MS, BASE_SLEEP_MS * Math.pow(2, retryCount));
            return backoff + (long) (Math.random() * backoff * 0.2);
        }
    }
}
