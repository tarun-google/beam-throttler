# Beam Throttler

## Overview

This project provides components for throttling elements within an Apache Beam pipeline. It offers two main approaches: a `ShuffleThrottler` that manages rate limiting within the pipeline using a shuffle and a `TokenBucketRateLimiter`, and an `ExternalThrottler` that communicates with an external rate-limiting service.

This can be useful in scenarios where you need to control the throughput of a pipeline to avoid overwhelming downstream systems.

## Features

*   **ShuffleThrottler**: A `PTransform` that throttles a `PCollection` by shuffling it and then applying a token bucket rate-limiting algorithm. This is useful for self-contained pipelines.
*   **ExternalThrottler**: A `PTransform` that throttles a `PCollection` by acquiring permits from an external HTTP-based rate-limiting service. This is useful when you need to coordinate throttling across multiple pipelines or systems.
*   **RateLimiterService**: A simple, standalone rate-limiting service built with Javalin that can be used in conjunction with the `ExternalThrottler`.
*   **TokenBucketRateLimiter**: A thread-safe token bucket rate limiter implementation.

## Throttlers

### ShuffleThrottler

The `ShuffleThrottler` is a `PTransform` that can be applied to any `PCollection`. It works by first assigning a random key to each element, grouping the elements by key (a shuffle), and then applying a `TokenBucketRateLimiter` to each group.

**Usage:**

```java
p.apply("ShuffleThrottle", new ShuffleThrottler<Long>(2).withBatchingDuration(Duration.standardSeconds(1)))
```

In this example, the `ShuffleThrottler` is configured to allow 2 elements per second.

### ExternalThrottler

The `ExternalThrottler` is a `PTransform` that relies on an external service to decide whether to throttle or not. It sends a request to the service to acquire a "permit" for a batch of elements. If the service denies the permit, the throttler will wait and retry.

**Usage:**

```java
p.apply("ExternalThrottleStreaming", new ExternalThrottler<KV<Integer, Long>>(10, "http://localhost:8080"))
```

In this example, the `ExternalThrottler` is configured to acquire permits in batches of 10 from a service running at `http://localhost:8080`.

## Rate Limiter Service

The project includes a simple `RateLimiterService` that can be used with the `ExternalThrottler`. It's a Javalin-based web service that exposes two endpoints:

*   `POST /acquire`: Acquires a number of permits.
*   `POST /rate`: Updates the rate of the rate limiter.

**Running the service:**

You can run the `RateLimiterService` from your IDE or by building a JAR and running it from the command line.

## Getting Started

To build and run the project, you will need:

*   Java 11 or later
*   Apache Maven

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/tannapareddy/beam-throttler.git
    cd beam-throttler
    ```
2.  **Build the project:**
    ```bash
    mvn clean install
    ```
3.  **Run the example pipelines:**
    You can run the example pipelines in `ThrottlerPipeline.java` from your IDE.

