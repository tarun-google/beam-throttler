package com.example.ExternalServiceThrottler;

import com.example.ExternalServiceThrottler.ExternalThrottlerDTOs.AcquireRequest;
import com.example.ExternalServiceThrottler.ExternalThrottlerDTOs.AcquireResponse;
import com.example.ExternalServiceThrottler.ExternalThrottlerDTOs.RateUpdateRequest;
import com.example.ExternalServiceThrottler.ExternalThrottlerDTOs.StatusResponse;
import com.example.TokenBucketRateLimiter;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;

public class RateLimiterService {

    private static final TokenBucketRateLimiter rateLimiter = TokenBucketRateLimiter.create(20, 20);

    public static void main(String[] args) {
        Javalin app = Javalin.create(config -> config.jsonMapper(new JavalinJackson())).start(8080);

        app.post("/acquire", ctx -> {
            AcquireRequest req = ctx.bodyAsClass(AcquireRequest.class);
            if (rateLimiter.tryAcquire(req.getPermits())) {
                ctx.json(new AcquireResponse(true, 0L));
            } else {
                long retryAfter = rateLimiter.getRetryAfterMillis(req.getPermits());
                ctx.json(new AcquireResponse(false, retryAfter));
            }
        });

        app.post("/rate", ctx -> {
            RateUpdateRequest req = ctx.bodyAsClass(RateUpdateRequest.class);
            rateLimiter.setRate(req.getPermitsPerSecond());
            ctx.json(new StatusResponse("rate updated"));
        });
    }
}
