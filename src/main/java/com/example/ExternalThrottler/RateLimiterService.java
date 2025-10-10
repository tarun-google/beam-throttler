package com.example.ExternalThrottler;

import com.example.ExternalThrottler.ExternalThrottlerDTOs.AcquireRequest;
import com.example.ExternalThrottler.ExternalThrottlerDTOs.AcquireResponse;
import com.example.ExternalThrottler.ExternalThrottlerDTOs.RateUpdateRequest;
import com.example.ExternalThrottler.ExternalThrottlerDTOs.StatusResponse;
import com.example.TokenBucketRateLimiter;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;

public class RateLimiterService {

    private static final TokenBucketRateLimiter rateLimiter = TokenBucketRateLimiter.create(20.0);

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
