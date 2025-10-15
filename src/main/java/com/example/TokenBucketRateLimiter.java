package com.example;

public class TokenBucketRateLimiter {
    private long permitsPerSecond;
    private long capacity;
    private long lastRefillTimestamp;
    private long availablePermits;

    private TokenBucketRateLimiter(long permitsPerSecond, long capacity) {
        this.setRate(permitsPerSecond);
        this.lastRefillTimestamp = 0;
        this.availablePermits = this.capacity;
    }

    public static TokenBucketRateLimiter create(long permitsPerSecond, long capacity) {
        return new TokenBucketRateLimiter(permitsPerSecond, capacity);
    }

    public boolean tryAcquire(long permits) {
        refill();
        System.out.println(availablePermits);
        if (availablePermits >= permits) {
            availablePermits -= permits;
            return true;
        }
        return false;
    }

    public void acquire(long permits) throws InterruptedException {
        boolean acquired = false;
        while(!acquired){
            if(tryAcquire(permits)){
                acquired = true;
            } else{
                long waitTimeMillis = getRetryAfterMillis(permits);
                System.out.println("Throttled. Retrying after"+waitTimeMillis+"ms");
                if(waitTimeMillis >0){
                    Thread.sleep(waitTimeMillis);
                }
            }
        }
    }

    public long getRetryAfterMillis(long permits) {
        refill();
        double permitsNeeded = permits - availablePermits;
        if (permitsNeeded <= 0) {
            return 0;
        }
        double secondsToWait = permitsNeeded / permitsPerSecond;
        return (long) (secondsToWait * 1000) + 1;
    }

    public void setRate(long permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
    }

    private void refill() {
        long now = System.nanoTime();
        long elapsedNanos = now - lastRefillTimestamp;
        long permitsToAdd = (long) (elapsedNanos / 1_000_000_000.0 * permitsPerSecond);
        availablePermits = Math.min(capacity, availablePermits + permitsToAdd);
        lastRefillTimestamp = now;
    }
}
