package com.example;

public class TokenBucketRateLimiter {
    private int permitsPerSecond;
    private int capacity;
    private long lastRefillTimestamp;
    private double availablePermits;

    private TokenBucketRateLimiter(int permitsPerSecond, int capacity) {
        this.setRate(permitsPerSecond);
        this.lastRefillTimestamp = 0;
        this.availablePermits = 0;
        this.capacity = capacity;
    }

    public static TokenBucketRateLimiter create(int permitsPerSecond, int capacity) {
        return new TokenBucketRateLimiter(permitsPerSecond, capacity);
    }

    public boolean tryAcquire(int permits) {
        refill();
        if (this.availablePermits >= permits) {
            this.availablePermits -= permits;
            return true;
        }
        return false;
    }

    public void acquire(int permits) throws InterruptedException {
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

    public void acquire() throws  InterruptedException {
        acquire(1);
    }

    public long getRetryAfterMillis(int permits) {
        refill();
        double permitsNeeded = permits - this.availablePermits;
        if (permitsNeeded <= 0) {
            return 0;
        }
        double secondsToWait = permitsNeeded / this.permitsPerSecond;
        return (long) (secondsToWait * 1000) + 1;
    }

    public void setRate(int permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
    }

    private void refill() {
        long now = System.nanoTime();
        long elapsedNanos = now - this.lastRefillTimestamp;
        double permitsToAdd = elapsedNanos / 1_000_000_000.0 * permitsPerSecond;
        this.availablePermits = Math.min(this.capacity, this.availablePermits + permitsToAdd);
        this.lastRefillTimestamp = now;
    }
}
