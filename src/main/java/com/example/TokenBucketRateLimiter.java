package com.example;

public class TokenBucketRateLimiter {
    private double permitsPerSecond;
    private double capacity;
    private long lastRefillTimestamp;
    private double availablePermits;

    private TokenBucketRateLimiter(double permitsPerSecond) {
        this.setRate(permitsPerSecond);
        this.lastRefillTimestamp = System.nanoTime();
        this.availablePermits = this.capacity;
    }

    public static TokenBucketRateLimiter create(double permitsPerSecond) {
        return new TokenBucketRateLimiter(permitsPerSecond);
    }

    public synchronized boolean tryAcquire(int permits) {
        refill();
        if (availablePermits >= permits) {
            availablePermits -= permits;
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

    public long getRetryAfterMillis(int permits) {
        refill();
        double permitsNeeded = permits - availablePermits;
        if (permitsNeeded <= 0) {
            return 0;
        }
        double secondsToWait = permitsNeeded / permitsPerSecond;
        return (long) (secondsToWait * 1000) + 1;
    }

    public synchronized void setRate(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
        this.capacity = Math.max(1.0, permitsPerSecond);
    }

    private synchronized void refill() {
        long now = System.nanoTime();
        long elapsedNanos = now - lastRefillTimestamp;
        double permitsToAdd = elapsedNanos / 1_000_000_000.0 * permitsPerSecond;
        availablePermits = Math.min(capacity, availablePermits + permitsToAdd);
        lastRefillTimestamp = now;
    }
}
