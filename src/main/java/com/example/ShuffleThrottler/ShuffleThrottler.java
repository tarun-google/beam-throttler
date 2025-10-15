package com.example.ShuffleThrottler;


import com.example.TokenBucketRateLimiter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.Random;

public class ShuffleThrottler<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private final int permitsPerSecond;
    private final int numberOfShards;
    private final Long batchSize;
    private final Duration batchDuration;
    private static final Long DEFAULT_SIZE = 10L;

    private ShuffleThrottler(int permitsPerSecond, int numberOfShards, Long batchSize, Duration batchDuration) {
        this.permitsPerSecond = permitsPerSecond;
        this.numberOfShards = numberOfShards;
        this.batchSize = batchSize;
        this.batchDuration = batchDuration;
    }

    public static <T> ShuffleThrottler<T> of(int permitsPerSecond, int numberOfShards) {
        return new ShuffleThrottler<>(permitsPerSecond, numberOfShards, null, null);
    }

    public ShuffleThrottler<T> withBatchSize(long batchSize){
        return new ShuffleThrottler<>(this.permitsPerSecond, this.numberOfShards, batchSize, this.batchDuration);
    }

    public ShuffleThrottler<T> withBatchDuration(Duration batchDuration){
        return new ShuffleThrottler<>(this.permitsPerSecond, this.numberOfShards, this.batchSize, batchDuration);
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        PCollection<KV<Integer, T>> keyedInput = input
                .apply("AssignRandomKey", ParDo.of(new AssignRandomKey<T>(this.numberOfShards)));

        GroupIntoBatches<Integer, T> groupIntoBatches = GroupIntoBatches.ofSize(DEFAULT_SIZE);
        if (batchSize != null) {
            groupIntoBatches = groupIntoBatches.withByteSize(batchSize);
        }if (batchDuration != null) {
            groupIntoBatches = groupIntoBatches.withMaxBufferingDuration(batchDuration);
        }
        PCollection<KV<Integer, Iterable<T>>> groupedInput = keyedInput.apply("GroupIntoBatches", groupIntoBatches);

        return groupedInput.apply("RateLimitGroups", ParDo.of(new RateLimitingGroupDoFn<T>(this.permitsPerSecond, this.numberOfShards)));
    }

    private static class AssignRandomKey<T> extends DoFn<T, KV<Integer, T>> {
        private final int shards;
        private transient Random random;

        public AssignRandomKey(int shards) {
            this.shards = shards;
        }

        @Setup
        public void setup() {
            this.random = new Random();
        }

        @ProcessElement
        public void processElement(@Element T element, OutputReceiver<KV<Integer, T>> out) {
            out.output(KV.of(random.nextInt(shards), element));
        }
    }

    private static class RateLimitingGroupDoFn<T> extends DoFn<KV<Integer, Iterable<T>>, T> {

        private final int permitsPerSecond;
        private final int numberOfShards;
        private transient TokenBucketRateLimiter rateLimiter;

        public RateLimitingGroupDoFn(int permitsPerSecond, int numberOfShards) {
            this.permitsPerSecond = permitsPerSecond;
            this.numberOfShards = numberOfShards;
        }

        @Setup
        public void setup() {
            int ratePerShard = permitsPerSecond / numberOfShards;
            this.rateLimiter = TokenBucketRateLimiter.create(ratePerShard, 10);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws InterruptedException {
            for (T element : c.element().getValue()) {
                rateLimiter.acquire();
                c.output(element);
            }
        }
    }
}
