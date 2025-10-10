package com.example.ShuffleThrottler;


import com.example.TokenBucketRateLimiter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.util.Random;

public class ShuffleThrottler<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private final double permitsPerSecond;
    private final int numberOfShards = 1;
    private Duration batchingDuration;

    public ShuffleThrottler(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
    }

    public ShuffleThrottler<T> withBatchingDuration(Duration batchingDuration) {
        this.batchingDuration = batchingDuration;
        return this;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        PCollection<KV<Integer, T>> keyedInput = input
                .apply("AssignRandomKey", ParDo.of(new AssignRandomKey<T>(this.numberOfShards)));

        PCollection<KV<Integer, Iterable<T>>> groupedInput = null;
        if (input.isBounded() == PCollection.IsBounded.UNBOUNDED &&
                input.getWindowingStrategy().getWindowFn() instanceof GlobalWindows) {
            groupedInput = keyedInput
                    .apply("StreamingWindow", Window.<KV<Integer, T>>into(new GlobalWindows())
                            .triggering(Repeatedly.forever(
                                    AfterProcessingTime.pastFirstElementInPane()
                                            .plusDelayOf(this.batchingDuration)))
                            .withAllowedLateness(Duration.ZERO)
                            .discardingFiredPanes())
                    .apply("GroupIntoShards", GroupByKey.<Integer, T>create());
        } else{
            groupedInput = keyedInput.apply("GroupIntoShards", GroupByKey.<Integer, T>create());
        }
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

        private final double permitsPerSecond;
        private final int numberOfShards;
        private transient TokenBucketRateLimiter rateLimiter;

        public RateLimitingGroupDoFn(double permitsPerSecond, int numberOfShards) {
            this.permitsPerSecond = permitsPerSecond;
            this.numberOfShards = numberOfShards;
        }

        @Setup
        public void setup() {
            double ratePerShard = permitsPerSecond / numberOfShards;
            this.rateLimiter = TokenBucketRateLimiter.create(ratePerShard);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws InterruptedException {
            for (T element : c.element().getValue()) {
                rateLimiter.acquire(1);
                c.output(element);
            }
        }
    }
}
