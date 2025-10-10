package com.example;

import com.example.ExternalThrottler.ExternalThrottler;
import com.example.ShuffleThrottler.ShuffleThrottler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

public class ThrottlerPipeline {

    public static void main(String[] args) {
           //runStreamingTest();
           //runBatchTest();
           //runWindowKVTest();
    }


    public static void runStreamingTest() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        p.apply("GenerateStreamingData", GenerateSequence.from(0).withRate(5, Duration.standardSeconds(1)))
                .apply("ShuffleThrottle", new ShuffleThrottler<Long>(2).withBatchingDuration(Duration.standardSeconds(1)))
               // .apply("ExternalThrottleStreaming", new ExternalThrottler<Long>(1, "http://localhost:8080"))
                .apply("PrintStreamingElements", ParDo.of(new DoFn<Long, Void>() {
                    @ProcessElement
                    public void processElement(@Element Long element) {
                        System.out.println("Streaming element: " + element);
                    }
                }));

        p.run();
    }

    public static void runBatchTest() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        p.apply("GenerateBatchData", GenerateSequence.from(0).to(100))
                .apply("AddKeyForBatch", ParDo.of(new DoFn<Long, KV<Integer, Long>>() {
                    @ProcessElement
                    public void processElement(@Element Long element, OutputReceiver<KV<Integer, Long>> out) {
                        out.output(KV.of((int) (element % 2) + 1, element)); // Single key for batch throttling
                    }
                }))
                // .apply("ShuffleThrottleBatch", new ShuffleThrottler<KV<Integer, Long>>(10))
                .apply("ExternalThrottleStreaming", new ExternalThrottler<KV<Integer, Long>>(10, "http://localhost:8080"))
                .apply("PrintBatchElements", ParDo.of(new DoFn<KV<Integer, Long>, Void>() {
                    @ProcessElement
                    public void processElement(@Element KV<Integer, Long> element) {
                        System.out.println("Batch element: " + element.getValue());
                    }
                }));
        p.run();
    }

    public static void runWindowKVTest() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        p.apply("GenerateWindowData", GenerateSequence.from(0).withRate(10, Duration.standardSeconds(1)))
                .apply("ApplyWindowing", Window.<Long>into(FixedWindows.of(Duration.standardSeconds(5))))
                .apply("AddKeyForWindow", ParDo.of(new DoFn<Long, KV<Integer, Long>>() {
                    @ProcessElement
                    public void processElement(@Element Long element, OutputReceiver<KV<Integer, Long>> out) {
                        out.output(KV.of((int) (element % 10) + 1, element));
                    }
                }))
                .apply("ShuffleThrottleWindow", new ShuffleThrottler<KV<Integer, Long>>(5))
                .apply("PrintWindowElements", ParDo.of(new DoFn<KV<Integer, Long>, Void>() {
                    @ProcessElement
                    public void processElement(@Element KV<Integer, Long> element) {
                        System.out.println("Windowed element: " + element.getValue());
                    }
                }));
        p.run();
    }
}
