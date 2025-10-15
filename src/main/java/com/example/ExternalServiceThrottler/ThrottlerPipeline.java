package com.example.ExternalServiceThrottler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;

import java.io.IOException;

public class ThrottlerPipeline {


    private static final String RATE_LIMITER_URL = "http://localhost:8080";

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        p.apply("GenerateUnboundedData", GenerateSequence.from(0).withRate(10, Duration.standardSeconds(1)))
                .apply("ProcessWithExternalThrottling", ParDo.of(new DoFn<Long, String>() {

                    private transient ExternalThrottler throttler;

                    @Setup
                    public void setup() {
                        this.throttler = ExternalThrottler.of(RATE_LIMITER_URL, 10);
                    }

                    @ProcessElement
                    public void processElement(@Element Long element, OutputReceiver<String> out) throws IOException, InterruptedException {
                        throttler.waitAndAcquire();
                        String output = "Processing element: " + element;
                        System.out.println(output);
                        out.output(output);
                    }
                }));

        p.run().waitUntilFinish();
    }
}