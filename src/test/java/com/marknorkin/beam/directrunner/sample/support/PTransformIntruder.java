package com.marknorkin.beam.directrunner.sample.support;

import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@RequiredArgsConstructor
public class PTransformIntruder extends PTransform<PCollection<String>, PDone> {

    private final SingleOutput<String, String> parDo;
    private final PTransform<PCollection<String>, PDone> transform;

    @Override
    public PDone expand(PCollection<String> input) {

        return input
            .apply("Do something additional you want", parDo)
            .apply("Continue doing what you should", transform);
    }
}
