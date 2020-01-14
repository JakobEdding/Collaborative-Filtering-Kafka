package com.bakdata.demo.processors;

import com.bakdata.demo.messages.FeatureMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class MFeatureCalculator extends AbstractProcessor<Integer, FeatureMessage> {
    private ProcessorContext context;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(final Integer partition, final FeatureMessage msg) {
        System.out.println("got this message in mfeaturecalc");
        System.out.println(msg);
    }

    @Override
    public void close() {}
}