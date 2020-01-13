package com.bakdata.demo.processors;

import com.bakdata.demo.messages.FeatureMessage;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public final class MFeatureCalculator {
    public static class MyProcessorSupplier implements ProcessorSupplier<Integer, FeatureMessage> {

        @Override
        public Processor<Integer, FeatureMessage> get() {
            return new Processor<Integer, FeatureMessage>() {
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
            };
        }
    }
}