package com.bakdata.demo;

import com.bakdata.demo.apps.ALSApp;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.HashSet;
import java.util.Set;

public final class UFeatureCalculator {
    public static class MyProcessorSupplier implements ProcessorSupplier<Integer, String> {

        @Override
        public Processor<Integer, String> get() {
            return new Processor<Integer, String>() {
                private ProcessorContext context;
                private Set<Integer> finishedPartitions;

                @Override
                @SuppressWarnings("unchecked")
                public void init(final ProcessorContext context) {
                    this.context = context;
                    this.finishedPartitions = new HashSet<>();
                }

                @Override
                public void process(final Integer partition, final String eofMessage) {
                    int finishedPartition = Integer.parseInt(eofMessage.split("_")[1]);
                    this.finishedPartitions.add(finishedPartition);

                    if (this.finishedPartitions.size() == ALSApp.NUM_PARTITIONS) {
                        System.out.println(String.format("received EOF from all partitions on partition %d", partition));
                    }
                }

                @Override
                public void close() {}
            };
        }
    }
}