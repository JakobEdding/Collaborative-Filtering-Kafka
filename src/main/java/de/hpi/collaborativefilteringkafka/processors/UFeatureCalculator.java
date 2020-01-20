package de.hpi.collaborativefilteringkafka.processors;

import de.hpi.collaborativefilteringkafka.messages.FeatureMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.ArrayList;
import java.util.HashMap;

public class UFeatureCalculator extends AbstractProcessor<Integer, FeatureMessage> {
    private ProcessorContext context;
    private HashMap<Integer, HashMap<Integer, ArrayList<Float>>> movieIdToUserFeatureVectors;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;

        this.movieIdToUserFeatureVectors = new HashMap<>();
    }

    @Override
    public void process(final Integer partition, final FeatureMessage msg) {
        System.out.println(String.format("Received in UFeatureCalc on partition %d this message: %s", partition, msg.toString()));
    }

    @Override
    public void close() {}
}