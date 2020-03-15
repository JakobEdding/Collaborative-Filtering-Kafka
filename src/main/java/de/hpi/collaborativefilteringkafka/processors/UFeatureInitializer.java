package de.hpi.collaborativefilteringkafka.processors;

import de.hpi.collaborativefilteringkafka.apps.ALSApp;
import de.hpi.collaborativefilteringkafka.messages.FeatureMessage;
import de.hpi.collaborativefilteringkafka.messages.IdRatingPairMessage;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class UFeatureInitializer extends AbstractProcessor<Integer, IdRatingPairMessage> {
    private ProcessorContext context;
    private Set<Short> finishedPartitions;
    private KeyValueStore<Integer, ArrayList<Integer>> uInBlocksMidStore;
    private KeyValueStore<Integer, ArrayList<Short>> uInBlocksRatingsStore;
    private KeyValueStore<Integer, ArrayList<Short>> uOutBlocksStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
        this.finishedPartitions = new HashSet<>();

        this.uInBlocksMidStore = (KeyValueStore<Integer, ArrayList<Integer>>) this.context.getStateStore(ALSApp.U_INBLOCKS_MID_STORE);
        this.uInBlocksRatingsStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.U_INBLOCKS_RATINGS_STORE);
        this.uOutBlocksStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.U_OUTBLOCKS_STORE);
    }

    @Override
    public void process(final Integer partition, final IdRatingPairMessage eofMessage) {
        this.finishedPartitions.add(eofMessage.rating);

        if (this.finishedPartitions.size() != ALSApp.NUM_PARTITIONS) {
            return;
        }

        KeyValueIterator<Integer, ArrayList<Integer>> uInBlocksMidIterator = uInBlocksMidStore.all();
        KeyValueIterator<Integer, ArrayList<Short>> uInBlocksRatingsIterator = uInBlocksRatingsStore.all();

        while (uInBlocksMidIterator.hasNext()) {
            KeyValue<Integer, ArrayList<Integer>> userIdToMovieIds = uInBlocksMidIterator.next();
            KeyValue<Integer, ArrayList<Short>> userIdToRatings = uInBlocksRatingsIterator.next();

            float averageRating = (float) userIdToRatings.value.stream().mapToDouble(val -> val).average().orElse(1.0);
            float[] featureVector = new float[ALSApp.NUM_FEATURES];
            featureVector[0] = averageRating;

            for(int i = 1; i < ALSApp.NUM_FEATURES; i++) {
                featureVector[i] = (float) Math.random();
            }

            int userId = userIdToMovieIds.key;
            FeatureMessage featureMsgToBeSent = new FeatureMessage(userId, null, featureVector);

            for(int targetPartition : this.uOutBlocksStore.get(userId)) {
                featureMsgToBeSent.setDependentIds((ArrayList<Integer>) userIdToMovieIds.value.stream().filter(id -> (id % ALSApp.NUM_PARTITIONS) == targetPartition).collect(Collectors.toList()));
                context.forward(targetPartition, featureMsgToBeSent);
            }
        }
    }

    @Override
    public void close() {}
}