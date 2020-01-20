package de.hpi.collaborativefilteringkafka.processors;

import de.hpi.collaborativefilteringkafka.apps.ALSApp;
import de.hpi.collaborativefilteringkafka.messages.FeatureMessage;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class UFeatureInitializer extends AbstractProcessor<Integer, String> {
    private ProcessorContext context;
    private Set<Integer> finishedPartitions;
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
    public void process(final Integer partition, final String eofMessage) {
        int finishedPartition = Integer.parseInt(eofMessage.split("_")[1]);
        this.finishedPartitions.add(finishedPartition);

        if (this.finishedPartitions.size() == ALSApp.NUM_PARTITIONS) {
            System.out.println(String.format("received EOF from all partitions on partition %d", partition));
//                        debugStuff();

            KeyValueIterator<Integer, ArrayList<Integer>> uInBlocksMidIterator = uInBlocksMidStore.all();
            KeyValueIterator<Integer, ArrayList<Short>> uInBlocksRatingsIterator = uInBlocksRatingsStore.all();

            while (uInBlocksMidIterator.hasNext()) {
                KeyValue<Integer, ArrayList<Integer>> userIdToMovieIds = uInBlocksMidIterator.next();
                KeyValue<Integer, ArrayList<Short>> userIdToRatings = uInBlocksRatingsIterator.next();

                float averageRating = (float) userIdToRatings.value.stream().mapToDouble(val -> val).average().orElse(1.0);
                ArrayList<Float> featureVector = new ArrayList<>(ALSApp.NUM_FEATURES);
                featureVector.add(averageRating);

                for(int i = 1; i < ALSApp.NUM_FEATURES; i++) {
                    featureVector.add((float) (ALSApp.MIN_RATING + Math.random() * (ALSApp.MAX_RATING - ALSApp.MIN_RATING)));
                }

                int userId = userIdToMovieIds.key;
                for(int targetPartition : this.uOutBlocksStore.get(userId)) {
                    context.forward(targetPartition, new FeatureMessage(userId, userIdToMovieIds.value, featureVector));
                }
            }
        }
    }

    private void debugStuff() {
        System.out.println("dumping store contents now");

        String[] storeNamesValueTypeInteger = new String[] {
                ALSApp.M_INBLOCKS_UID_STORE,
                ALSApp.U_INBLOCKS_MID_STORE
        };

        String[] storeNamesValueTypeShort = new String[] {
                ALSApp.M_INBLOCKS_RATINGS_STORE,
                ALSApp.M_OUTBLOCKS_STORE,
                ALSApp.U_INBLOCKS_RATINGS_STORE,
                ALSApp.U_OUTBLOCKS_STORE
        };

        for (String storeName : storeNamesValueTypeInteger) {
            KeyValueStore<Integer, ArrayList<Integer>> store = (KeyValueStore<Integer, ArrayList<Integer>>) this.context.getStateStore(storeName);
            KeyValueIterator<Integer, ArrayList<Integer>> iterator = store.all();
            System.out.println(storeName);
            while (iterator.hasNext()) {
                KeyValue<Integer, ArrayList<Integer>> kv = iterator.next();
                System.out.println(String.format("%d: %s", kv.key, kv.value.toString()));
            }
        }

        for (String storeName : storeNamesValueTypeShort) {
            KeyValueStore<Integer, ArrayList<Short>> store = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(storeName);
            KeyValueIterator<Integer, ArrayList<Short>> iterator = store.all();
            System.out.println(storeName);
            while (iterator.hasNext()) {
                KeyValue<Integer, ArrayList<Short>> kv = iterator.next();
                System.out.println(String.format("%d: %s", kv.key, kv.value.toString()));
            }
        }
    }

    @Override
    public void close() {}
}