package de.hpi.collaborativefilteringkafka.processors;

import de.hpi.collaborativefilteringkafka.apps.ALSApp;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Arrays;

public class URatings2BlocksProcessor extends AbstractProcessor<Integer, String> {
    private ProcessorContext context;
    private KeyValueStore<Integer, ArrayList<Integer>> uInBlocksMidStore;
    private KeyValueStore<Integer, ArrayList<Short>> uInBlocksRatingsStore;
    private KeyValueStore<Integer, ArrayList<Short>> uOutBlocksStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
        this.uInBlocksMidStore = (KeyValueStore<Integer, ArrayList<Integer>>) this.context.getStateStore(ALSApp.U_INBLOCKS_MID_STORE);
        this.uInBlocksRatingsStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.U_INBLOCKS_RATINGS_STORE);
        this.uOutBlocksStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.U_OUTBLOCKS_STORE);
    }

    @Override
    public void process(final Integer userId, final String movieIdRatingPair) {
        if (movieIdRatingPair.equals("EOF")) {
            for(int partition = 0; partition < ALSApp.NUM_PARTITIONS; partition++) {
                this.context.forward(partition, String.format("EOF-for-partition_%d", context.partition()));
            }
            this.context.commit();
            return;
        }

        System.out.println(String.format("URatings2BlocksProcessor - processing key: %d value: %s", userId, movieIdRatingPair));

        String[] split = movieIdRatingPair.split(",");

        int movieId = Integer.parseInt(split[0]);
        short rating = Short.parseShort(split[1]);
        Integer partitionInt = movieId % ALSApp.NUM_PARTITIONS;
        short partition = partitionInt.shortValue();

        ArrayList<Integer> movieIds = this.uInBlocksMidStore.get(userId);
        ArrayList<Short> ratings = this.uInBlocksRatingsStore.get(userId);
        ArrayList<Short> partitions = this.uOutBlocksStore.get(userId);
        if (movieIds == null) {
            movieIds =  new ArrayList<>(Arrays.asList(movieId));
            ratings =  new ArrayList<>(Arrays.asList(rating));
            partitions =  new ArrayList<>(Arrays.asList(partition));
        } else {
            movieIds.add(movieId);
            ratings.add(rating);
            partitions.add(partition);
        }
        this.uInBlocksMidStore.put(userId, movieIds);
        this.uInBlocksRatingsStore.put(userId, ratings);
        this.uOutBlocksStore.put(userId, partitions);

        this.context.commit();
    }

    @Override
    public void close() {}
}