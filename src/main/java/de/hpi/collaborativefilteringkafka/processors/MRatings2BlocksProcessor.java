package de.hpi.collaborativefilteringkafka.processors;

import de.hpi.collaborativefilteringkafka.apps.ALSApp;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class MRatings2BlocksProcessor extends AbstractProcessor<Integer, String> {
    private ProcessorContext context;
    private KeyValueStore<Integer, ArrayList<Integer>> mInBlocksUidStore;
    private KeyValueStore<Integer, ArrayList<Short>> mInBlocksRatingsStore;
    private KeyValueStore<Integer, ArrayList<Short>> mOutBlocksStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
        this.mInBlocksUidStore = (KeyValueStore<Integer, ArrayList<Integer>>) this.context.getStateStore(ALSApp.M_INBLOCKS_UID_STORE);
        this.mInBlocksRatingsStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.M_INBLOCKS_RATINGS_STORE);
        this.mOutBlocksStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.M_OUTBLOCKS_STORE);
    }

    @Override
    public void process(final Integer movieId, final String ratingsForOneMovie) {
        if (ratingsForOneMovie.equals("EOF")) {
            this.context.forward(movieId, ratingsForOneMovie);
            this.context.commit();
            return;
        }
        System.out.println(String.format("MRatings2BlocksProcessor - processing key: %d value: %s", movieId, ratingsForOneMovie));

        ArrayList<Integer> userIds = new ArrayList<>();
        ArrayList<Short> ratings = new ArrayList<>();
        Set<Short> partitions = new HashSet<>();
        for (String userIdRatingPair : ratingsForOneMovie.split(";")) {
            String[] split = userIdRatingPair.split(",");
            int userId = Integer.parseInt(split[0]);
            userIds.add(userId);
            ratings.add(Short.parseShort(split[1]));
            Integer partitionInt = userId % ALSApp.NUM_PARTITIONS;
            partitions.add(partitionInt.shortValue());
        }

        this.mInBlocksUidStore.put(movieId, userIds);
        this.mInBlocksRatingsStore.put(movieId, ratings);
        ArrayList<Short> partitionsList = new ArrayList<>(partitions);
        this.mOutBlocksStore.put(movieId, partitionsList);

        for (String userIdRatingPair : ratingsForOneMovie.split(";")) {
            // TODO: does this partition?
            this.context.forward(
                Integer.parseInt(userIdRatingPair.split(",")[0]),
                movieId + "," + userIdRatingPair.split(",")[1]
            );
        }

        // TODO: commit periodically rather than after every record for better performance?
        this.context.commit();
    }

    @Override
    public void close() {}
}