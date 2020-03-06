package de.hpi.collaborativefilteringkafka.processors;

import de.hpi.collaborativefilteringkafka.apps.ALSApp;
import de.hpi.collaborativefilteringkafka.messages.IdRatingPairMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

public class MRatings2BlocksProcessor extends AbstractProcessor<Integer, IdRatingPairMessage> {
    private ProcessorContext context;
    private KeyValueStore<Integer, ArrayList<Integer>> mInBlocksUidStore;
    private KeyValueStore<Integer, ArrayList<Short>> mInBlocksRatingsStore;
    private KeyValueStore<Integer, ArrayList<Short>> mOutBlocksStore;
    private HashSet<Integer> movieIdAgg;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
        this.mInBlocksUidStore = (KeyValueStore<Integer, ArrayList<Integer>>) this.context.getStateStore(ALSApp.M_INBLOCKS_UID_STORE);
        this.mInBlocksRatingsStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.M_INBLOCKS_RATINGS_STORE);
        this.mOutBlocksStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.M_OUTBLOCKS_STORE);
        this.movieIdAgg = new HashSet<>();
    }

    @Override
    public void process(final Integer movieId, final IdRatingPairMessage userIdRatingPairMsg) {
        if (userIdRatingPairMsg.id == -1) {
            System.out.println(String.format(
                    "Got EOF on MRatings2BlocksProcessor for partition %d at %s with this many movies: %d",
                    context.partition(),
                    new Timestamp(System.currentTimeMillis()),
                    this.movieIdAgg.size()
            ));
            for(int partition = 0; partition < ALSApp.NUM_PARTITIONS; partition++) {
                this.context.forward(partition, new IdRatingPairMessage(-1, (short) context.partition()));
            }
            this.context.commit();
            return;
        }
        this.movieIdAgg.add(movieId);

        int userId = userIdRatingPairMsg.id;
        short rating = userIdRatingPairMsg.rating;

        short partition = (short) (userId % ALSApp.NUM_PARTITIONS);

        ArrayList<Integer> userIds = this.mInBlocksUidStore.get(movieId);
        ArrayList<Short> ratings = this.mInBlocksRatingsStore.get(movieId);
        ArrayList<Short> partitions = this.mOutBlocksStore.get(movieId);
        if (userIds == null) {
            userIds =  new ArrayList<>(Collections.singletonList(userId));
            ratings =  new ArrayList<>(Collections.singletonList(rating));
            partitions =  new ArrayList<>(Collections.singletonList(partition));
        } else {
            userIds.add(userId);
            ratings.add(rating);
            if(!partitions.contains(partition)) {
                partitions.add(partition);
            }
        }
        this.mInBlocksUidStore.put(movieId, userIds);
        this.mInBlocksRatingsStore.put(movieId, ratings);
        this.mOutBlocksStore.put(movieId, partitions);

        this.context.forward(userId, new IdRatingPairMessage(movieId, rating));
    }

    @Override
    public void close() {}
}