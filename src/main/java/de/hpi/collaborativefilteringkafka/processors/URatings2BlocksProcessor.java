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
import java.util.Set;

public class URatings2BlocksProcessor extends AbstractProcessor<Integer, IdRatingPairMessage> {
    private ProcessorContext context;
    private KeyValueStore<Integer, ArrayList<Integer>> uInBlocksMidStore;
    private KeyValueStore<Integer, ArrayList<Short>> uInBlocksRatingsStore;
    private HashSet<Integer> userIdAgg;
    private Set<Short> finishedPartitions;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
        this.uInBlocksMidStore = (KeyValueStore<Integer, ArrayList<Integer>>) this.context.getStateStore(ALSApp.U_INBLOCKS_MID_STORE);
        this.uInBlocksRatingsStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.U_INBLOCKS_RATINGS_STORE);
        this.userIdAgg = new HashSet<>();
        this.finishedPartitions = new HashSet<>();

//        this.context.schedule(Duration.ofSeconds(2), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
//            this.context.commit();
//        });
    }

    @Override
    public void process(final Integer userId, final IdRatingPairMessage movieIdRatingPairMsg) {
        if (movieIdRatingPairMsg.id == -1) {
//            System.out.println(String.format(
//                    "Got EOF on URatings2BlocksProcessor for partition %d at %s with this many users: %d; uInBlocksMidStore approx num entries: %d; uInBlocksRatingsStore approx num entries: %d; uOutBlocksStore approx num entries: %d",
//                    context.partition(),
//                    new Timestamp(System.currentTimeMillis()),
//                    this.userIdAgg.size(),
//                    this.uInBlocksMidStore.approximateNumEntries(),
//                    this.uInBlocksRatingsStore.approximateNumEntries(),
//                    this.uOutBlocksStore.approximateNumEntries()
//            ));
            System.out.println(String.format(
                    "Got EOF on URatings2BlocksProcessor for partition %d at %s with this many users: %d",
                    context.partition(),
                    new Timestamp(System.currentTimeMillis()),
                    this.userIdAgg.size()
            ));
            this.finishedPartitions.add(movieIdRatingPairMsg.rating);

            if (this.finishedPartitions.size() == ALSApp.NUM_PARTITIONS) {
                for (int partition = 0; partition < ALSApp.NUM_PARTITIONS; partition++) {
                    this.context.forward(partition, new IdRatingPairMessage(-1, (short) context.partition()));
                }
                this.context.commit();
            }

            return;
        }

        this.userIdAgg.add(userId);

//        System.out.println(String.format("URatings2BlocksProcessor - processing key: %d value: %s", userId, movieIdRatingPairMsg));

        int movieId = movieIdRatingPairMsg.id;
        short rating = movieIdRatingPairMsg.rating;
        short partition = (short) (movieId % ALSApp.NUM_PARTITIONS);

        ArrayList<Integer> movieIds = this.uInBlocksMidStore.get(userId);
        ArrayList<Short> ratings = this.uInBlocksRatingsStore.get(userId);
        if (movieIds == null) {
            movieIds =  new ArrayList<>(Collections.singletonList(movieId));
            ratings =  new ArrayList<>(Collections.singletonList(rating));
        } else {
            movieIds.add(movieId);
            ratings.add(rating);
        }
        this.uInBlocksMidStore.put(userId, movieIds);
        this.uInBlocksRatingsStore.put(userId, ratings);

//        this.context.commit();
    }

    @Override
    public void close() {}
}