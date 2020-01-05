package com.bakdata.demo;

import com.bakdata.demo.apps.ALSApp;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;

public final class URatings2BlocksProcessor {
    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public static class MyProcessorSupplier implements ProcessorSupplier<Integer, String> {

        @Override
        public Processor<Integer, String> get() {
            return new Processor<Integer, String>() {
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
//                    if (ratingsForOneMovie.equals("EOF")) {
//                        this.context.commit();
//                        return;
//                    }
                    System.out.println(String.format("URatings2BlocksProcessor - processing key: %d value: %s", userId, movieIdRatingPair));

                    String[] split = movieIdRatingPair.split(",");

                    int movieId = Integer.parseInt(split[0]);
                    short rating = Short.parseShort(split[1]));
                    byte[] keyBytes = Serdes.Integer().serializer().serialize("doesntmatter", movieId);
                    Integer partitionInt = Utils.toPositive(Utils.murmur2(keyBytes)) % ALSApp.NUM_PARTITIONS;
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

                    // TODO:
                    for (String movieIdRatingPair : ratingsForOneMovie.split(";")) {
                        // TODO: does this partition?
                        this.context.forward(
                                Integer.parseInt(movieIdRatingPair.split(",")[0]),
                                movieId + "," + movieIdRatingPair.split(",")[1]
                        );
                    }

                    // TODO: commit periodically rather than after every record for better performance?
                    this.context.commit();
                }

                @Override
                public void close() {}
            };
        }
    }
}