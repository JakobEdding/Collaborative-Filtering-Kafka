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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public final class MRatings2BlocksProcessor {
    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
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
//                    if (ratingsForOneMovie.equals("EOF")) {
//                        this.context.commit();
//                        return;
//                    }
                    System.out.println(String.format("MRatings2BlocksProcessor - processing key: %d value: %s", movieId, ratingsForOneMovie));

                    ArrayList<Integer> userIds = new ArrayList<>();
                    ArrayList<Short> ratings = new ArrayList<>();
                    Set<Short> partitions = new HashSet<>();
                    for (String userIdRatingPair : ratingsForOneMovie.split(";")) {
                        String[] split = userIdRatingPair.split(",");
                        int userId = Integer.parseInt(split[0]);
                        userIds.add(userId);
                        ratings.add(Short.parseShort(split[1]));
                        byte[] keyBytes = Serdes.Integer().serializer().serialize("doesntmatter", userId);
                        Integer partitionInt = Utils.toPositive(Utils.murmur2(keyBytes)) % ALSApp.NUM_PARTITIONS;
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
            };
        }
    }
}