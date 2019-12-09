package com.bakdata.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Properties;

public final class MovieRatingsToKVStoreProcessor {
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

    static class MyProcessorSupplier implements ProcessorSupplier<String, String> {

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;
                private KeyValueStore<Integer, ByteBuffer> kvStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(final ProcessorContext context) {
                    this.context = context;
                    this.kvStore = (KeyValueStore<Integer, ByteBuffer>) this.context.getStateStore("ratingsForMovies");
                }

                @Override
                public void process(final String movieId, final String ratingsForOneMovie) {
                    // phase 1: write movieId rating vectors to store for **movie perspective**
                    ArrayList<Integer> ratings = new ArrayList<>();
                    for (String userIdRatingPair : ratingsForOneMovie.split(";")) {
                        ratings.add(Integer.parseInt(userIdRatingPair.split(",")[1]));
                    }

                    ByteBuffer bb = ByteBuffer.allocate(4 * ratings.size());
                    for (int rating : ratings) {
                        bb.putInt(rating);
                    }

                    this.kvStore.put(Integer.parseInt(movieId), bb);

                    // phase 2: turn around ratings to have userId,movieId,rating triples for **user perspective**
                    for (String userIdRatingPair : ratingsForOneMovie.split(";")) {
                        this.context.forward(
                            userIdRatingPair.split(",")[0],
                            movieId + "," + userIdRatingPair.split(",")[1]
                        );
                    }

                    // phase 3: commit
                    // TODO: commit periodically rather than after every record for better performance?
                    this.context.commit();
                }

                @Override
                public void close() {}
            };
        }
    }
}