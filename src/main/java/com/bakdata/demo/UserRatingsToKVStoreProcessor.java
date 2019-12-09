package com.bakdata.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;
import java.util.Properties;

public final class UserRatingsToKVStoreProcessor {
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

    static class MyProcessorSupplier implements ProcessorSupplier<Integer, String> {

        @Override
        public Processor<Integer, String> get() {
            return new Processor<Integer, String>() {
                private ProcessorContext context;
                private KeyValueStore<Integer, ByteBuffer> kvStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(final ProcessorContext context) {
                    this.context = context;
                    this.kvStore = (KeyValueStore<Integer, ByteBuffer>) this.context.getStateStore("ratingsForUsers");
                }

                @Override
                public void process(final Integer userId, final String movieIdRatingPair) {
                    int rating = Integer.parseInt(movieIdRatingPair.split(",")[1]);

                    ByteBuffer oldBb = this.kvStore.get(userId);
                    int newBbSize = 4;
                    if (oldBb != null) {
                        newBbSize += oldBb.capacity();
                    }

                    ByteBuffer newBb = ByteBuffer.allocate(newBbSize);
                    if (oldBb != null) {
                        for (int i = 0; i < oldBb.capacity(); i += 4) {
                            newBb.putInt(oldBb.getInt(i));
                        }
                    }
                    newBb.putInt(rating);

                    // TODO: partition store?
                    this.kvStore.put(userId, newBb);

                    this.context.commit();
                }

                @Override
                public void close() {}
            };
        }
    }
}