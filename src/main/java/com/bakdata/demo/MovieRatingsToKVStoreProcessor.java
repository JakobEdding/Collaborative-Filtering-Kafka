package com.bakdata.demo;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;

public final class MovieRatingsToKVStoreProcessor {

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
                    this.context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
                        try (final KeyValueIterator<Integer, ByteBuffer> iter = kvStore.all()) {
                            while (iter.hasNext()) {
                                final KeyValue<Integer, ByteBuffer> entry = iter.next();
                                context.forward(entry.key, entry.value);
                            }
                        }
                    });
                    this.kvStore = (KeyValueStore<Integer, ByteBuffer>) context.getStateStore("ratingsForMovies");
                }

                @Override
                public void process(final String dummy, final String ratingsForOneMovie) {
                    int movieId = Integer.parseInt(ratingsForOneMovie.split(":")[0]);

                    ArrayList<Integer> ratings = new ArrayList<>();
                    for (String userIdRatingPair : ratingsForOneMovie.split(":")[1].split(";")) {
                        ratings.add(Integer.parseInt(userIdRatingPair.split(",")[1]));
                    }

                    ByteBuffer bb = ByteBuffer.allocate(4 * ratings.size());
                    for (int rating : ratings) {
                        bb.putInt(rating);
                    }

                    this.kvStore.put(movieId, bb);
                    context.commit();
                }

                @Override
                public void close() {}
            };
        }
    }
}