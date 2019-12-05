package com.bakdata.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;

import static org.junit.Assert.*;

public class MovieRatingsToKVStoreProcessorTest {
    @Test
    public void test() {
        final MockProcessorContext context = new MockProcessorContext();

        // Create, initialize, and register the state store.
        final KeyValueStore<Integer, ByteBuffer> store =
                Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("ratingsForMovies"), Serdes.Integer(), Serdes.ByteBuffer())
                        .withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
                        .build();
        store.init(context, store);
        context.register(store, null);

        // Create and initialize the processor under test
        final Processor<String, String> processor = new MovieRatingsToKVStoreProcessor.MyProcessorSupplier().get();
        processor.init(context);

        // send a record to the processor
        // TODO: use real key to use multiple partitions
        processor.process("key", "3:1,5;2,4");

        // note that the processor commits, but does not forward, during process()
        assertTrue(context.committed());
        assertTrue(context.forwarded().isEmpty());

        // now, we trigger the punctuator, which iterates over the state store and forwards the contents.
        context.scheduledPunctuators().get(0).getPunctuator().punctuate(0L);

        // finally, we can verify the output.
        final Iterator<MockProcessorContext.CapturedForward> capturedForwards = context.forwarded().iterator();
        // TODO: question: these captured forwards come from the Processor > init > schedule ?!
        KeyValue<Integer, ByteBuffer> capturedKV = capturedForwards.next().keyValue();

        assertEquals(3, (long) capturedKV.key);

        int[] ratings = new int[] {5, 4};

        // += 4 because we're iterating over a byte[] and not an int[]
        for (int i = 0; i < capturedKV.value.capacity(); i += 4) {
            assertEquals(capturedKV.value.getInt(i), ratings[i/4]);
        }

        assertFalse(capturedForwards.hasNext());
    }
}