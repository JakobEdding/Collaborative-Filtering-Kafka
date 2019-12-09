package com.bakdata.demo;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class UserRatingsToKVStoreProcessorTest {
    private StoreBuilder<KeyValueStore<Integer, ByteBuffer>> storeSupplier;
    private TopologyTestDriver testDriver;
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();
    private ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

    @Before
    public void setup() {
        Topology builder = new Topology();

        this.storeSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("ratingsForUsers"),
                Serdes.Integer(),
                Serdes.ByteBuffer()
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.

        builder.addSource("NewSource", "userId-movieId-rating-triple")
                .addProcessor("UserRatingsToKVStoreProcess", () -> new UserRatingsToKVStoreProcessor.MyProcessorSupplier().get(), "NewSource")
                .addStateStore(storeSupplier, "UserRatingsToKVStoreProcess");
//                .addSink("Sink", "", "Process");

        testDriver = new TopologyTestDriver(builder, MovieRatingsToKVStoreProcessor.getStreamsConfig());
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    public void testProcessor() {
        final MockProcessorContext context = new MockProcessorContext();

        // Create, initialize, and register the state store.
        final KeyValueStore<Integer, ByteBuffer> store = this.storeSupplier.build();
        store.init(context, store);
        context.register(store, null);

        // Create and initialize the processor under test
        final Processor<Integer, String> processor = new UserRatingsToKVStoreProcessor.MyProcessorSupplier().get();
        processor.init(context);

        // send a record to the processor
        processor.process(1, "3,4");
        processor.process(1, "2,5");

        assertTrue(context.committed());

        assertNotNull(store.get(1));
        ByteBuffer processedValue = store.get(1);

        int[] ratings = new int[] {4, 5};
        // += 4 because we're iterating over a byte[] and not an int[]
        for (int i = 0; i < processedValue.capacity(); i += 4) {
            assertEquals(processedValue.getInt(i), ratings[i/4]);
        }
    }
}