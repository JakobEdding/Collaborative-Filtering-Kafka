package com.bakdata.demo;

import com.bakdata.demo.serdes.ListSerde;
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

import java.util.ArrayList;

import static org.junit.Assert.*;

public class UserRatingsToKVStoreProcessorTest {
    private StoreBuilder<KeyValueStore<Integer, ArrayList<Integer>>> storeSupplier;
    private TopologyTestDriver testDriver;
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();
    private ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

    @Before
    public void setup() {
        Topology builder = new Topology();

        this.storeSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("ratings-for-users"),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Integer())
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
        final KeyValueStore<Integer, ArrayList<Integer>> store = this.storeSupplier.build();
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
        ArrayList<Integer> processedValue = store.get(1);

        int[] ratings = new int[] {4, 5};
        for (int i = 0; i < processedValue.size(); i++) {
            assertEquals((int) processedValue.get(i), ratings[i]);
        }
    }
}