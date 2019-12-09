package com.bakdata.demo;

import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class MovieRatingsToKVStoreProcessorTest {
    private StoreBuilder<KeyValueStore<Integer, ByteBuffer>> storeSupplier;
    private TopologyTestDriver testDriver;
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();
    private ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

    @Before
    public void setup() {
        Topology builder = new Topology();

        this.storeSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("ratingsForMovies"),
                Serdes.Integer(),
                Serdes.ByteBuffer()
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.

        builder.addSource("Source", "movieIds-with-ratings")
                .addProcessor("Process", () -> new MovieRatingsToKVStoreProcessor.MyProcessorSupplier().get(), "Source")
                .addStateStore(storeSupplier, "Process")
                .addSink("Sink", "userId-movieId-rating-triple", "Process");

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
    public void testTopology() {
        testDriver.pipeInput(recordFactory.create("movieIds-with-ratings", "3", "1,5;2,4"));

        ProducerRecord<String, String> outputRecord1 = testDriver.readOutput(
                "userId-movieId-rating-triple",
                stringDeserializer,
                stringDeserializer);
        OutputVerifier.compareKeyValue(outputRecord1, "1", "3,5");

        ProducerRecord<String, String> outputRecord2 = testDriver.readOutput(
                "userId-movieId-rating-triple",
                stringDeserializer,
                stringDeserializer);
        OutputVerifier.compareKeyValue(outputRecord2, "2", "3,4");

        assertNull(testDriver.readOutput("userId-movieId-rating-triple", stringDeserializer, longDeserializer));
    }

    @Test
    public void testProcessor() {
        final MockProcessorContext context = new MockProcessorContext();

        // Create, initialize, and register the state store.
        final KeyValueStore<Integer, ByteBuffer> store = this.storeSupplier.build();
        store.init(context, store);
        context.register(store, null);

        // Create and initialize the processor under test
        final Processor<String, String> processor = new MovieRatingsToKVStoreProcessor.MyProcessorSupplier().get();
        processor.init(context);

        // send a record to the processor
        processor.process("3", "1,5;2,4");

        assertTrue(context.committed());

        assertNotNull(store.get(3));
        ByteBuffer processedValue = store.get(3);

        int[] ratings = new int[] {5, 4};
        // += 4 because we're iterating over a byte[] and not an int[]
        for (int i = 0; i < processedValue.capacity(); i += 4) {
            assertEquals(processedValue.getInt(i), ratings[i/4]);
        }
    }
}