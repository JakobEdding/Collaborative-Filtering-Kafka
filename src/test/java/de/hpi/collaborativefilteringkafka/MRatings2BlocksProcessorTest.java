package de.hpi.collaborativefilteringkafka;

import de.hpi.collaborativefilteringkafka.processors.MRatings2BlocksProcessor;
import de.hpi.collaborativefilteringkafka.serdes.List.ListSerde;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
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

import java.util.ArrayList;

import static org.junit.Assert.*;

public class MRatings2BlocksProcessorTest {
    private StoreBuilder<KeyValueStore<Integer, ArrayList<Integer>>> storeSupplier;
    private TopologyTestDriver testDriver;
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private IntegerDeserializer intDeserializer = new IntegerDeserializer();
    private ConsumerRecordFactory<Integer, String> recordFactory = new ConsumerRecordFactory<>(new IntegerSerializer(), new StringSerializer());

    @Before
    public void setup() {
        Topology builder = new Topology();

        this.storeSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("ratings-for-movies"),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Integer())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.

        builder.addSource("Source", "movieIds-with-ratings")
                .addProcessor("Process", () -> new MRatings2BlocksProcessor.MyProcessorSupplier().get(), "Source")
                .addStateStore(storeSupplier, "Process")
                .addSink("Sink", "userId-movieId-rating-triple", "Process");

        testDriver = new TopologyTestDriver(builder, MRatings2BlocksProcessor.getStreamsConfig());
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
        testDriver.pipeInput(recordFactory.create("movieIds-with-ratings", 2, "2,2;3,1"));
        testDriver.pipeInput(recordFactory.create("movieIds-with-ratings", 3, "1,5;2,4"));

        ProducerRecord<Integer, String> outputRecord1 = testDriver.readOutput(
                "userId-movieId-rating-triple",
                intDeserializer,
                stringDeserializer);
        OutputVerifier.compareKeyValue(outputRecord1, 2, "2,2");

        ProducerRecord<Integer, String> outputRecord2 = testDriver.readOutput(
                "userId-movieId-rating-triple",
                intDeserializer,
                stringDeserializer);
        OutputVerifier.compareKeyValue(outputRecord2, 3, "2,1");

        ProducerRecord<Integer, String> outputRecord3 = testDriver.readOutput(
                "userId-movieId-rating-triple",
                intDeserializer,
                stringDeserializer);
        OutputVerifier.compareKeyValue(outputRecord3, 1, "3,5");

        ProducerRecord<Integer, String> outputRecord4 = testDriver.readOutput(
                "userId-movieId-rating-triple",
                intDeserializer,
                stringDeserializer);
        OutputVerifier.compareKeyValue(outputRecord4, 2, "3,4");

        assertNull(testDriver.readOutput("userId-movieId-rating-triple", intDeserializer, stringDeserializer));
    }

    @Test
    public void testProcessor() {
        final MockProcessorContext context = new MockProcessorContext();

        // Create, initialize, and register the state store.
        final KeyValueStore<Integer, ArrayList<Integer>> store = this.storeSupplier.build();
        store.init(context, store);
        context.register(store, null);

        // Create and initialize the processor under test
        final Processor<Integer, String> processor = new MRatings2BlocksProcessor.MyProcessorSupplier().get();
        processor.init(context);

        // send a record to the processor
        processor.process(3, "1,5;2,4");

        assertTrue(context.committed());

        assertNotNull(store.get(3));
        ArrayList<Integer> processedValue = store.get(3);

        int[] ratings = new int[] {5, 4};
        for (int i = 0; i < processedValue.size(); i++) {
            assertEquals((int) processedValue.get(i), ratings[i]);
        }
    }
}