package de.hpi.collaborativefilteringkafka;

import de.hpi.collaborativefilteringkafka.processors.MRatings2BlocksProcessor;
import de.hpi.collaborativefilteringkafka.processors.URatings2BlocksProcessor;
import de.hpi.collaborativefilteringkafka.serdes.List.ListSerde;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WholeTopologyTest {
    private TopologyTestDriver testDriver;
    private ConsumerRecordFactory<Integer, String> recordFactory = new ConsumerRecordFactory<>(new IntegerSerializer(), new StringSerializer());

    @Before
    public void setup() {
        // TODO: use getTopology()

        Topology builder = new Topology();

        StoreBuilder ratingsForMoviesStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("ratings-for-movies"),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Integer())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.

        StoreBuilder ratingsForUsersStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("ratings-for-users"),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Integer())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.

        builder.addSource("Source", "movieIds-with-ratings")
                .addProcessor("MRatings2Blocks", () -> new MRatings2BlocksProcessor.MyProcessorSupplier().get(), "Source")
                .addStateStore(ratingsForMoviesStoreSupplier, "MRatings2Blocks")
                .addProcessor("URatings2Blocks", () -> new URatings2BlocksProcessor.MyProcessorSupplier().get(), "MRatings2Blocks")
                .addStateStore(ratingsForUsersStoreSupplier, "URatings2Blocks");

        testDriver = new TopologyTestDriver(builder, MRatings2BlocksProcessor.getStreamsConfig());
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
//            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    public void testTopology() throws InterruptedException {
        // Input on movieIds-with-ratings topic
        // movieId -> userId,rating
        // 2 -> 2,2
        // 2 -> 3,1
        // 2 -> 1,2
        // 3 -> 1,5
        // 3 -> 2,4
        testDriver.pipeInput(recordFactory.create("movieIds-with-ratings", 2, "2,2;3,1;1,2"));
        testDriver.pipeInput(recordFactory.create("movieIds-with-ratings", 3, "1,5;2,4"));

        KeyValueStore<Integer, ArrayList<Integer>> store = testDriver.getKeyValueStore("ratings-for-users");

        // Output in ratingsForUsers store
        // userId -> ratings
        // 2 -> 2|4
        // 3 -> 1
        // 1 -> 2|5
        assertNotNull(store.get(2));
        assertNotNull(store.get(3));
        assertNotNull(store.get(1));

        assertEquals(2, store.get(2).size());
        assertEquals(1, store.get(3).size());
        assertEquals(2, store.get(1).size());

        int[] ratingsForUser2 = new int[] {2, 4};
        for (int i = 0; i < store.get(2).size(); i++) {
            assertEquals((int) store.get(2).get(i), ratingsForUser2[i]);
        }

        assertEquals((int) store.get(3).get(0), 1);

        int[] ratingsForUser1 = new int[] {2, 5};
        for (int i = 0; i < store.get(1).size(); i++) {
            assertEquals((int) store.get(1).get(i), ratingsForUser1[i]);
        }
    }
}
