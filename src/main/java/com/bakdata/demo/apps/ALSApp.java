package com.bakdata.demo.apps;

import com.bakdata.demo.MRatings2BlocksProcessor;
import com.bakdata.demo.URatings2BlocksProcessor;
import com.bakdata.demo.serdes.ListSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;
import java.util.Properties;

public class ALSApp extends BaseKafkaApp {
    public final static int NUM_PARTITIONS = 4;

    public final static String MOVIEIDS_WITH_RATINGS_TOPIC = "movieIds-with-ratings";
    public final static String USERIDS_TO_MOVIEIDS_RATINGS_TOPIC = "userIds-to-movieids-ratings";

    public final static String M_INBLOCKS_UID_STORE = "m-inblocks-uid";
    public final static String M_INBLOCKS_RATINGS_STORE = "m-inblocks-ratings";
    public final static String M_OUTBLOCKS_STORE = "m-outblocks";

    public final static String U_INBLOCKS_MID_STORE = "u-inblocks-mid";
    public final static String U_INBLOCKS_RATINGS_STORE = "u-inblocks-ratings";
    public final static String U_OUTBLOCKS_STORE = "u-outblocks";

    public ALSApp() {}

    @Override
    public Topology getTopology(Properties properties) {
        StoreBuilder mInBlocksUidStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(M_INBLOCKS_UID_STORE),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Integer())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.
        StoreBuilder mInBlocksRatingsStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(M_INBLOCKS_RATINGS_STORE),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Short())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.
        StoreBuilder mOutBlocksStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(M_OUTBLOCKS_STORE),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Short())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.

        StoreBuilder uInBlocksMidStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(U_INBLOCKS_MID_STORE),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Integer())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.
        StoreBuilder uInBlocksRatingsStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(U_INBLOCKS_RATINGS_STORE),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Short())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.
        StoreBuilder uOutBlocksStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(U_OUTBLOCKS_STORE),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Short())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.

        return new Topology()
                // TODO: implement new architecture as devised in architecture slides for last meeting
                .addSource("movieids-with-ratings-source", MOVIEIDS_WITH_RATINGS_TOPIC)
                .addProcessor(
                        "MRatings2Blocks",
                        () -> new MRatings2BlocksProcessor.MyProcessorSupplier().get(),
                        "movieids-with-ratings-source"
                )
                .addStateStore(mInBlocksUidStoreSupplier, "MRatings2Blocks")
                .addStateStore(mInBlocksRatingsStoreSupplier, "MRatings2Blocks")
                .addStateStore(mOutBlocksStoreSupplier, "MRatings2Blocks")
                // add sink/source combination here so that records are not kept inside same partition between processors
                .addSink("userids-to-movieids-ratings-sink", USERIDS_TO_MOVIEIDS_RATINGS_TOPIC)

                .addSource("userids-to-movieids-ratings-source", USERIDS_TO_MOVIEIDS_RATINGS_TOPIC)
                .addProcessor(
                        "URatings2Blocks",
                        () -> new URatings2BlocksProcessor.MyProcessorSupplier().get(),
                        "userids-to-movieids-ratings-source"
                )
                .addStateStore(uInBlocksMidStoreSupplier, "URatings2Blocks")
                .addStateStore(uInBlocksRatingsStoreSupplier, "URatings2Blocks")
                .addStateStore(uOutBlocksStoreSupplier, "URatings2Blocks");
    }

    @Override
    public String APPLICATION_ID_CONFIG() {
        return "collaborative-filtering-als";
    }

}
