package com.bakdata.demo.apps;

import com.bakdata.demo.processors.MFeatureCalculator;
import com.bakdata.demo.processors.MRatings2BlocksProcessor;
import com.bakdata.demo.processors.UFeatureInitializer;
import com.bakdata.demo.processors.URatings2BlocksProcessor;
import com.bakdata.demo.producers.PureModStreamPartitioner;
import com.bakdata.demo.serdes.FeatureMessage.FeatureMessageDeserializer;
import com.bakdata.demo.serdes.FeatureMessage.FeatureMessageSerializer;
import com.bakdata.demo.serdes.List.ListSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;
import java.util.Properties;

public class ALSApp extends BaseKafkaApp {
    public final static int NUM_PARTITIONS = 4;
    public final static int NUM_FEATURES = 5;
    // TODO: what actual init "small" values are used in Spark MLLib?
    public final static int MIN_RATING = 1;
    public final static int MAX_RATING = 5;

    public final static String MOVIEIDS_WITH_RATINGS_TOPIC = "movieIds-with-ratings";
    public final static String USERIDS_TO_MOVIEIDS_RATINGS_TOPIC = "userIds-to-movieIds-ratings";
    public final static String EOF_TOPIC = "eof";
    public final static String USER_FEATURES_TOPIC = "user-features";
    public final static String MOVIE_FEATURES_TOPIC = "movie-features";

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
                .addSink("userids-to-movieids-ratings-sink", USERIDS_TO_MOVIEIDS_RATINGS_TOPIC, new PureModStreamPartitioner<Integer, Object>(), "MRatings2Blocks")

                .addSource("userids-to-movieids-ratings-source", USERIDS_TO_MOVIEIDS_RATINGS_TOPIC)
                .addProcessor(
                        "URatings2Blocks",
                        () -> new URatings2BlocksProcessor.MyProcessorSupplier().get(),
                        "userids-to-movieids-ratings-source"
                )
                .addStateStore(uInBlocksMidStoreSupplier, "URatings2Blocks")
                .addStateStore(uInBlocksRatingsStoreSupplier, "URatings2Blocks")
                .addStateStore(uOutBlocksStoreSupplier, "URatings2Blocks")
                .addSink("eof-sink", EOF_TOPIC, new PureModStreamPartitioner<Integer, Object>(), "URatings2Blocks")

                .addSource("eof-source", EOF_TOPIC)
                .addProcessor(
                        "UFeatureInitializer",
                        () -> new UFeatureInitializer.MyProcessorSupplier().get(),
                        "eof-source"
                )
                .connectProcessorAndStateStores("UFeatureInitializer", M_INBLOCKS_UID_STORE, M_INBLOCKS_RATINGS_STORE, M_OUTBLOCKS_STORE, U_INBLOCKS_MID_STORE, U_INBLOCKS_RATINGS_STORE, U_OUTBLOCKS_STORE)
                .addSink(
                        "user-features-sink",
                        USER_FEATURES_TOPIC,
                        Serdes.Integer().serializer(),
                        new FeatureMessageSerializer(),
                        new PureModStreamPartitioner<Integer, Object>(),
                        "UFeatureInitializer"
                )
//                .addSink("user-features-sink", USER_FEATURES_TOPIC, new PureModStreamPartitioner<Integer, Object>(), "UFeatureInitializer", "UFeatureCalculator")

                .addSource(
                        "user-features-source",
                        Serdes.Integer().deserializer(),
                        new FeatureMessageDeserializer(),
                        USER_FEATURES_TOPIC
                )
                .addProcessor(
                        "MFeatureCalculator",
                        () -> new MFeatureCalculator.MyProcessorSupplier().get(),
                        "user-features-source"
                )
                .addSink(
                        "movie-features-sink",
                        MOVIE_FEATURES_TOPIC,
                        Serdes.Integer().serializer(),
                        new FeatureMessageSerializer(),
                        new PureModStreamPartitioner<Integer, Object>(),
                        "MFeatureCalculator"
                )
                
//                .addSource("movie-features-source", MOVIE_FEATURES_TOPIC)
//                .addProcessor(
//                        "UFeatureCalculator",
//                        () -> new UFeatureCalculator.MyProcessorSupplier().get(),
//                        "movie-features-source"
//                )
////                .addSink("user-features-sink", USER_FEATURES_TOPIC, new PureModStreamPartitioner<Integer, Object>(), "UFeatureCalculator")
                ;
    }

    @Override
    public String APPLICATION_ID_CONFIG() {
        return "collaborative-filtering-als";
    }

}
