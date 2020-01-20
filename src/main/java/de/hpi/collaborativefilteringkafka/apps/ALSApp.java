package de.hpi.collaborativefilteringkafka.apps;

import de.hpi.collaborativefilteringkafka.processors.*;
import de.hpi.collaborativefilteringkafka.producers.PureModStreamPartitioner;
import de.hpi.collaborativefilteringkafka.serdes.FeatureMessage.FeatureMessageDeserializer;
import de.hpi.collaborativefilteringkafka.serdes.FeatureMessage.FeatureMessageSerializer;
import de.hpi.collaborativefilteringkafka.serdes.List.ListSerde;
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
    public final static float ALS_LAMBDA = 0.05f;
    public final static int NUM_ALS_ITERATIONS = 3;

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

//    @Override
//    public Topology getTopology(Properties properties) {
//        StoreBuilder mInBlocksUidStoreSupplier = Stores.keyValueStoreBuilder(
//                Stores.inMemoryKeyValueStore(M_INBLOCKS_UID_STORE),
//                Serdes.Integer(),
//                new ListSerde(ArrayList.class, Serdes.Integer())
//        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.
//        StoreBuilder mInBlocksRatingsStoreSupplier = Stores.keyValueStoreBuilder(
//                Stores.inMemoryKeyValueStore(M_INBLOCKS_RATINGS_STORE),
//                Serdes.Integer(),
//                new ListSerde(ArrayList.class, Serdes.Short())
//        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.
//        StoreBuilder mOutBlocksStoreSupplier = Stores.keyValueStoreBuilder(
//                Stores.inMemoryKeyValueStore(M_OUTBLOCKS_STORE),
//                Serdes.Integer(),
//                new ListSerde(ArrayList.class, Serdes.Short())
//        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.
//
//        StoreBuilder uInBlocksMidStoreSupplier = Stores.keyValueStoreBuilder(
//                Stores.inMemoryKeyValueStore(U_INBLOCKS_MID_STORE),
//                Serdes.Integer(),
//                new ListSerde(ArrayList.class, Serdes.Integer())
//        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.
//        StoreBuilder uInBlocksRatingsStoreSupplier = Stores.keyValueStoreBuilder(
//                Stores.inMemoryKeyValueStore(U_INBLOCKS_RATINGS_STORE),
//                Serdes.Integer(),
//                new ListSerde(ArrayList.class, Serdes.Short())
//        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.
//        StoreBuilder uOutBlocksStoreSupplier = Stores.keyValueStoreBuilder(
//                Stores.inMemoryKeyValueStore(U_OUTBLOCKS_STORE),
//                Serdes.Integer(),
//                new ListSerde(ArrayList.class, Serdes.Short())
//        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.
//
//        return new Topology()
//                .addSource("movieids-with-ratings-source", MOVIEIDS_WITH_RATINGS_TOPIC)
//                .addProcessor("MRatings2Blocks", MRatings2BlocksProcessor::new, "movieids-with-ratings-source")
//                .addStateStore(mInBlocksUidStoreSupplier, "MRatings2Blocks")
//                .addStateStore(mInBlocksRatingsStoreSupplier, "MRatings2Blocks")
//                .addStateStore(mOutBlocksStoreSupplier, "MRatings2Blocks")
//                // add sink/source combination here so that records are not kept inside same partition between processors
//                .addSink("userids-to-movieids-ratings-sink", USERIDS_TO_MOVIEIDS_RATINGS_TOPIC, new PureModStreamPartitioner<Integer, Object>(), "MRatings2Blocks")
//
//                .addSource("userids-to-movieids-ratings-source", USERIDS_TO_MOVIEIDS_RATINGS_TOPIC)
//                .addProcessor("URatings2Blocks", URatings2BlocksProcessor::new, "userids-to-movieids-ratings-source")
//                .addStateStore(uInBlocksMidStoreSupplier, "URatings2Blocks")
//                .addStateStore(uInBlocksRatingsStoreSupplier, "URatings2Blocks")
//                .addStateStore(uOutBlocksStoreSupplier, "URatings2Blocks")
//                .addSink("eof-sink", EOF_TOPIC, new PureModStreamPartitioner<Integer, Object>(), "URatings2Blocks")
//
//                .addSource("eof-source", EOF_TOPIC)
//                .addProcessor("UFeatureInitializer", UFeatureInitializer::new, "eof-source")
//                .connectProcessorAndStateStores("UFeatureInitializer", M_INBLOCKS_UID_STORE, M_INBLOCKS_RATINGS_STORE, M_OUTBLOCKS_STORE, U_INBLOCKS_MID_STORE, U_INBLOCKS_RATINGS_STORE, U_OUTBLOCKS_STORE)
//                .addSink(
//                        "user-features-sink",
//                        USER_FEATURES_TOPIC,
//                        Serdes.Integer().serializer(),
//                        new FeatureMessageSerializer(),
//                        new PureModStreamPartitioner<Integer, Object>(),
//                        "UFeatureInitializer"
//                )
//
//                .addSource(
//                        "user-features-source",
//                        Serdes.Integer().deserializer(),
//                        new FeatureMessageDeserializer(),
//                        USER_FEATURES_TOPIC
//                )
//                .addProcessor("MFeatureCalculator", MFeatureCalculator::new, "user-features-source")
//                .addSink(
//                        "movie-features-sink",
//                        MOVIE_FEATURES_TOPIC,
//                        Serdes.Integer().serializer(),
//                        new FeatureMessageSerializer(),
//                        new PureModStreamPartitioner<Integer, Object>(),
//                        "MFeatureCalculator"
//                )
//                .connectProcessorAndStateStores("MFeatureCalculator", M_INBLOCKS_UID_STORE, M_INBLOCKS_RATINGS_STORE, M_OUTBLOCKS_STORE)
//                .addSource(
//                        "movie-features-source",
//                        Serdes.Integer().deserializer(),
//                        new FeatureMessageDeserializer(),
//                        MOVIE_FEATURES_TOPIC)
//                .addProcessor("UFeatureCalculator", UFeatureCalculator::new, "movie-features-source")
//                .addSink(
//                        "user-features-sink-2",
//                        USER_FEATURES_TOPIC,
//                        Serdes.Integer().serializer(),
//                        new FeatureMessageSerializer(),
//                        new PureModStreamPartitioner<Integer, Object>(),
//                        "UFeatureCalculator"
//                )
//                .connectProcessorAndStateStores("UFeatureCalculator", U_INBLOCKS_MID_STORE, U_INBLOCKS_RATINGS_STORE, U_OUTBLOCKS_STORE)
//                ;
//    }

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

        Topology topology = new Topology()
                .addSource("movieids-with-ratings-source", MOVIEIDS_WITH_RATINGS_TOPIC)
                .addProcessor("MRatings2Blocks", MRatings2BlocksProcessor::new, "movieids-with-ratings-source")
                .addStateStore(mInBlocksUidStoreSupplier, "MRatings2Blocks")
                .addStateStore(mInBlocksRatingsStoreSupplier, "MRatings2Blocks")
                .addStateStore(mOutBlocksStoreSupplier, "MRatings2Blocks")
                // add sink/source combination here so that records are not kept inside same partition between processors
                .addSink("userids-to-movieids-ratings-sink", USERIDS_TO_MOVIEIDS_RATINGS_TOPIC, new PureModStreamPartitioner<Integer, Object>(), "MRatings2Blocks")

                .addSource("userids-to-movieids-ratings-source", USERIDS_TO_MOVIEIDS_RATINGS_TOPIC)
                .addProcessor("URatings2Blocks", URatings2BlocksProcessor::new, "userids-to-movieids-ratings-source")
                .addStateStore(uInBlocksMidStoreSupplier, "URatings2Blocks")
                .addStateStore(uInBlocksRatingsStoreSupplier, "URatings2Blocks")
                .addStateStore(uOutBlocksStoreSupplier, "URatings2Blocks")
                .addSink("eof-sink", EOF_TOPIC, new PureModStreamPartitioner<Integer, Object>(), "URatings2Blocks")

                .addSource("eof-source", EOF_TOPIC)
                .addProcessor("UFeatureInitializer", UFeatureInitializer::new, "eof-source")
                .connectProcessorAndStateStores("UFeatureInitializer", M_INBLOCKS_UID_STORE, M_INBLOCKS_RATINGS_STORE, M_OUTBLOCKS_STORE, U_INBLOCKS_MID_STORE, U_INBLOCKS_RATINGS_STORE, U_OUTBLOCKS_STORE)
                .addSink(
                        "user-features-sink-0",
                        USER_FEATURES_TOPIC + "-0",
                        Serdes.Integer().serializer(),
                        new FeatureMessageSerializer(),
                        new PureModStreamPartitioner<Integer, Object>(),
                        "UFeatureInitializer"
                )
                ;

        for (int i = 0; i < NUM_ALS_ITERATIONS; i++) {
            topology
            .addSource(
                    "user-features-source-" + i,
                    Serdes.Integer().deserializer(),
                    new FeatureMessageDeserializer(),
                    USER_FEATURES_TOPIC + "-" + i
            )
            .addProcessor("MFeatureCalculator-" + i, MFeatureCalculator::new, "user-features-source-" + i)
            .addSink(
                    "movie-features-sink-" + i,
                    MOVIE_FEATURES_TOPIC + "-" + i,
                    Serdes.Integer().serializer(),
                    new FeatureMessageSerializer(),
                    new PureModStreamPartitioner<Integer, Object>(),
                    "MFeatureCalculator-" + i
            )
            .connectProcessorAndStateStores("MFeatureCalculator-" + i, M_INBLOCKS_UID_STORE, M_INBLOCKS_RATINGS_STORE, M_OUTBLOCKS_STORE)
            .addSource(
                    "movie-features-source-" + i,
                    Serdes.Integer().deserializer(),
                    new FeatureMessageDeserializer(),
                    MOVIE_FEATURES_TOPIC + "-" + i
            )
            .addProcessor("UFeatureCalculator-" + i, UFeatureCalculator::new, "movie-features-source-" + i)
            .addSink(
                    "user-features-sink-" + (i + 1),
                    USER_FEATURES_TOPIC + "-" + (i + 1),
                    Serdes.Integer().serializer(),
                    new FeatureMessageSerializer(),
                    new PureModStreamPartitioner<Integer, Object>(),
                    "UFeatureCalculator-" + i
            )
            .connectProcessorAndStateStores("UFeatureCalculator-" + i, U_INBLOCKS_MID_STORE, U_INBLOCKS_RATINGS_STORE, U_OUTBLOCKS_STORE)
            ;
        }

        return topology;
    }

    @Override
    public String APPLICATION_ID_CONFIG() {
        return "collaborative-filtering-als";
    }

}
