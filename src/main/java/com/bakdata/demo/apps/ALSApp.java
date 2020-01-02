package com.bakdata.demo.apps;

import com.bakdata.demo.MovieRatingsToKVStoreProcessor;
import com.bakdata.demo.UserRatingsToKVStoreProcessor;
import com.bakdata.demo.serdes.ListSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.ArrayList;
import java.util.Properties;

public class ALSApp extends BaseKafkaApp {
    public final static String RATINGS_FOR_MOVIES_STORE = "ratings-for-movies";
    public final static String RATINGS_FOR_USERS_STORE = "ratings-for-users";

    public final static String MOVIEIDS_WITH_RATINGS_TOPIC = "movieIds-with-ratings";

    public ALSApp() {}

    @Override
    public Topology getTopology(Properties properties) {
        StoreBuilder ratingsForMoviesStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(RATINGS_FOR_MOVIES_STORE),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Integer())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.

        StoreBuilder ratingsForUsersStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(RATINGS_FOR_USERS_STORE),
                Serdes.Integer(),
                new ListSerde(ArrayList.class, Serdes.Integer())
        ).withLoggingDisabled();  // Changelog is not supported by MockProcessorContext.

        return new Topology()
                .addSource("Source", MOVIEIDS_WITH_RATINGS_TOPIC)
                .addProcessor(
                        "MovieRatingsToKVStore",
                        () -> new MovieRatingsToKVStoreProcessor.MyProcessorSupplier().get(),
                        "Source"
                )
                .addStateStore(ratingsForMoviesStoreSupplier, "MovieRatingsToKVStore")
                .addProcessor(
                        "UserRatingsToKVStoreProcess",
                        () -> new UserRatingsToKVStoreProcessor.MyProcessorSupplier().get(),
                        "MovieRatingsToKVStore"
                )
                .addStateStore(ratingsForUsersStoreSupplier, "UserRatingsToKVStoreProcess");
    }

    @Override
    public String APPLICATION_ID_CONFIG() {
        return "collaborative-filtering-als";
    }

}
