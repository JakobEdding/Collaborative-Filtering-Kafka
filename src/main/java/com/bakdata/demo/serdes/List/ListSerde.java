package com.bakdata.demo.serdes.List;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.List;


// TODO: This is taken from https://github.com/apache/kafka/pull/6592 and should be removed once the PR is merged
public final class ListSerde<Inner> extends Serdes.WrapperSerde<List<Inner>> {

    public ListSerde() {
        super(new ListSerializer<>(), new ListDeserializer<>());
    }

    public <L extends List> ListSerde(Class<L> listClass, Serde<Inner> serde) {
        super(new ListSerializer<>(serde.serializer()), new ListDeserializer<>(listClass, serde.deserializer()));
    }
}