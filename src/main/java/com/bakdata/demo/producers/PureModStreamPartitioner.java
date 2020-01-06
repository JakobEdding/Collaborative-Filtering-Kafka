package com.bakdata.demo.producers;

import org.apache.kafka.streams.processor.StreamPartitioner;

public class PureModStreamPartitioner<K, V> implements StreamPartitioner<K, V> {

    public PureModStreamPartitioner() {}

    @Override
    public Integer partition(final String topic, final K key, final V value, final int numPartitions) {
        return ((Integer) key) % numPartitions;
    }
}