package de.hpi.collaborativefilteringkafka.serdes.FloatArray;

import org.apache.kafka.common.serialization.Serdes;


public final class FloatArraySerde extends Serdes.WrapperSerde {

    public FloatArraySerde() {
        super(new FloatArraySerializer(), new FloatArrayDeserializer());
    }
}