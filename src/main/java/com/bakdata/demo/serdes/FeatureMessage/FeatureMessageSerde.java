package com.bakdata.demo.serdes.FeatureMessage;

import org.apache.kafka.common.serialization.Serdes;


public final class FeatureMessageSerde extends Serdes.WrapperSerde {

    public FeatureMessageSerde() {
        super(new FeatureMessageSerializer(), new FeatureMessageDeserializer());
    }
}