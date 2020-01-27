package de.hpi.collaborativefilteringkafka.serdes.IdRatingPairMessage;

import org.apache.kafka.common.serialization.Serdes;


public final class IdRatingPairMessageSerde extends Serdes.WrapperSerde {

    public IdRatingPairMessageSerde() {
        super(new IdRatingPairMessageSerializer(), new IdRatingPairMessageDeserializer());
    }
}