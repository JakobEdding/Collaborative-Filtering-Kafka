package de.hpi.collaborativefilteringkafka.serdes.IdRatingPairMessage;

import de.hpi.collaborativefilteringkafka.messages.IdRatingPairMessage;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class IdRatingPairMessageDeserializer implements Deserializer<IdRatingPairMessage> {

    private Deserializer<Integer> idDeserializer;
    private Deserializer<Short> ratingDeserializer;

    public IdRatingPairMessageDeserializer() {
        this.idDeserializer = new IntegerDeserializer();
        this.ratingDeserializer = new ShortDeserializer();
    }

    @Override
    public IdRatingPairMessage deserialize(String topic, byte[] data) {
        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            int id = dis.readInt();
            short rating = dis.readShort();
            return new IdRatingPairMessage(id, rating);
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize into a IdRatingPairMessage", e);
        }
    }

    @Override
    public void close() {
        this.idDeserializer.close();
        this.ratingDeserializer.close();
    }

}