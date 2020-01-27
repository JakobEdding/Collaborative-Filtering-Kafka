package de.hpi.collaborativefilteringkafka.serdes.IdRatingPairMessage;

import de.hpi.collaborativefilteringkafka.messages.IdRatingPairMessage;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortSerializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IdRatingPairMessageSerializer implements Serializer<IdRatingPairMessage> {

    private Serializer<Integer> idSerializer;
    private Serializer<Short> ratingSerializer;

    public IdRatingPairMessageSerializer() {
        this.idSerializer = new IntegerSerializer();
        this.ratingSerializer = new ShortSerializer();
    }

    @Override
    public byte[] serialize(String topic, IdRatingPairMessage data) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.write(idSerializer.serialize(topic, data.id));
            out.write(ratingSerializer.serialize(topic, data.rating));
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize IdRatingPairMessage", e);
        }
    }

    @Override
    public void close() {
        idSerializer.close();
        ratingSerializer.close();
    }
}
