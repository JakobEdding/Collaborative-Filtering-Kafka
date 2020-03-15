package de.hpi.collaborativefilteringkafka.serdes.FeatureMessage;

import de.hpi.collaborativefilteringkafka.messages.FeatureMessage;
import de.hpi.collaborativefilteringkafka.serdes.List.ListSerializer;
import de.hpi.collaborativefilteringkafka.serdes.FloatArray.FloatArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FeatureMessageSerializer implements Serializer<FeatureMessage> {

    private Serializer<Integer> idSerializer;
    private FloatArraySerializer featuresSerializer;

    public FeatureMessageSerializer() {
        this.idSerializer = new IntegerSerializer();
        this.featuresSerializer = new FloatArraySerializer();
    }

    @Override
    public byte[] serialize(String topic, FeatureMessage data) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(data.id);
            out.write(featuresSerializer.serialize(topic, data.features));
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize FeatureMessage", e);
        }
    }

    @Override
    public void close() {
        idSerializer.close();
        featuresSerializer.close();
    }
}
