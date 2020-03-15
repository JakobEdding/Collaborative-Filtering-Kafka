package de.hpi.collaborativefilteringkafka.serdes.FloatArray;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FloatArraySerializer implements Serializer<float[]> {

    public FloatArraySerializer() {}

    @Override
    public byte[] serialize(String topic, float[] data) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(data.length);
            for(float elem : data) {
                out.writeFloat(elem);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize FloatArray", e);
        }
    }

    @Override
    public void close() {}
}
