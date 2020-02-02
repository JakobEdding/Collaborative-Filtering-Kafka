package de.hpi.collaborativefilteringkafka.serdes.FloatArray;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class FloatArrayDeserializer implements Deserializer<float[]> {

    public FloatArrayDeserializer() {}

    @Override
    public float[] deserialize(String topic, byte[] data) {
        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            int length = dis.readInt();
            float[] floatArray = new float[length];
            for(int i = 0; i < length; i++) {
                floatArray[i] = dis.readFloat();
            }
            return floatArray;
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize into a FloatArray", e);
        }
    }

    @Override
    public void close() {}

}