package de.hpi.collaborativefilteringkafka.serdes.FeatureMessage;

import de.hpi.collaborativefilteringkafka.messages.FeatureMessage;
import de.hpi.collaborativefilteringkafka.serdes.List.ListSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

// TODO: This is taken from https://github.com/apache/kafka/pull/6592 and should be removed once the PR is merged
public class FeatureMessageSerializer implements Serializer<FeatureMessage> {

    private Serializer<Integer> idSerializer;
    private ListSerializer<Integer> dependentIdsSerializer;
    private ListSerializer<Float> featuresSerializer;

    public FeatureMessageSerializer() {
        this.idSerializer = new IntegerSerializer();
        this.dependentIdsSerializer = new ListSerializer<>(Serdes.Integer().serializer());
        this.featuresSerializer = new ListSerializer<>(Serdes.Float().serializer());
    }

    @Override
    public byte[] serialize(String topic, FeatureMessage data) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(data.id);
            out.write(dependentIdsSerializer.serialize(topic, data.dependentIds));
            out.write(featuresSerializer.serialize(topic, data.features));
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize FeatureMessage", e);
        }
    }

    @Override
    public void close() {
        idSerializer.close();
        dependentIdsSerializer.close();
        featuresSerializer.close();
    }
}
