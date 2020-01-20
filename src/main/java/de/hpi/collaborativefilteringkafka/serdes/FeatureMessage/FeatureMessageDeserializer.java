package de.hpi.collaborativefilteringkafka.serdes.FeatureMessage;

import de.hpi.collaborativefilteringkafka.apps.ALSApp;
import de.hpi.collaborativefilteringkafka.messages.FeatureMessage;
import de.hpi.collaborativefilteringkafka.serdes.List.ListDeserializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serdes;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// TODO: This is taken from https://github.com/apache/kafka/pull/6592 and should be removed once the PR is merged
public class FeatureMessageDeserializer implements Deserializer<FeatureMessage> {

    private Deserializer<Integer> idDeserializer;
    private Deserializer<List<Integer>> dependentIdsDeserializer;
    private Deserializer<List<Float>> featuresDeserializer;

    public FeatureMessageDeserializer() {
        this.idDeserializer = new IntegerDeserializer();
        this.dependentIdsDeserializer = new ListDeserializer<>(ArrayList.class, Serdes.Integer().deserializer());
        this.featuresDeserializer = new ListDeserializer<>(ArrayList.class, Serdes.Float().deserializer());
    }

    @Override
    public FeatureMessage deserialize(String topic, byte[] data) {
        try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            int lengthOfFeaturesList = 4 * (1 + ALSApp.NUM_FEATURES);
            // 4 is length of id int
            int lengthOfDependentIdsList = data.length - 4 - lengthOfFeaturesList;

            int id = dis.readInt();

            byte[] dependentIdsPayload = new byte[lengthOfDependentIdsList];
            if (dis.read(dependentIdsPayload) == -1) {
                throw new SerializationException("End of the stream was reached prematurely");
            }
            ArrayList<Integer> dependentIds = (ArrayList) this.dependentIdsDeserializer.deserialize(topic, dependentIdsPayload);

            byte[] featuresPayload = new byte[lengthOfFeaturesList];
            if (dis.read(featuresPayload) == -1) {
                throw new SerializationException("End of the stream was reached prematurely");
            }
            ArrayList<Float> features = (ArrayList) this.featuresDeserializer.deserialize(topic, featuresPayload);

            return new FeatureMessage(id, dependentIds, features);
        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize into a FeatureMessage", e);
        }
    }

    @Override
    public void close() {
        this.idDeserializer.close();
        this.dependentIdsDeserializer.close();
        this.featuresDeserializer.close();
    }

}