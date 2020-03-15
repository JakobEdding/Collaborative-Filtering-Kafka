package de.hpi.collaborativefilteringkafka.messages;

import java.util.ArrayList;
import java.util.Arrays;

public class FeatureMessage {
    public int id;
    public float[] features;

    public FeatureMessage(int id, float[] features) {
        this.id = id;
        this.features = features;
    }

    public String toString() {
        return String.format("Id: %d, Features: %s", this.id, Arrays.toString(this.features));
    }
}
