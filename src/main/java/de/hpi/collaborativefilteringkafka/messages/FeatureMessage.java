package de.hpi.collaborativefilteringkafka.messages;

import java.util.ArrayList;

public class FeatureMessage {
    // TODO: getters using lombok
    public int id;
    public ArrayList<Float> features;

    public FeatureMessage(int id, ArrayList<Float> features) {
        this.id = id;
        this.features = features;
    }

    public String toString() {
        return String.format("Id: %d, Features: %s", this.id, this.features);
    }
}
