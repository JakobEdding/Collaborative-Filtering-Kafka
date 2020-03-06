package de.hpi.collaborativefilteringkafka.messages;

import java.util.ArrayList;
import java.util.Arrays;

public class FeatureMessage {
    public int id;
    public ArrayList<Integer> dependentIds;
    public float[] features;

    public FeatureMessage(int id, ArrayList<Integer> dependentIds, float[] features) {
        this.id = id;
        this.dependentIds = dependentIds;
        this.features = features;
    }

    public void setDependentIds(ArrayList<Integer> dependentIds) {
        this.dependentIds = dependentIds;
    }

    public String toString() {
        return String.format("Id: %d, Dependent-Ids: %s, Features: %s", this.id, this.dependentIds, Arrays.toString(this.features));
    }
}
