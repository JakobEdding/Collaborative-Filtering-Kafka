package de.hpi.collaborativefilteringkafka.messages;

import java.util.ArrayList;

public class FeatureMessage {
    // TODO: getters using lombok
    public int id;
    public ArrayList<Integer> dependentIds;
    public ArrayList<Float> features;

    public FeatureMessage(int id, ArrayList<Integer> dependentIds, ArrayList<Float> features) {
        this.id = id;
        this.dependentIds = dependentIds;
        this.features = features;
    }

    public void setDependentIds(ArrayList<Integer> dependentIds) {
        this.dependentIds = dependentIds;
    }

    public void setDependentId(int dependentId) {
        this.dependentIds = new ArrayList<>();
        this.dependentIds.add(dependentId);
    }

    public String toString() {
        return String.format("Id: %d, Dependent-Ids: %s, Features: %s", this.id, this.dependentIds, this.features);
    }
}
