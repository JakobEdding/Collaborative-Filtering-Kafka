package com.bakdata.demo.processors;

import com.bakdata.demo.apps.ALSApp;
import com.bakdata.demo.messages.FeatureMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.HashMap;

public class MFeatureCalculator extends AbstractProcessor<Integer, FeatureMessage> {
    private ProcessorContext context;
    private KeyValueStore<Integer, ArrayList<Integer>> mInBlocksUidStore;
    private HashMap<Integer, HashMap<Integer, ArrayList<Float>>> movieIdToUserFeatureVectors;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;
        this.mInBlocksUidStore = (KeyValueStore<Integer, ArrayList<Integer>>) this.context.getStateStore(ALSApp.M_INBLOCKS_UID_STORE);
        this.movieIdToUserFeatureVectors = new HashMap<>();
    }

    @Override
    public void process(final Integer partition, final FeatureMessage msg) {
        int userIdForFeatures = msg.id;
        ArrayList<Integer> movieIds = msg.dependentIds;
        ArrayList<Float> features = msg.features;

        for(int movieId: movieIds) {
            ArrayList<Integer> inBlockUidsForM = this.mInBlocksUidStore.get(movieId);
            if (inBlockUidsForM == null) {
                // wrong partition for movie
                continue;
            }

            HashMap<Integer, ArrayList<Float>> userIdToFeature = movieIdToUserFeatureVectors.get(movieId);
            if (userIdToFeature == null) {
                userIdToFeature = new HashMap<>();
            }
            userIdToFeature.put(userIdForFeatures, features);
            movieIdToUserFeatureVectors.put(movieId, userIdToFeature);

            if (userIdToFeature.size() == inBlockUidsForM.size()) {
                // everything necessary for movie feature calculation has been received
                float[][] uFeatures = new float[ALSApp.NUM_FEATURES][inBlockUidsForM.size()];
                for (int i = 0; i < inBlockUidsForM.size(); i++) {
                    for (int j = 0; j < ALSApp.NUM_FEATURES; i++) {
                        // Hendrik meinte pop
                        float[j][i] = userIdToFeature.get(i);
                    }
                }
//                FMatrixRMaj uFeaturesMatrix = new
            }
        }
    }

    @Override
    public void close() {}
}