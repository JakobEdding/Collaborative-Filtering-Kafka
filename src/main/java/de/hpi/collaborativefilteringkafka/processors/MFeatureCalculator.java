package de.hpi.collaborativefilteringkafka.processors;

import de.hpi.collaborativefilteringkafka.apps.ALSApp;
import de.hpi.collaborativefilteringkafka.messages.FeatureMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.ejml.data.FMatrixRMaj;
import org.ejml.dense.row.CommonOps_FDRM;

import java.util.ArrayList;
import java.util.HashMap;

public class MFeatureCalculator extends AbstractProcessor<Integer, FeatureMessage> {
    private ProcessorContext context;
    private KeyValueStore<Integer, ArrayList<Integer>> mInBlocksUidStore;
    private KeyValueStore<Integer, ArrayList<Short>> mInBlocksRatingsStore;
    private KeyValueStore<Integer, ArrayList<Short>> mOutBlocksStore;
    private HashMap<Integer, HashMap<Integer, ArrayList<Float>>> movieIdToUserFeatureVectors;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;

        this.mInBlocksUidStore = (KeyValueStore<Integer, ArrayList<Integer>>) this.context.getStateStore(ALSApp.M_INBLOCKS_UID_STORE);
        this.mInBlocksRatingsStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.M_INBLOCKS_RATINGS_STORE);
        this.mOutBlocksStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.M_OUTBLOCKS_STORE);

        this.movieIdToUserFeatureVectors = new HashMap<>();
    }

    @Override
    public void process(final Integer partition, final FeatureMessage msg) {
        System.out.println(String.format("Received: MFeatureCalculator - partition %d - message: %s", partition, msg.toString()));

        int userIdForFeatures = msg.id;
        ArrayList<Integer> movieIds = msg.dependentIds;
        ArrayList<Float> features = msg.features;

        for (int movieId : movieIds) {
            ArrayList<Integer> inBlockUidsForM = this.mInBlocksUidStore.get(movieId);
            if (inBlockUidsForM == null) {
                // wrong partition for movie
                System.out.println(String.format("Received: MFeatureCalculator - partition %d - this movie is not on this partition: %d", partition, movieId));
                continue;
            }

            HashMap<Integer, ArrayList<Float>> userIdToFeature = movieIdToUserFeatureVectors.get(movieId);
            if (userIdToFeature == null) {
                userIdToFeature = new HashMap<>();
            }
            userIdToFeature.put(userIdForFeatures, features);
            movieIdToUserFeatureVectors.put(movieId, userIdToFeature);

            if (userIdToFeature.size() == inBlockUidsForM.size()) {  // everything necessary for movie feature calculation has been received
                float[][] uFeatures = new float[inBlockUidsForM.size()][ALSApp.NUM_FEATURES];
                int i = 0;
                for (Integer userId : inBlockUidsForM) {
                    ArrayList<Float> featuresForCurrentUserId = userIdToFeature.get(userId);
                    for (int j = 0; j < ALSApp.NUM_FEATURES; j++) {
                        uFeatures[i][j] = featuresForCurrentUserId.get(j);
                    }
                    i++;
                }
                // user features matrix ordered by userid (rows) with ALSApp.NUM_FEATURES features (cols)
                FMatrixRMaj uFeaturesMatrix = new FMatrixRMaj(uFeatures);

                ArrayList<Short> movieIdRatingsList = this.mInBlocksRatingsStore.get(movieId);

                float[][] movieIdRatingsArray = new float[movieIdRatingsList.size()][1];
                for (int k = 0; k < movieIdRatingsArray.length; k++) {
                    movieIdRatingsArray[k][0] = (float) movieIdRatingsList.get(k);
                }

                FMatrixRMaj movieIdRatingsVector = new FMatrixRMaj(movieIdRatingsArray);

                FMatrixRMaj V = new FMatrixRMaj(ALSApp.NUM_FEATURES, 1);
                CommonOps_FDRM.multTransA(uFeaturesMatrix, movieIdRatingsVector, V);

                FMatrixRMaj A = new FMatrixRMaj(ALSApp.NUM_FEATURES, ALSApp.NUM_FEATURES);
                CommonOps_FDRM.multTransA(uFeaturesMatrix, uFeaturesMatrix, A);

                FMatrixRMaj normalization = new FMatrixRMaj(ALSApp.NUM_FEATURES, ALSApp.NUM_FEATURES);
                CommonOps_FDRM.scale((float) movieIdRatingsArray.length, CommonOps_FDRM.identity(ALSApp.NUM_FEATURES), normalization);

                FMatrixRMaj newA = new FMatrixRMaj(ALSApp.NUM_FEATURES, ALSApp.NUM_FEATURES);
                CommonOps_FDRM.add(A, ALSApp.ALS_LAMBDA, normalization, newA);

                FMatrixRMaj mFeaturesVector = new FMatrixRMaj(ALSApp.NUM_FEATURES, 1);
                CommonOps_FDRM.invert(newA);
                CommonOps_FDRM.mult(newA, V, mFeaturesVector);

                ArrayList<Float> mFeaturesVectorFloat = new ArrayList<>(ALSApp.NUM_FEATURES);
                for (int l = 0; l < ALSApp.NUM_FEATURES; l++) {
                    mFeaturesVectorFloat.add(mFeaturesVector.get(l, 0));
                }

                for (int targetPartition : this.mOutBlocksStore.get(movieId)) {
                    context.forward(targetPartition, new FeatureMessage(movieId, this.mInBlocksUidStore.get(movieId), mFeaturesVectorFloat));
                }
            }
        }
    }

    @Override
    public void close() {}
}