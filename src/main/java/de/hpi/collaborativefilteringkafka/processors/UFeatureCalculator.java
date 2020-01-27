package de.hpi.collaborativefilteringkafka.processors;

import de.hpi.collaborativefilteringkafka.apps.ALSApp;
import de.hpi.collaborativefilteringkafka.messages.FeatureMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.ejml.data.FMatrixRMaj;
import org.ejml.dense.row.CommonOps_FDRM;

import java.util.ArrayList;
import java.util.HashMap;

public class UFeatureCalculator extends AbstractProcessor<Integer, FeatureMessage> {
    private ProcessorContext context;
    private KeyValueStore<Integer, ArrayList<Integer>> uInBlocksMidStore;
    private KeyValueStore<Integer, ArrayList<Short>> uInBlocksRatingsStore;
    private KeyValueStore<Integer, ArrayList<Short>> uOutBlocksStore;
    private HashMap<Integer, HashMap<Integer, ArrayList<Float>>> userIdToMovieFeatureVectors;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;

        this.uInBlocksMidStore = (KeyValueStore<Integer, ArrayList<Integer>>) this.context.getStateStore(ALSApp.U_INBLOCKS_MID_STORE);
        this.uInBlocksRatingsStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.U_INBLOCKS_RATINGS_STORE);
        this.uOutBlocksStore = (KeyValueStore<Integer, ArrayList<Short>>) this.context.getStateStore(ALSApp.U_OUTBLOCKS_STORE);

        this.userIdToMovieFeatureVectors = new HashMap<>();
    }

    @Override
    public void process(final Integer partition, final FeatureMessage msg) {
//        System.out.println(String.format("Received: UFeatureCalculator - partition %d - message: %s", partition, msg.toString()));

        int movieIdForFeatures = msg.id;
        ArrayList<Integer> userIds = msg.dependentIds;
        ArrayList<Float> features = msg.features;

        for (int userId : userIds) {
            ArrayList<Integer> inBlockMidsForU = this.uInBlocksMidStore.get(userId);
            if (inBlockMidsForU == null) {
                // wrong partition for user
//                System.out.println(String.format("Received: UFeatureCalculator - partition %d - this user is not on this partition: %d", partition, userId));
                continue;
            }

            HashMap<Integer, ArrayList<Float>> movieIdToFeature = userIdToMovieFeatureVectors.get(userId);
            if (movieIdToFeature == null) {
                movieIdToFeature = new HashMap<>();
            }
            movieIdToFeature.put(movieIdForFeatures, features);
            userIdToMovieFeatureVectors.put(userId, movieIdToFeature);

            if (movieIdToFeature.size() == inBlockMidsForU.size()) {  // everything necessary for user feature calculation has been received
                float[][] mFeatures = new float[inBlockMidsForU.size()][ALSApp.NUM_FEATURES];
                int i = 0;
                for (Integer movieId : inBlockMidsForU) {
                    ArrayList<Float> featuresForCurrentMovieId = movieIdToFeature.get(movieId);
                    for (int j = 0; j < ALSApp.NUM_FEATURES; j++) {
                        mFeatures[i][j] = featuresForCurrentMovieId.get(j);
                    }
                    i++;
                }
                // movie features matrix ordered by movieid (rows) with ALSApp.NUM_FEATURES features (cols)
                FMatrixRMaj mFeaturesMatrix = new FMatrixRMaj(mFeatures);

                ArrayList<Short> userIdRatingsList = this.uInBlocksRatingsStore.get(userId);

                float[][] userIdRatingsArray = new float[userIdRatingsList.size()][1];
                for (int k = 0; k < userIdRatingsArray.length; k++) {
                    userIdRatingsArray[k][0] = (float) userIdRatingsList.get(k);
                }

                FMatrixRMaj userIdRatingsVector = new FMatrixRMaj(userIdRatingsArray);

                FMatrixRMaj V = new FMatrixRMaj(ALSApp.NUM_FEATURES, 1);
                CommonOps_FDRM.multTransA(mFeaturesMatrix, userIdRatingsVector, V);

                FMatrixRMaj A = new FMatrixRMaj(ALSApp.NUM_FEATURES, ALSApp.NUM_FEATURES);
                CommonOps_FDRM.multTransA(mFeaturesMatrix, mFeaturesMatrix, A);

                FMatrixRMaj normalization = new FMatrixRMaj(ALSApp.NUM_FEATURES, ALSApp.NUM_FEATURES);
                CommonOps_FDRM.scale((float) userIdRatingsArray.length, CommonOps_FDRM.identity(ALSApp.NUM_FEATURES), normalization);

                FMatrixRMaj newA = new FMatrixRMaj(ALSApp.NUM_FEATURES, ALSApp.NUM_FEATURES);
                CommonOps_FDRM.add(A, ALSApp.ALS_LAMBDA, normalization, newA);

                FMatrixRMaj uFeaturesVector = new FMatrixRMaj(ALSApp.NUM_FEATURES, 1);
                CommonOps_FDRM.invert(newA);
                CommonOps_FDRM.mult(newA, V, uFeaturesVector);

                ArrayList<Float> uFeaturesVectorFloat = new ArrayList<>(ALSApp.NUM_FEATURES);
                for (int l = 0; l < ALSApp.NUM_FEATURES; l++) {
                    uFeaturesVectorFloat.add(uFeaturesVector.get(l, 0));
                }

                String sourceTopic = this.context.topic();
                int sourceTopicIteration = Integer.parseInt(sourceTopic.substring(sourceTopic.length() - 1));
                int sinkTopicIteration = sourceTopicIteration + 1;

                FeatureMessage featureMsgToBeSent = new FeatureMessage(
                        userId,
                        this.uInBlocksMidStore.get(userId),
                        uFeaturesVectorFloat
                );

                if (sourceTopicIteration == ALSApp.NUM_ALS_ITERATIONS - 1) {
//                    System.out.println(String.format("finishing: UFeatureCalculator - sending message: %s", featureMsgToBeSent.toString()));
                    context.forward(
                            0,
                            featureMsgToBeSent,
                            To.child("user-features-sink-" + sinkTopicIteration)
                    );
                } else {
//                    System.out.println(String.format("not finishing: UFeatureCalculator - sending message: %s", featureMsgToBeSent.toString()));
                    for (int targetPartition : this.uOutBlocksStore.get(userId)) {
                        // TODO: don't hardcode sink name
                        context.forward(
                                targetPartition,
                                featureMsgToBeSent,
                                To.child("user-features-sink-" + sinkTopicIteration)
                        );
                    }
                }
            }
        }
    }

    @Override
    public void close() {}
}