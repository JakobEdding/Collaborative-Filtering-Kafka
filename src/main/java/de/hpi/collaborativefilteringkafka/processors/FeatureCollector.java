package de.hpi.collaborativefilteringkafka.processors;

import de.hpi.collaborativefilteringkafka.apps.ALSApp;
import de.hpi.collaborativefilteringkafka.messages.FeatureMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.ejml.data.FMatrixRMaj;
import org.ejml.dense.row.CommonOps_FDRM;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.TreeMap;

public class FeatureCollector extends AbstractProcessor<Integer, FeatureMessage> {
    private ProcessorContext context;

    private TreeMap<Integer, float[]> mFeaturesMap;
    private TreeMap<Integer, float[]> uFeaturesMap;
    private int mostRecentMFeaturesMapSize;
    private int mostRecentUFeaturesMapSize;
    private boolean hasPredictionMatrixBeenComputed;

    private FMatrixRMaj mFeaturesMatrix;
    private FMatrixRMaj uFeaturesMatrix;

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context) {
        this.context = context;

        this.mFeaturesMap = new TreeMap<>();
        this.uFeaturesMap = new TreeMap<>();
        this.mostRecentMFeaturesMapSize = this.mFeaturesMap.size();
        this.mostRecentUFeaturesMapSize = this.uFeaturesMap.size();
        this.hasPredictionMatrixBeenComputed = false;

        // TODO: optimize wait time?
        this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            // if there are no final feature vectors yet, just skip
            if (this.mFeaturesMap.size() == ALSApp.NUM_MOVIES && this.uFeaturesMap.size() == ALSApp.NUM_USERS && !this.hasPredictionMatrixBeenComputed) {
                // check whether no new final feature vectors have been added in the mean time
                if (this.mFeaturesMap.size() == this.mostRecentMFeaturesMapSize
                    && this.uFeaturesMap.size() == this.mostRecentUFeaturesMapSize) {
                    System.out.println(String.format("Start Prediction Matrix Computation at %s", new Timestamp(System.currentTimeMillis())));
                    this.constructFeatureMatrices();
                    this.calculatePredictionMatrix();
                    this.hasPredictionMatrixBeenComputed = true;
                } else {
                    this.mostRecentMFeaturesMapSize = this.mFeaturesMap.size();
                    this.mostRecentUFeaturesMapSize = this.uFeaturesMap.size();
                }
            }
        });

        this.context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            System.out.println(String.format("Matrix is %d x %d", this.mFeaturesMap.size(), this.uFeaturesMap.size()));
        });
    }

    @Override
    public void process(final Integer partition, final FeatureMessage msg) {
        if (this.context.topic().equals("movie-features-" + ALSApp.NUM_ALS_ITERATIONS)) {
            this.mFeaturesMap.put(msg.id, msg.features);
        } else if (this.context.topic().equals("user-features-" + ALSApp.NUM_ALS_ITERATIONS)) {
            this.uFeaturesMap.put(msg.id, msg.features);
        }
    }

    private void constructFeatureMatrices() {
        float[][] mFeaturesMatrixArray = new float[this.mFeaturesMap.size()][ALSApp.NUM_FEATURES];
        int i = 0;
        for (float[] mFeatures : this.mFeaturesMap.values()) {
            System.arraycopy(mFeatures, 0, mFeaturesMatrixArray[i], 0, ALSApp.NUM_FEATURES);
            i++;
        }
        this.mFeaturesMatrix = new FMatrixRMaj(mFeaturesMatrixArray);

        float[][] uFeaturesMatrixArray = new float[this.uFeaturesMap.size()][ALSApp.NUM_FEATURES];
        i = 0;
        for (float[] uFeatures : this.uFeaturesMap.values()) {
            System.arraycopy(uFeatures, 0, uFeaturesMatrixArray[i], 0, ALSApp.NUM_FEATURES);
            i++;
        }
        this.uFeaturesMatrix = new FMatrixRMaj(uFeaturesMatrixArray);
    }

    private void calculatePredictionMatrix() {
        FMatrixRMaj predictionMatrix = new FMatrixRMaj(this.uFeaturesMap.size(), this.mFeaturesMap.size());
        CommonOps_FDRM.multTransB(this.uFeaturesMatrix, this.mFeaturesMatrix, predictionMatrix);

        System.out.println(String.format("Done at %s", new Timestamp(System.currentTimeMillis())));
//        System.out.println("result");
//        System.out.println(predictionMatrix);
    }

    @Override
    public void close() {}
}