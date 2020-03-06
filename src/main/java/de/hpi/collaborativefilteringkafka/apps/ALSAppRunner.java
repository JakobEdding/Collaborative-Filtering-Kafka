package de.hpi.collaborativefilteringkafka.apps;

import de.hpi.collaborativefilteringkafka.producers.NetflixDataFormatProducer;

import java.io.File;
import java.sql.Timestamp;


class ALSAppRunner {
    public static void main(String[] args) {
        ALSApp alsApp = new ALSApp();

        System.out.println(String.format("Start at %s", new Timestamp(System.currentTimeMillis())));

//        String pathToTestDataFile = new File("./data/data_sample_tiny.txt").getAbsolutePath();
//        String pathToTestDataFile = new File("./data/data_sample_small.txt").getAbsolutePath();
        String pathToTestDataFile = new File("./data/data_sample_medium.txt").getAbsolutePath();
        NetflixDataFormatProducer producer = new NetflixDataFormatProducer(pathToTestDataFile);

        try {
            producer.runProducer();
            System.out.println(String.format("Producer is done at %s", new Timestamp(System.currentTimeMillis())));
            alsApp.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
