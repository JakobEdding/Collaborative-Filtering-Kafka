package de.hpi.collaborativefilteringkafka.apps;

import de.hpi.collaborativefilteringkafka.producers.NetflixDataFormatProducer;

import java.io.File;
import java.sql.Timestamp;


class ALSAppRunner {
    public static void main(String[] args) {
        ALSApp alsApp = new ALSApp();

//        String pathToTestDataFile = new File("./testdata/movie_ratings_very_small").getAbsolutePath();
        String pathToTestDataFile = new File("/Users/j/Documents/Uni/MLDS/.datasets.nosync/netflix-prize-data-small/combined_data_1.txt").getAbsolutePath();
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
