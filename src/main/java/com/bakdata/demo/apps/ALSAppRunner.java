package com.bakdata.demo.apps;

import com.bakdata.demo.producers.NetflixDataFormatProducer;

import java.io.File;


class ALSAppRunner {
    public static void main(String[] args) {
        String pathToTestDataFile = new File("./testdata/movie_ratings_very_small").getAbsolutePath();
        NetflixDataFormatProducer producer = new NetflixDataFormatProducer(pathToTestDataFile);

        try {
            producer.runProducer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
