package de.hpi.collaborativefilteringkafka.apps;

import de.hpi.collaborativefilteringkafka.producers.NetflixDataFormatProducer;

import java.io.File;
import java.sql.Timestamp;


class ALSAppRunner {
    public static void main(String[] args) {
        if (args.length < 7) {
            System.out.println("\u001B[31mARGUMENTS MISSING\u001B[0m");
            return;
        }

        ALSApp alsApp = new ALSApp(
                Integer.parseInt(args[0]),
                Integer.parseInt(args[1]),
                Float.parseFloat(args[2]),
                Integer.parseInt(args[3]),
                Integer.parseInt(args[5]),
                Integer.parseInt(args[6])
        );

        System.out.println(String.format("Start at %s", new Timestamp(System.currentTimeMillis())));

        String pathToTestDataFile = new File(args[4]).getAbsolutePath();
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
