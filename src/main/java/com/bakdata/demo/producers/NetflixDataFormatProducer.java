package com.bakdata.demo.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NetflixDataFormatProducer {
    private Producer<Integer, String> producer;
    private String dataFilePath;
    // TODO: get this from 1st processor?
    private String topicName;

    public NetflixDataFormatProducer(String dataFilePath) {
        this.dataFilePath = dataFilePath;
        this.topicName = "movieIds-with-ratings";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "netflix-data-producer-" + UUID.randomUUID().toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        this.producer = new KafkaProducer<>(props);
    }

    private void sendAndLog(ProducerRecord<Integer, String> record) {
        try {
            RecordMetadata metadata = this.producer.send(record).get();
            System.out.println("Record sent to partition " + metadata.partition()
                    + " with offset " + metadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
    }

    public void runProducer() throws IOException {
        BufferedReader dataFileReader = new BufferedReader(new FileReader(this.dataFilePath));
        String row;
        int currentMovieId = -1;
        ArrayList<String> userIdRatingPairs = new ArrayList<>();

        while ((row = dataFileReader.readLine()) != null) {
            if (row.endsWith(":")) {
                if (currentMovieId != -1) {
                    ProducerRecord<Integer, String> record = new ProducerRecord<>(
                            this.topicName, currentMovieId, String.join(";", userIdRatingPairs));
                    userIdRatingPairs = new ArrayList<>();
                    this.sendAndLog(record);
                }
                currentMovieId = Integer.parseInt(row.split(":")[0]);
            } else {
                userIdRatingPairs.add(row.substring(0, row.lastIndexOf(',')));
            }
        }

        ProducerRecord<Integer, String> record = new ProducerRecord<>(this.topicName, currentMovieId, String.join(";", userIdRatingPairs));
        this.sendAndLog(record);

        dataFileReader.close();

        // send EOF to signal that producer is done
        record = new ProducerRecord<>(this.topicName, -1, "EOF");
        this.sendAndLog(record);
    }
}
