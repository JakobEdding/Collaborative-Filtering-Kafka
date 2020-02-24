package de.hpi.collaborativefilteringkafka.producers;

import de.hpi.collaborativefilteringkafka.apps.ALSApp;
import de.hpi.collaborativefilteringkafka.messages.IdRatingPairMessage;
import de.hpi.collaborativefilteringkafka.serdes.IdRatingPairMessage.IdRatingPairMessageSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NetflixDataFormatProducer {
    private Producer<Integer, IdRatingPairMessage> producer;
    private String dataFilePath;
    // TODO: get this from 1st processor?
    private String topicName;

    public NetflixDataFormatProducer(String dataFilePath) {
        this.dataFilePath = dataFilePath;
        this.topicName = ALSApp.MOVIEIDS_WITH_RATINGS_TOPIC;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "netflix-data-producer-" + UUID.randomUUID().toString());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IdRatingPairMessageSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PureModPartitioner.class.getName());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.MAX_VALUE);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 300000);

        this.producer = new KafkaProducer<>(props);
    }

    private void sendAndLog(ProducerRecord<Integer, IdRatingPairMessage> record) {
        try {
            // ATTENTION: send() itself is asynchronous, but get() is synchronous
            // if you send() and get() instead of just calling send(), producer becomes ~50 times slower!
            RecordMetadata metadata = this.producer.send(record).get();
            System.out.println("Record sent to partition " + metadata.partition()
                    + " with offset " + metadata.offset());
        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
    }

    public void runProducer() throws IOException {
        System.out.println(String.format("Producer starts running at %s", new Timestamp(System.currentTimeMillis())));
        BufferedReader dataFileReader = new BufferedReader(new FileReader(this.dataFilePath));
        String row;
        int currentMovieId = -1;

        while ((row = dataFileReader.readLine()) != null) {
            if (row.endsWith(":")) {
                currentMovieId = Integer.parseInt(row.split(":")[0]);
            } else {
                String[] split = row.split(",");
                ProducerRecord<Integer, IdRatingPairMessage> record = new ProducerRecord<>(
                        this.topicName, currentMovieId, new IdRatingPairMessage(Integer.parseInt(split[0]), Short.parseShort(split[1])));
//                this.sendAndLog(record);
                this.producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception ex) {
                        if (ex != null) {
                            System.out.println(String.format("Failed to produce record. Got Exception: %s", ex));
                        }
                    }
                });
            }
        }

        dataFileReader.close();

        // send EOF to signal that producer is done
        for(int partition = 0; partition < ALSApp.NUM_PARTITIONS; partition++) {
            ProducerRecord<Integer, IdRatingPairMessage> record = new ProducerRecord<>(this.topicName, partition, new IdRatingPairMessage(-1, (short) -1));
//            this.sendAndLog(record);
            this.producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception ex) {
                    if (ex != null) {
                        System.out.println(String.format("Failed to produce record. Got Exception: %s", ex));
                    }
                }
            });
        }
    }
}
