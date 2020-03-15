package de.hpi.collaborativefilteringkafka.apps;

import de.hpi.collaborativefilteringkafka.serdes.IdRatingPairMessage.IdRatingPairMessageSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.Callable;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

public abstract class BaseKafkaApp implements Callable<Void> {

    private String host = "localhost";
    private int port = 8070;
    public final static String brokers = "localhost:29092,localhost:29093";

    @Override
    public Void call() throws Exception {
        final Properties properties = this.getProperties();
        final Topology topology = getTopology(properties);
        final KafkaStreams streams = new KafkaStreams(topology, properties);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (Exception e) {
                log.warn("Error in shutdown", e);
            }
        }));

        streams.cleanUp();
        streams.start();

        return null;
    }

    private Properties getProperties() {
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.APPLICATION_ID_CONFIG());
//        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, String.format("%s:%s", this.host, this.port));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, IdRatingPairMessageSerde.class.getName());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 300000);

        // Enable tracking message flow in Confluent Control Center
//        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
//                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
//        props.put(
//                StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
//                "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        return props;
    }

    public abstract Topology getTopology(Properties properties);

    public abstract String APPLICATION_ID_CONFIG();
}
