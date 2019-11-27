package com.bakdata.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.bakdata.demo.WordCountDemo.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class WordCountDemoTest {

    private TopologyTestDriver testDriver;
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();
    private ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        WordCountDemo.createWordCountStream(builder);
        testDriver = new TopologyTestDriver(builder.build(), WordCountDemo.getStreamsConfig());
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    public void testOneWord() {
        testDriver.pipeInput(recordFactory.create(INPUT_TOPIC, null, "Hello"));

        ProducerRecord<String, Long> outputRecord = testDriver.readOutput(
                OUTPUT_TOPIC,
                stringDeserializer,
                longDeserializer);

        OutputVerifier.compareKeyValue(outputRecord, "hello", 1L);
        assertNull(testDriver.readOutput(OUTPUT_TOPIC, stringDeserializer, longDeserializer));
    }

    @Test
    public void testCountListOfWords() throws InterruptedException {
        final List<String> inputValues = Arrays.asList(
                "Apache Kafka Streams Example",
                "Using Kafka Streams Test Utils",
                "Reading and Writing Kafka Topic"
        );
        final Map<String, Long> expectedWordCounts = new LinkedHashMap<>();
        expectedWordCounts.put("apache", 1L);
        expectedWordCounts.put("kafka", 3L);
        expectedWordCounts.put("streams", 2L);
        expectedWordCounts.put("example", 1L);
        expectedWordCounts.put("using", 1L);
        expectedWordCounts.put("test", 1L);
        expectedWordCounts.put("utils", 1L);
        expectedWordCounts.put("reading", 1L);
        expectedWordCounts.put("and", 1L);
        expectedWordCounts.put("writing", 1L);
        expectedWordCounts.put("topic", 1L);

        List<ConsumerRecord<byte[], byte[]>> records = inputValues.stream()
                .map(value -> recordFactory.create(INPUT_TOPIC, null, value))
                .collect(Collectors.toList());
        testDriver.pipeInput(records);

        final KeyValueStore<String, Long> countsKeyValueStore = testDriver.getKeyValueStore(STORE_NAME);
        expectedWordCounts.forEach((key,value) -> {
            assertEquals(value, countsKeyValueStore.get(key));
        });
    }
}