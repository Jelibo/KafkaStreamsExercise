package com.memsource.hack2019.analytics20;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.google.gson.Gson;
import com.memsource.hack2019.analytics20.domain.Event;
import com.memsource.hack2019.analytics20.parser.EventJsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class App {

    public static void main(String[] args) {
        final String topicName = "homework";
        final String outputTopic = "homework-result";
        final Gson gson = new Gson();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, topicName + "_homework-v0.1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 500);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(topicName);
        source.print(Printed.toSysOut());

        source
                // map key to unitId && value to Event object
                .map((key, value) -> {
                    Event event = EventJsonParser.parseEvent(value, gson);
                    return KeyValue.pair(
                            event.getAfter() != null ?
                                    event.getAfter().gettUnits().get(0).gettUnitId() : null,
                            event);
                })
                // filter according to business rules
                .filter((key, event) -> event.shouldProcess())
                // map to confirmed segment values
                .map((key, event) -> KeyValue.pair(key, event.getAfter().isConfirmedSegment() ? 1 : 0))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                // aggregate confirmed segments, newValue wins
                .reduce((oldValue, newValue) -> newValue)
                // regroup by original taskId by sub-stringing
                .groupBy((key, value) -> KeyValue.pair(key.substring(0, key.lastIndexOf(":")), value),
                        Grouped.with(Serdes.String(), Serdes.Integer()))
                // reduce handling confirming and un-confirming segments
                .reduce(
                        Integer::sum,
                        App::subtract
                )
                .toStream()
                //.print(Printed.toSysOut());
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.cleanUp();
                streams.close();

                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Integer subtract(Integer aggValue, Integer oldValue) {
        return aggValue - oldValue;
    }
}
