package com.memsource.hack2019.analytics20.builder;

import com.google.gson.Gson;
import com.memsource.hack2019.analytics20.domain.Event;
import com.memsource.hack2019.analytics20.parser.EventJsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class TopologyBuilder {

    final static private Gson gson = new Gson();

    private TopologyBuilder() {
    }

    public static StreamsBuilder getBuilder(String inputTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(inputTopic);
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
                        (aggValue, oldValue) -> aggValue - oldValue
                )
                .toStream()
                //.print(Printed.toSysOut());
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        return builder;
    }
}
