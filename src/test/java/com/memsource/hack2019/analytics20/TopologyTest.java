package com.memsource.hack2019.analytics20;

import static org.hamcrest.CoreMatchers.equalTo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.memsource.hack2019.analytics20.builder.TopologyBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Assert;
import org.junit.Test;

public class TopologyTest {

    private static final String inputTopic = "inputTopic";
    private static final String outputTopic = "outputTopic";

    @Test
    public void testResult() throws IOException {

        final String inputFileName = "kafka-messages.jsonline";
        final List<String> lines = Files.readAllLines(Path.of("src/test/resources/" + inputFileName));

        final List<KeyValue<String, String>> inputValues = lines.stream()
                .map(l -> KeyValue.pair((String) null, l))
                .collect(Collectors.toList());

        // We want to compute the sum of the length of words (values) by the first letter (new key) of words.
        final Map<String, Integer> expectedOutput = new HashMap<>();
        expectedOutput.put("TyazVPlL11HYaTGs1_dc1", 2);
        expectedOutput.put("jNazVPlL11HFhTGs1_dc1", 0);

        //
        // Step 1: Create the topology and its configuration.
        //
        final StreamsBuilder builder = TopologyBuilder.getBuilder(inputTopic, outputTopic);
        final Properties streamsConfiguration = createTopologyConfiguration();

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
            // Step 2: Setup input and output topics.
            final TestInputTopic<String, String> input = testDriver
                    .createInputTopic(inputTopic,
                            Serdes.String().serializer(),
                            Serdes.String().serializer());
            final TestOutputTopic<String, Integer> output = testDriver
                    .createOutputTopic(outputTopic, Serdes.String().deserializer(), Serdes.Integer().deserializer());

            // Step 3: Write the input.
            input.pipeKeyValueList(inputValues);

            // Step 4: Validate the output.
            Assert.assertThat(output.readKeyValuesToMap(), equalTo(expectedOutput));
        }
    }

    private Properties createTopologyConfiguration() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        // streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        return streamsConfiguration;
    }

}
