package io.debezium.examples.kstreams.transactionaggregation;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.serde.DebeziumSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.Collections;
import java.util.regex.Pattern;

public class TransactionAggregatorTopology {
    private final Pattern inputPattern;
    private final String outputTopic;

    public TransactionAggregatorTopology(Pattern inputPattern, String outputTopic) {
        this.inputPattern = inputPattern;
        this.outputTopic = outputTopic;
    }

    public Topology build() {

        StreamsBuilder builder = new StreamsBuilder();

        Serde<JsonNode> inputValueSerde = DebeziumSerdes.payloadJson(JsonNode.class);

        inputValueSerde.configure(Collections.emptyMap(), false);

        Serde<byte[]> inputKeySerde = Serdes.ByteArray();

        builder.stream(inputPattern, Consumed.with(inputKeySerde, inputValueSerde));

        return builder.build();
    }
}
