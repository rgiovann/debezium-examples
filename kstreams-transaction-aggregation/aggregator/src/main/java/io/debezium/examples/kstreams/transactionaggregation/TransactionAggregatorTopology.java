package io.debezium.examples.kstreams.transactionaggregation;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

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
        return builder.build();
    }
}
