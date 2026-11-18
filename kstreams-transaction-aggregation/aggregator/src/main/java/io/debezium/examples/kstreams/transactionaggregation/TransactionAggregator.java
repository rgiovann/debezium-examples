package io.debezium.examples.kstreams.transactionaggregation;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

class TransactionAggregator extends ContextualProcessor<String, JsonNode, String, AggregatedTransaction> {

    @Override
    public void process(Record<String, JsonNode> record) {
    }

}
