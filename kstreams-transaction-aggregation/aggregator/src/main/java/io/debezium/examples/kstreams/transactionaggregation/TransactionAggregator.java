package io.debezium.examples.kstreams.transactionaggregation;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

class TransactionAggregator extends ContextualProcessor<String, JsonNode, String, AggregatedTransaction> {

    private KeyValueStore<String, OpenTransaction> openTransactions;

    @Override
    public void init(ProcessorContext<String, AggregatedTransaction> context) {
        super.init(context);
        openTransactions = context.getStateStore(TransactionAggregatorTopology.OPEN_TRANSACTIONS_STORE_NAME);
    }

    @Override
    public void process(Record<String, JsonNode> record) {
    }

}
