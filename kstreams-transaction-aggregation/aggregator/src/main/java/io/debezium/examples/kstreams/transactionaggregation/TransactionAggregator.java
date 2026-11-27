package io.debezium.examples.kstreams.transactionaggregation;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class TransactionAggregator extends ContextualProcessor<String, JsonNode, String, AggregatedTransaction> {

    private KeyValueStore<String, OpenTransaction> openTransactions;

    @Override
    public void init(ProcessorContext<String, AggregatedTransaction> context) {
        super.init(context);
        openTransactions = context.getStateStore(TransactionAggregatorTopology.OPEN_TRANSACTIONS_STORE_NAME);
    }

    @Override
    public void process(Record<String, JsonNode> record) {
        String txId = record.key();
        JsonNode value = record.value();

        if (value.hasNonNull("status")) {
            return;
        }

        AggregatedEvent event = toAggregatedEvent(value);

        long totalOrder = value.get("transaction").get("total_order").asLong();

        OpenTransaction current = openTransactions.get(txId);

        Map<Long, AggregatedEvent> events;
        Long expectedEventCount;
        List<DataCollectionItem> dataCollections;
        long tsMs;
        if (current == null) {
            events = new HashMap<>();
            expectedEventCount = null;
            dataCollections = null;
            tsMs = value.get("source").get("ts_ms").asLong();
        } else {
            events = new HashMap<>(current.events());
            expectedEventCount = current.expectedEventCount();
            dataCollections = current.dataCollections();
            tsMs = current.tsMs();
        }

        events.put(totalOrder, event);

        openTransactions.put(txId, new OpenTransaction(events, expectedEventCount, dataCollections, tsMs));
    }

    private static AggregatedEvent toAggregatedEvent(JsonNode dataEvent) {
        JsonNode source = dataEvent.get("source");
        String table = source.get("schema").asText()
                + "."
                + source.get("table").asText();
        String op = dataEvent.get("op").asText();
        JsonNode before = dataEvent.get("before");
        JsonNode after = dataEvent.get("after");

        return new AggregatedEvent(table, op, before, after);
    }

}
