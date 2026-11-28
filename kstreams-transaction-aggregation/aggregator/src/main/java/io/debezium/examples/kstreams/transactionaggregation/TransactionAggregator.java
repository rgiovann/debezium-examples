package io.debezium.examples.kstreams.transactionaggregation;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
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

        OpenTransaction current = openTransactions.get(txId);

        if (current == null) {
            current = new OpenTransaction(new HashMap<>(),
                    null,
                    null,
                    extractTsMs(value));
        }

        openTransactions.put(txId, applyDataEvent(current, value));
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

    private static OpenTransaction applyDataEvent(OpenTransaction current, JsonNode dataEvent) {
        AggregatedEvent event = toAggregatedEvent(dataEvent);
        long totalOrder = dataEvent.get("transaction").get("total_order").asLong();
        Map<Long, AggregatedEvent> events = new HashMap<>(current.events());
        events.put(totalOrder, event);
        return new OpenTransaction(events,
                current.expectedEventCount(),
                current.dataCollections(),
                current.tsMs());
    }

    private static long extractTsMs(JsonNode message) {
        return message.hasNonNull("status")
                ? message.get("ts_ms").asLong()
                : message.get("source").get("ts_ms").asLong();
    }
}
