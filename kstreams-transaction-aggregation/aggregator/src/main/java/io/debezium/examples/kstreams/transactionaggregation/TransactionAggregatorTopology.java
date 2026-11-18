package io.debezium.examples.kstreams.transactionaggregation;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.serde.DebeziumSerdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.regex.Pattern;

public class TransactionAggregatorTopology {
    private static final String REPARTITION_TOPIC_NAME = "transactions-by-id";
    private static final String OPEN_TRANSACTIONS_STORE_NAME = "open-transactions";

    private final Pattern inputPattern;
    private final String outputTopic;

    public TransactionAggregatorTopology(Pattern inputPattern, String outputTopic) {
        this.inputPattern = inputPattern;
        this.outputTopic = outputTopic;
    }

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<byte[]> inputKeySerde = Serdes.ByteArray();

        Serde<JsonNode> inputValueSerde = DebeziumSerdes.payloadJson(JsonNode.class);
        inputValueSerde.configure(Collections.emptyMap(), false);

        Serde<String> txIdSerde = Serdes.String();

        Serde<OpenTransaction> openTransactionSerde = DebeziumSerdes.payloadJson(OpenTransaction.class);
        openTransactionSerde.configure(Collections.emptyMap(), false);

        // Stores open transactions by txID until they are emitted.
        StoreBuilder<KeyValueStore<String, OpenTransaction>> openTransactionsStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(OPEN_TRANSACTIONS_STORE_NAME),
                        txIdSerde,
                        openTransactionSerde);

        builder.addStateStore(openTransactionsStoreBuilder);

        Predicate<byte[], JsonNode> isPartOfTransaction = (key, value) ->
                value.hasNonNull("status") || value.hasNonNull("transaction");

        builder.stream(inputPattern, Consumed.with(inputKeySerde, inputValueSerde))
                // Discard tombstone records emitted by the connector after a DELETE operation.
                .filter((key, value) -> value != null)
                // Discard snapshot events without a transaction block;
                // keep BEGIN and END events, which contain a status.
                .filter(isPartOfTransaction)
                // Use the transaction ID as the key, extracted from the PostgreSQL id (txId:LSN).
                // The LSN changes for each message within the same transaction.
                .selectKey((key, value) -> extractTxId(value))
                // process() does not trigger repartitioning automatically.
                // Repartition by txID to ensure all messages from the same transaction are processed by the same task.
                .repartition(Repartitioned.with(txIdSerde, inputValueSerde).withName(REPARTITION_TOPIC_NAME))
                // aggregates each transaction's messages in the store and emits the transaction when it closes
                .process(TransactionAggregator::new, OPEN_TRANSACTIONS_STORE_NAME);

        return builder.build();
    }

    private static String extractTxId(JsonNode value) {
        String txIdLsn = value.hasNonNull("status")
                ? value.get("id").asText()
                : value.get("transaction").get("id").asText();

        return txIdLsn.substring(0, txIdLsn.indexOf(":"));
    }

//    KStream<String, AggregatedTransaction> process(
//            () -> new TransactionAggregator(),
//    OPEN_TRANSACTIONS_STORE_NAME){
//
//    }
}
