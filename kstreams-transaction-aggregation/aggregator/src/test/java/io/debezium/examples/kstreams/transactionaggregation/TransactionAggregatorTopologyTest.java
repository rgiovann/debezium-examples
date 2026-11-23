package io.debezium.examples.kstreams.transactionaggregation;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TransactionAggregatorTopologyTest {
    private static final String INPUT_PATTERN = "dbserver1\\.(transaction|inventory\\..*)";
    private static final String OUTPUT_TOPIC = "aggregated-transactions";
    private static final String TRANSACTION_TOPIC = "dbserver1.transaction";
    private static final String CUSTOMERS_TOPIC = "dbserver1.inventory.customers";
    private static final String ORDERS_TOPIC = "dbserver1.inventory.orders";
    private static final String TRANSACTION_796_DIR = "/transaction-796/";

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> transactionInput;
    private TestInputTopic<String, String> customersInput;
    private TestInputTopic<String, String> ordersInput;
    private TestOutputTopic<String, String> aggregatedOutput;

    @BeforeEach
    void setUp() {
        driver = new TopologyTestDriver(new TransactionAggregatorTopology(Pattern.compile(INPUT_PATTERN), OUTPUT_TOPIC)
                .build());

        transactionInput = driver.createInputTopic(TRANSACTION_TOPIC,
                new StringSerializer(), new StringSerializer());

        customersInput = driver.createInputTopic(CUSTOMERS_TOPIC,
                new StringSerializer(), new StringSerializer());

        ordersInput = driver.createInputTopic(ORDERS_TOPIC,
                new StringSerializer(), new StringSerializer());

        aggregatedOutput = driver.createOutputTopic(OUTPUT_TOPIC,
                new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    @DisplayName("Emits a complete transaction when its events arrive in order")
    void shouldEmitCompleteTransactionWhenEventsArriveInOrder() throws IOException {
        transactionInput.pipeInput(readFixtureFromResource(TRANSACTION_796_DIR + "01-begin.json"));
        customersInput.pipeInput(readFixtureFromResource(TRANSACTION_796_DIR + "02-customers-update.json"));
        ordersInput.pipeInput(readFixtureFromResource(TRANSACTION_796_DIR + "03-orders-insert.json"));
        ordersInput.pipeInput(readFixtureFromResource(TRANSACTION_796_DIR + "04-orders-insert.json"));
        transactionInput.pipeInput(readFixtureFromResource(TRANSACTION_796_DIR + "05-end.json"));

        assertEquals(1, aggregatedOutput.getQueueSize());
    }

    @Test
    @DisplayName("Waits for the last event when END arrives before it")
    void shouldWaitForLastEventWhenEndArrivesFirst() {
        fail("Not implemented yet");
    }

    @Test
    @DisplayName("Accepts a data event that arrives before BEGIN")
    void shouldAcceptEventBeforeBegin() {
        fail("Not implemented yet");
    }

    @Test
    @DisplayName("Counts a duplicated data event once")
    void shouldCountDuplicatedEventOnce() {
        fail("Not implemented yet");
    }

    @Test
    @DisplayName("Aggregates two interleaved transactions separately")
    void shouldAggregateInterleavedTransactions() {
        fail("Not implemented yet");
    }

    @Test
    @DisplayName("Skips a snapshot event and opens no state")
    void shouldSkipSnapshotEvent() {
        fail("Not implemented yet");
    }

    @Test
    @DisplayName("Emits nothing for a transaction with no captured events")
    void shouldEmitNothingForEmptyTransaction() {
        fail("Not implemented yet");
    }

    @Test
    @DisplayName("Skips a tombstone")
    void shouldSkipTombstone() {
        fail("Not implemented yet");
    }

    private static String readFixtureFromResource(String filePath) throws IOException {
        try (InputStream inputStream = TransactionAggregatorTopologyTest.class.getResourceAsStream(filePath)) {
            return new String(Objects.requireNonNull(inputStream, "Fixture " + filePath + " not found.")
                    .readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
