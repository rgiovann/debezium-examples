package io.debezium.examples.kstreams.transactionaggregation;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.List;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record AggregatedTransaction(AggregatedTransactionStatus status,
                                    long tsMs,
                                    Long expectedEventCount,
                                    long receivedEventCount,
                                    List<DataCollectionItem> dataCollections,
                                    List<AggregatedEvent> events) {}
