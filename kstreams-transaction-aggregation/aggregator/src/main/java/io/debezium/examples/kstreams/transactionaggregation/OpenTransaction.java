package io.debezium.examples.kstreams.transactionaggregation;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.util.List;
import java.util.Map;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record OpenTransaction(Map<Long, AggregatedEvent> events,
                              Long expectedEventCount,
                              List<DataCollectionItem> dataCollections,
                              long tsMs) {
}
