package io.debezium.examples.kstreams.transactionaggregation;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record DataCollectionItem(String dataCollection, long eventCount) {
}
