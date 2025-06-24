package org.columbusEventsSyncLambda;

import org.columbusEventsSyncLambda.models.Event;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import java.util.stream.Collectors;

public class DynamoDBReader {

    private final String tableName;

    public DynamoDBReader(String tableName) {
        this.tableName = tableName;
    }

    public List<Event> getAllEvents(DynamoDbClient dynamoDbClient) {
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .build();

        return dynamoDbClient.scan(scanRequest).items().stream()
                .map(item -> item.get("_airbyte_data"))
                .filter(Objects::nonNull)
                .map(AttributeValue::m)
                .map(this::mapToEvent)
                .collect(Collectors.toList());
    }

    private Event mapToEvent(Map<String, AttributeValue> data) {
        return Event.builder()
                .locationName(nullCheckString(data.get("locationName")))
                .eventName(nullCheckString(data.get("eventName")))
                .date(nullCheckString(data.get("date")))
                .time(nullCheckString(data.get("time")))
                .isBadTraffic(nullCheckBool(data.get("causesTraffic")))
                .isDesiredEvent(nullCheckBool(data.get("interestingEvent")))
                .build();
    }

    private static String nullCheckString(AttributeValue attribute) {
        return (attribute != null && attribute.s() != null) ? attribute.s() : "";
    }

    private static boolean nullCheckBool(AttributeValue attribute) {
        return attribute != null && attribute.bool() != null ? attribute.bool() : true;
    }
}
