package org.columbusEventsImportLambda.aws;

import org.columbusEventsImportLambda.models.DynamoDBEvent;
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

    public List<DynamoDBEvent> fetchEvents(DynamoDbClient dynamoDbClient) {
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .build();

        return dynamoDbClient.scan(scanRequest).items().stream()
                .map(item -> item.get("_airbyte_data"))
                .filter(Objects::nonNull)
                .map(AttributeValue::m)
                .map(this::mapToDynamoDbEvent)
                .collect(Collectors.toList());
    }

    private DynamoDBEvent mapToDynamoDbEvent(Map<String, AttributeValue> item) {

        return  DynamoDBEvent.builder()
                        .id(nullCheckString(item.get("id")))
                        .eventName(nullCheckString(item.get("eventName")))
                        .locationName(nullCheckString(item.get("locationName")))
                        .date(nullCheckString(item.get("date")))
                        .time(nullCheckString(item.get("time")))
                        .isBadTraffic(nullCheckBool(item.get("isBadTraffic")))
                        .isDesiredEvent(nullCheckBool(item.get("isDesiredEvent")))
                        .build();
    }

    private static String nullCheckString(AttributeValue attribute) {
        return (attribute != null && attribute.s() != null) ? attribute.s() : "";
    }

    private static boolean nullCheckBool(AttributeValue attribute) {
        return attribute != null && attribute.bool() != null ? attribute.bool() : true;
    }
}
