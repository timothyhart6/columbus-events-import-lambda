package org.columbusEventsImportLambda.service.aws;

import org.columbusEventsImportLambda.models.DynamoDBEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.List;
import java.util.Map;

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

        ScanResponse response = dynamoDbClient.scan(scanRequest);

        return response.items().stream()
                .filter(item -> item != null && item.containsKey("eventName"))
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
