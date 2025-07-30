package org.columbusEventsImportLambda.service;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.columbusEventsImportLambda.service.google.GoogleSheetService;
import org.columbusEventsImportLambda.models.DynamoDBEvent;
import org.columbusEventsImportLambda.models.Event;
import org.columbusEventsImportLambda.models.GoogleEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class ImportService {

    private static final String TABLE_NAME = "airbyte_sync_ColumbusEvents";
    public static final String GOOGLE_SHEET_ID = "COLUMBUS_GOOGLE_SHEET_ID";
    private final DynamoDbClient dynamoDbClient;
    private final GoogleSheetService googleSheetService;
    private final String sheetId;

    public ImportService(DynamoDbClient dynamoDbClient, GoogleSheetService googleSheetService, String sheetId) {
        this.dynamoDbClient = dynamoDbClient;
        this.googleSheetService = googleSheetService;
        this.sheetId = sheetId;

    }

    public void importGoogleRecordsToDynamoDB(List<GoogleEvent> googleEvents, List<DynamoDBEvent> dynamoDBEvents) {
        Map<String, DynamoDBEvent> dbEventMap = dynamoDBEvents.stream()
                .collect(Collectors.toMap(DynamoDBEvent::getId, Function.identity()));

        List<DynamoDBEvent> eventsToUpsertList = new ArrayList<>();

        for (GoogleEvent googleEvent : googleEvents) {
            String compositeKey = generateCompositeKey(googleEvent);

            DynamoDBEvent dbEvent = dbEventMap.get(compositeKey);
            if (dbEvent == null) {
                log.info("New event being added to the database");
                eventsToUpsertList.add(buildDynamoDBEvent(googleEvent, compositeKey));
            } else if (hasDifferentFields(dbEvent, googleEvent)) {
                log.info("Existing event being updated in the database");
                eventsToUpsertList.add(buildDynamoDBEvent(googleEvent, compositeKey));
            }

            // Always delete from Google Sheet regardless
            googleSheetService.deleteGoogleEvent(sheetId, googleEvent.getRowNumber());
        }

        batchUpsertToDynamoDB(eventsToUpsertList);
    }

    String generateCompositeKey(Event event) {
        return event.getEventName() + "|" + event.getLocationName() + "|" + event.getDate();
    }

    DynamoDBEvent buildDynamoDBEvent(Event event, String id) {
        return DynamoDBEvent.builder()
                .id(id)
                .eventName(event.getEventName())
                .locationName(event.getLocationName())
                .date(event.getDate())
                .time(event.getTime())
                .isBadTraffic(event.isBadTraffic())
                .isDesiredEvent(event.isDesiredEvent())
                .build();
    }

    private boolean hasDifferentFields(Event dbEvent, Event googleEvent) {
        return !Objects.equals(dbEvent.getTime(), googleEvent.getTime()) ||
                dbEvent.isBadTraffic() != googleEvent.isBadTraffic() ||
                dbEvent.isDesiredEvent() != googleEvent.isDesiredEvent();
    }

    private void batchUpsertToDynamoDB(List<DynamoDBEvent> events) {
        List<WriteRequest> writeRequests = events.stream()
                .map(this::buildPutRequest)
                .collect(Collectors.toList());

        List<List<WriteRequest>> batches = Lists.partition(writeRequests, 25);

        for (List<WriteRequest> batch : batches) {
            Map<String, List<WriteRequest>> requestMap = Map.of(TABLE_NAME, batch);
            BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
                    .requestItems(requestMap)
                    .build();

            dynamoDbClient.batchWriteItem(batchRequest);
        }
    }

    WriteRequest buildPutRequest(DynamoDBEvent event) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s(event.getId()).build());
        item.put("eventName", AttributeValue.builder().s(event.getEventName()).build());
        item.put("locationName", AttributeValue.builder().s(event.getLocationName()).build());
        item.put("date", AttributeValue.builder().s(event.getDate()).build());
        item.put("time", AttributeValue.builder().s(event.getTime()).build());
        item.put("isBadTraffic", AttributeValue.builder().bool(event.isBadTraffic()).build());
        item.put("isDesiredEvent", AttributeValue.builder().bool(event.isDesiredEvent()).build());

        return WriteRequest.builder()
                .putRequest(PutRequest.builder().item(item).build())
                .build();
    }

}
