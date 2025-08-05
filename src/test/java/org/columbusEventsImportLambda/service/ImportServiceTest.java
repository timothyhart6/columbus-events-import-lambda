package org.columbusEventsImportLambda.service;

import org.columbusEventsImportLambda.models.DynamoDBEvent;
import org.columbusEventsImportLambda.models.Event;
import org.columbusEventsImportLambda.models.GoogleEvent;
import org.columbusEventsImportLambda.service.google.GoogleSheetService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ImportServiceTest {

    @Mock
    private DynamoDbClient dynamoDbClient;
    @Mock
    private GoogleSheetService googleSheetService;

    private ImportService importService;
    @BeforeEach
    void setup() {
        importService = new ImportService(dynamoDbClient, googleSheetService, "fake-sheet-id", "ColumbusEvents");

    }

    @Test
    public void recordsUpsertedToDatabase() {
        List<DynamoDBEvent> existingDbEvents = Collections.emptyList();

        importService.importGoogleRecordsToDynamoDB(List.of(googleEvent), existingDbEvents);

        verify(dynamoDbClient, times(1)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void manyRecordsUpsertedToDatabase() {
        List<GoogleEvent> googleEvents = IntStream.range(0, 30)
                .mapToObj(i -> GoogleEvent.builder()
                        .eventName("Event " + i)
                        .locationName("Location " + i)
                        .date("2025-08-" + String.format("%02d", i + 1))
                        .isDesiredEvent(false)
                        .isBadTraffic(false)
                        .build()).collect(Collectors.toUnmodifiableList());

        importService.importGoogleRecordsToDynamoDB(googleEvents, Collections.emptyList());

        verify(dynamoDbClient, times(2)).batchWriteItem(any(BatchWriteItemRequest.class));
    }



    @Test
    public void noRecordsUpsertedToDatabase() {
        importService.importGoogleRecordsToDynamoDB(Collections.emptyList(), Collections.emptyList());

        verify(dynamoDbClient, times(0)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void compositeKeyGeneratesSuccessfully() {
        String key = importService.generateCompositeKey(googleEvent);

        assertEquals("Test Event|Convention Center|2025-08-01", key);
    }

    @Test
    public void googleEventIsNotInDatabase() {
        GoogleEvent googleEvent = GoogleEvent.builder()
                .eventName("Concert")
                .locationName("Arena")
                .date("2025-08-01")
                .time("7 PM")
                .isBadTraffic(true)
                .isDesiredEvent(false)
                .rowNumber(2) // if logic decrements it, this will match actual = 1
                .build();

        importService.importGoogleRecordsToDynamoDB(List.of(googleEvent), Collections.emptyList());

        verify(googleSheetService, times(1)).deleteGoogleEvent("fake-sheet-id", 2);
        verify(dynamoDbClient, times(1)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void googleEventHasDifferingDBFields() {
        GoogleEvent googleEvent = GoogleEvent.builder()
                .eventName("Festival")
                .locationName("Park")
                .date("2025-08-01")
                .time("10 AM")
                .isBadTraffic(false)
                .isDesiredEvent(true)
                .rowNumber(3)
                .build();

        DynamoDBEvent dbEvent = DynamoDBEvent.builder()
                .id("Festival|Park|2025-08-01")
                .eventName("Festival")
                .locationName("Park")
                .date("2025-08-01")
                .time("9 AM") // time differs
                .isBadTraffic(false)
                .isDesiredEvent(true)
                .build();

        importService.importGoogleRecordsToDynamoDB(List.of(googleEvent), List.of(dbEvent));

        verify(googleSheetService, times(1)).deleteGoogleEvent("fake-sheet-id", 3);
        verify(dynamoDbClient, times(1)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void googleEventHasSameDBFields() {
        GoogleEvent googleEvent = GoogleEvent.builder()
                .eventName("Meeting")
                .locationName("Office")
                .date("2025-08-01")
                .time("2 PM")
                .isBadTraffic(false)
                .isDesiredEvent(true)
                .rowNumber(4)
                .build();

        DynamoDBEvent dbEvent = DynamoDBEvent.builder()
                .id("Meeting|Office|2025-08-01")
                .eventName("Meeting")
                .locationName("Office")
                .date("2025-08-01")
                .time("2 PM")
                .isBadTraffic(false)
                .isDesiredEvent(true)
                .build();

        importService.importGoogleRecordsToDynamoDB(List.of(googleEvent), List.of(dbEvent));

        verify(googleSheetService, times(1)).deleteGoogleEvent("fake-sheet-id", 4);
        verify(dynamoDbClient, times(0)).batchWriteItem(any(BatchWriteItemRequest.class));
    }

    @Test
    public void buildDynamoDBEventBuildsCorrectly() {
        Event event = GoogleEvent.builder()
                .eventName("Parade")
                .locationName("Main St")
                .date("2025-09-01")
                .time("10 AM")
                .isBadTraffic(true)
                .isDesiredEvent(false)
                .build();

        String id = "Parade|Main St|2025-09-01";

        DynamoDBEvent dynamoEvent = importService.buildDynamoDBEvent(event, id);

        assertEquals(id, dynamoEvent.getId());
        assertEquals("Parade", dynamoEvent.getEventName());
        assertEquals("Main St", dynamoEvent.getLocationName());
        assertEquals("2025-09-01", dynamoEvent.getDate());
        assertEquals("10 AM", dynamoEvent.getTime());
        assertTrue(dynamoEvent.isBadTraffic());
        assertFalse(dynamoEvent.isDesiredEvent());
    }

    @Test
    public void buildPutRequestBuildsCorrectly() {
        DynamoDBEvent event = DynamoDBEvent.builder()
                .id("123")
                .eventName("Game")
                .locationName("Stadium")
                .date("2025-10-01")
                .time("5 PM")
                .isBadTraffic(false)
                .isDesiredEvent(true)
                .build();

        WriteRequest request = importService.buildPutRequest(event);

        var item = request.putRequest().item();

        assertEquals("123", item.get("id").s());
        assertEquals("Game", item.get("eventName").s());
        assertEquals("Stadium", item.get("locationName").s());
        assertEquals("2025-10-01", item.get("date").s());
        assertEquals("5 PM", item.get("time").s());
        assertFalse(item.get("isBadTraffic").bool());
        assertTrue(item.get("isDesiredEvent").bool());
    }

    GoogleEvent googleEvent = GoogleEvent.builder()
            .eventName("Test Event")
            .locationName("Convention Center")
            .date("2025-08-01")
            .time("6:00 PM")
            .isBadTraffic(false)
            .isDesiredEvent(true)
            .rowNumber(1)
            .build();
}
