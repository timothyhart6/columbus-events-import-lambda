package org.columbusEventsImportLambda.service.aws;

import org.columbusEventsImportLambda.models.DynamoDBEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DynamoDBReaderTest {
    @Mock
    DynamoDbClient dynamoDbClient;

    DynamoDBReader reader;


    @BeforeEach
    public void setup() {
        reader = new DynamoDBReader("TestingTableName");
    }

    @Test
    public void successfullyFetchEvents() {

        Map<String, AttributeValue> fakeData = Map.of(
                "_airbyte_data", AttributeValue.fromM(Map.of(
                        "eventName", AttributeValue.fromS("Test Event"),
                        "locationName", AttributeValue.fromS("Convention Center")
                ))
        );

        ScanResponse fakeResponse = ScanResponse.builder()
                .items(List.of(fakeData))
                .build();

        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(fakeResponse);

       List<DynamoDBEvent> events = reader.fetchEvents(dynamoDbClient);

        verify(dynamoDbClient, times(1)).scan(any(ScanRequest.class));
        assertEquals(1, events.size());
        assertEquals("Test Event", events.get(0).getEventName());
    }

    @Test
    public void multipleEventsReturned() {
        Map<String, AttributeValue> item1 = Map.of("_airbyte_data", AttributeValue.fromM(
                Map.of("eventName", AttributeValue.fromS("Event 1"))
        ));

        Map<String, AttributeValue> item2 = Map.of("_airbyte_data", AttributeValue.fromM(
                Map.of("eventName", AttributeValue.fromS("Event 2"))
        ));

        ScanResponse fakeResponse = ScanResponse.builder().items(List.of(item1, item2)).build();
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(fakeResponse);

        List<DynamoDBEvent> events = reader.fetchEvents(dynamoDbClient);

        assertEquals(2, events.size());
        assertEquals("Event 1", events.get(0).getEventName());
        assertEquals("Event 2", events.get(1).getEventName());
    }


    @Test
    public void fetchEventsReturnsEmptyList() {
        ScanResponse emptyResponse = ScanResponse.builder().items(List.of()).build();
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(emptyResponse);

        List<DynamoDBEvent> events = reader.fetchEvents(dynamoDbClient);

        assertEquals(0, events.size());
    }

    @Test
    public void missingBooleanFieldsDefaultsToTrue() {
        Map<String, AttributeValue> partialData = Map.of(
                "_airbyte_data", AttributeValue.fromM(Map.of(
                        "eventName", AttributeValue.fromS("Boolean Default Test")
                ))
        );

        ScanResponse fakeResponse = ScanResponse.builder().items(List.of(partialData)).build();
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(fakeResponse);

        List<DynamoDBEvent> events = reader.fetchEvents(dynamoDbClient);

        assertTrue(events.get(0).isBadTraffic());
        assertTrue(events.get(0).isDesiredEvent());
    }

    @Test
    public void nullDataIsSkipped() {
        Map<String, AttributeValue> invalidData = Map.of();

        ScanResponse fakeResponse = ScanResponse.builder().items(List.of(invalidData)).build();
        when(dynamoDbClient.scan(any(ScanRequest.class))).thenReturn(fakeResponse);

        List<DynamoDBEvent> events = reader.fetchEvents(dynamoDbClient);

        assertEquals(0, events.size());
    }
}
