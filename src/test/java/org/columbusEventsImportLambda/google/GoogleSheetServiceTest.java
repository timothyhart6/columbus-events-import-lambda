package org.columbusEventsImportLambda.google;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.*;
import org.columbusEventsImportLambda.models.Event;
import org.columbusEventsImportLambda.models.GoogleEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class GoogleSheetServiceTest {
    @Mock
    (answer = Answers.RETURNS_DEEP_STUBS)
    HttpRequestFactory httpRequestFactory;
    @Mock
    HttpRequest httpRequest;
    @Mock
    HttpResponse httpResponse;

    @Test
    public void fetchEventsReturnsAString() throws IOException {
        GoogleSheetService sheetService = new GoogleSheetService(httpRequestFactory);

        String json = "{\n" +
                "  \"range\": \"ColumbusEvents!A2:D999\",\n" +
                "  \"majorDimension\": \"ROWS\",\n" +
                "  \"values\": [\n" +
                "    [\n" +
                "      \"Convention Center\",\n" +
                "      \"The Arnold Classic\",\n" +
                "      \"02-28-2025\",\n" +
                "      \"All Day\",\n" +
                "      \"TRUE\",\n" +
                "      \"TRUE\"\n" +
                "    ]\n" +
                "  ]\n" +
                "}";

        when(httpRequestFactory.buildGetRequest(any(GenericUrl.class)).execute()).thenReturn(httpResponse);
        when(httpResponse.parseAsString()).thenReturn(json);

        Event result = sheetService.fetchEvents("fakeId", "meaningless range").get(0);

        assertInstanceOf(Event.class, result);
        assertEquals("Convention Center", result.getLocationName());
        assertEquals("The Arnold Classic", result.getEventName());
        assertEquals("02-28-2025", result.getDate());
        assertEquals("All Day", result.getTime());
        assertTrue(result.isBadTraffic());
        assertTrue(result.isDesiredEvent());
    }

    @Test
    void fetchEventsHandlesNullRequestFactory() {
        //Unit test will not have credentials, which creates behavior needed for this test
        GoogleSheetService sheetService = new GoogleSheetService();

        List<GoogleEvent> result = sheetService.fetchEvents("id", "A2:D");

        assertTrue(result.isEmpty());
    }

    @Test
    void fetchEventsHandlesIOException() throws IOException {
        GoogleSheetService sheetService = new GoogleSheetService(httpRequestFactory);

        when(httpRequestFactory.buildGetRequest(any(GenericUrl.class))).thenThrow(new IOException("Fake IO error"));

        List<GoogleEvent> result = sheetService.fetchEvents("id", "A2:D");

        assertTrue(result.isEmpty());
    }

    @Test
    void fetchEventsHandlesParseFailure() throws IOException {
        when(httpRequestFactory.buildGetRequest(any(GenericUrl.class)).execute()).thenReturn(httpResponse);
        when(httpResponse.parseAsString()).thenThrow(new IOException("Parsing failed"));

        GoogleSheetService sheetService = new GoogleSheetService(httpRequestFactory);
        List<GoogleEvent> result = sheetService.fetchEvents("id", "A2:D");

        assertTrue(result.isEmpty());
    }

    @Test
    void deleteGoogleEvent_requestSuccessfullySends() throws Exception {
        GoogleSheetService sheetService = new GoogleSheetService(httpRequestFactory);

        when(httpRequestFactory.buildPostRequest(any(GenericUrl.class), any(HttpContent.class)))
                .thenReturn(httpRequest);

        sheetService.deleteGoogleEvent("testSpreadsheetId", 5);

        verify(httpRequest, times(1)).execute();
    }

    @Test
    void deleteGoogleEvent_sendsCorrectPayload() throws Exception {
        GoogleSheetService sheetService = new GoogleSheetService(httpRequestFactory);
        ArgumentCaptor<HttpContent> contentCaptor = ArgumentCaptor.forClass(HttpContent.class);

        when(httpRequestFactory.buildPostRequest(any(GenericUrl.class), contentCaptor.capture()))
                .thenReturn(httpRequest);
        when(httpRequest.execute()).thenReturn(mock(HttpResponse.class));

        sheetService.deleteGoogleEvent("testSpreadsheetId", 5);

        HttpContent content = contentCaptor.getValue();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        content.writeTo(out);
        String payload = out.toString(StandardCharsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(payload);

        JsonNode deleteRange = root.get("requests").get(0)
                .get("deleteDimension").get("range");

        assertEquals(5, deleteRange.get("startIndex").asInt());
        assertEquals(6, deleteRange.get("endIndex").asInt());
        assertEquals(0, deleteRange.get("sheetId").asInt());
        assertEquals("ROWS", deleteRange.get("dimension").asText());
    }

    @Test
    void deleteGoogleEvent_requestFails() throws Exception {
        GoogleSheetService sheetService = new GoogleSheetService(httpRequestFactory);

        when(httpRequestFactory.buildPostRequest(any(GenericUrl.class), any(HttpContent.class))).thenThrow(IOException.class);

        assertDoesNotThrow(() -> sheetService.deleteGoogleEvent("spreadsheetId", 5));

        verify(httpRequestFactory, times(1)).buildPostRequest(any(GenericUrl.class), any(HttpContent.class));
    }
}
